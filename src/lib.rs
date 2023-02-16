use chrono::{Duration, NaiveDateTime, Utc};
use futures::{future::BoxFuture, Future};
use std::{
    cell::RefCell,
    cmp::PartialEq,
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    rc::Rc,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, mpsc::channel, Mutex},
    task::JoinSet,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Event<V> {
    pub processing_date_time: NaiveDateTime,
    pub event_date_time: Option<NaiveDateTime>,
    pub watermark_date_time: Option<NaiveDateTime>,
    pub value: V,
}

impl<V> Event<V> {
    pub fn new(value: V) -> Self {
        Self {
            processing_date_time: Utc::now().naive_utc(),
            event_date_time: None,
            watermark_date_time: None,
            value,
        }
    }

    pub fn with_value<O>(self, value: O) -> Event<O> {
        Event {
            event_date_time: self.event_date_time,
            processing_date_time: self.processing_date_time,
            watermark_date_time: self.watermark_date_time,
            value,
        }
    }
}

pub struct Sender<V> {
    sender: mpsc::Sender<Event<V>>,
}

impl<V> Clone for Sender<V> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<V> Sender<V> {
    pub async fn send(&mut self, event: Event<V>) {
        if self.sender.send(event).await.is_err() {
            panic!("receiver closed");
        }
    }
}

pub trait Source<V> {
    fn run<'a>(self, sender: Sender<V>) -> BoxFuture<'a, ()>;
}

pub struct Receiver<V> {
    receiver: mpsc::Receiver<Event<V>>,
}

impl<V> Receiver<V> {
    pub async fn receive(&mut self) -> Option<Event<V>> {
        self.receiver.recv().await
    }
}

pub trait Sink<V> {
    fn run<'a>(self, receiver: Receiver<V>) -> BoxFuture<'a, ()>;
}

// Used as a workaround so that the borrowed lifetime of argument A can be
// captured in higher-rank trait bounds in DataStream::filter.
pub trait AsyncSingleArgFn<A>: Fn(A) -> <Self as AsyncSingleArgFn<A>>::Future {
    type Future: Future<Output = <Self as AsyncSingleArgFn<A>>::Output> + Send;
    type Output;
}

impl<A, FN, F> AsyncSingleArgFn<A> for FN
where
    FN: Fn(A) -> F,
    F: Future + Send,
{
    type Future = F;
    type Output = F::Output;
}

pub trait AsyncDoubleArgFn<A, B>: Fn(A, B) -> <Self as AsyncDoubleArgFn<A, B>>::Future {
    type Future: Future<Output = <Self as AsyncDoubleArgFn<A, B>>::Output> + Send;
    type Output;
}

impl<A, B, FN, F> AsyncDoubleArgFn<A, B> for FN
where
    FN: Fn(A, B) -> F,
    F: Future + Send,
{
    type Future = F;
    type Output = F::Output;
}

pub struct DataStream<V> {
    nodes: Rc<RefCell<JoinSet<()>>>,
    receiver: mpsc::Receiver<Event<V>>,
}

impl<V> DataStream<V>
where
    V: Send + Sync + 'static,
{
    pub fn add_sink<S>(self, sink: S)
    where
        S: Sink<V> + 'static,
    {
        let receiver = Receiver {
            receiver: self.receiver,
        };
        self.nodes.borrow_mut().spawn(sink.run(receiver));
    }

    pub fn map<O, FN, F>(self, map: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(Event<V>) -> F + Send + Sync + 'static,
        F: Future<Output = Event<O>> + Send + 'static,
    {
        let map = Arc::new(map);
        self.process_state(
            move |event, _, mut sender: Sender<O>| {
                let map = Arc::clone(&map);
                async move {
                    let event = map(event).await;
                    sender.send(event).await;
                }
            },
            (),
        )
    }

    pub fn filter<FN>(self, filter: FN) -> DataStream<V>
    where
        FN: for<'a> AsyncSingleArgFn<&'a Event<V>, Output = bool> + Send + Sync + 'static,
    {
        let filter = Arc::new(filter);
        self.process_state(
            move |event, _, mut sender: Sender<V>| {
                let filter = Arc::clone(&filter);
                async move {
                    if filter(&event).await {
                        sender.send(event).await;
                    }
                }
            },
            (),
        )
    }

    fn process<O, FN, F>(self, process: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(Event<V>, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.process_state(
            move |event, _, sender: Sender<O>| process(event, sender),
            (),
        )
    }

    pub fn process_state<O, FN, F, GST>(self, process: FN, global_state: GST) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(Event<V>, GST, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<O>>(1);

        self.nodes.borrow_mut().spawn(async move {
            let mut receiver = self.receiver;
            while let Some(event) = receiver.recv().await {
                let sender = Sender {
                    sender: sender.clone(),
                };
                process(event, global_state.clone(), sender).await;
            }
        });

        DataStream {
            nodes: Rc::clone(&self.nodes),
            receiver,
        }
    }

    pub fn key_by<KS, K>(self, selector: KS) -> KeyedDataStream<V, KS, K>
    where
        KS: Fn(&Event<V>) -> K,
        K: Hash + Eq + Send + Clone + 'static,
    {
        KeyedDataStream {
            data_stream: self,
            selector,
        }
    }
}

pub struct KeyedDataStream<V, KS, K>
where
    KS: Fn(&Event<V>) -> K,
    K: Hash + Eq + Send + Clone + 'static,
{
    data_stream: DataStream<V>,
    selector: KS,
}

impl<V, KS, K> KeyedDataStream<V, KS, K>
where
    V: Clone + Send + Sync + 'static,
    KS: Fn(&Event<V>) -> K + Send + 'static,
    K: Hash + Eq + Send + Clone + 'static,
{
    pub fn add_sink<S>(self, sink: S)
    where
        S: Sink<V> + Send + 'static,
    {
        self.data_stream.add_sink(sink);
    }

    pub fn map<O, FN, F>(self, map: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Event<V>) -> F + Send + Sync + 'static,
        F: Future<Output = Event<O>> + Send + 'static,
    {
        let map = Arc::new(map);
        self.process_state(
            move |key, event, _, _, mut sender: Sender<O>| {
                let map = Arc::clone(&map);
                async move {
                    let event = map(key, event).await;
                    sender.send(event).await;
                }
            },
            (),
            |_| (),
        )
    }

    pub fn filter<FN>(self, filter: FN) -> DataStream<V>
    where
        FN: for<'a> AsyncDoubleArgFn<K, &'a Event<V>, Output = bool> + Send + Sync + 'static,
    {
        let filter = Arc::new(filter);
        self.process_state(
            move |key, event, _, _, mut sender: Sender<V>| {
                let filter = Arc::clone(&filter);
                async move {
                    if filter(key, &event).await {
                        sender.send(event).await;
                    }
                }
            },
            (),
            |_| (),
        )
    }

    fn process<O, FN, F>(self, process: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Event<V>, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.process_state(
            move |key, event, _, _, sender: Sender<O>| process(key, event, sender),
            (),
            |_| (),
        )
    }

    pub fn process_state<O, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state_fn: KSTF,
    ) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Event<V>, GST, KST, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
        KSTF: Fn(&K) -> KST + Send + Sync + 'static,
        KST: Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<O>>(1);

        self.data_stream.nodes.borrow_mut().spawn(async move {
            let mut key_states: HashMap<K, KST> = HashMap::default();

            let mut receiver = self.data_stream.receiver;
            let selector = self.selector;
            while let Some(event) = receiver.recv().await {
                let key = selector(&event);

                let key_state = key_states
                    .entry(key.clone())
                    .or_insert_with_key(&key_state_fn);
                let sender = Sender {
                    sender: sender.clone(),
                };
                process(key, event, global_state.clone(), key_state.clone(), sender).await;
            }
        });

        DataStream {
            nodes: Rc::clone(&self.data_stream.nodes),
            receiver,
        }
    }

    pub fn window<P>(self, processor: P) -> WindowedDataStream<V, KS, K, P, P>
    where
        P: WindowAssigner<V> + WindowMerger,
    {
        let data_stream = self.data_stream;
        let key_selector = self.selector;
        let processor = Arc::new(processor);
        WindowedDataStream {
            data_stream: data_stream,
            windows_processor: WindowsProcessor {
                key_windows: Default::default(),
                key_selector,
                assigner: Arc::clone(&processor),
                merger: Arc::clone(&processor),
            },
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Window {
    // Contains all events that are >= start_date_time.
    start_date_time: NaiveDateTime,

    // Contains all events that are < end_date_time.
    end_date_time: NaiveDateTime,
}

impl Window {
    fn intersects(&self, other: &Self) -> bool {
        self.start_date_time <= other.end_date_time && self.end_date_time >= other.start_date_time
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            start_date_time: self.start_date_time.min(other.start_date_time),
            end_date_time: self.end_date_time.max(other.end_date_time),
        }
    }
}

pub trait WindowAssigner<V> {
    type Iterator: Iterator<Item = Window>;

    fn assign(&self, event: &Event<V>) -> Self::Iterator;
}

pub trait WindowMerger {
    fn merge<'a>(
        &'a self,
        windows: impl Iterator<Item = &'a Window>,
    ) -> Box<[(Box<[Window]>, Window)]>;
}

pub struct EventTimeSessionWindowProcessor {
    timeout: Duration,
}

impl EventTimeSessionWindowProcessor {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl<V> WindowAssigner<V> for EventTimeSessionWindowProcessor {
    type Iterator = std::iter::Once<Window>;

    fn assign(&self, event: &Event<V>) -> Self::Iterator {
        std::iter::once(Window {
            start_date_time: event.event_date_time.unwrap(),
            end_date_time: event.event_date_time.unwrap() + self.timeout,
        })
    }
}

impl WindowMerger for EventTimeSessionWindowProcessor {
    fn merge<'a>(
        &'a self,
        windows: impl Iterator<Item = &'a Window>,
    ) -> Box<[(Box<[Window]>, Window)]> {
        let mut merged = Vec::default();
        let mut current_merge: Option<(HashSet<Window>, Window)> = None;

        for window in windows {
            match current_merge {
                None => {
                    let mut merged_windows = HashSet::default();
                    merged_windows.insert(window.clone());
                    current_merge = Some((merged_windows, window.clone()));
                }
                Some(ref mut merge) => {
                    if merge.1.intersects(window) {
                        merge.1 = merge.1.union(window);
                        merge.0.insert(window.clone());
                    } else {
                        let mut merged_windows = HashSet::default();
                        merged_windows.insert(window.clone());
                        merged.push(
                            current_merge
                                .replace((merged_windows, window.clone()))
                                .unwrap(),
                        );
                    }
                }
            }
        }

        if let Some(merge) = current_merge {
            merged.push(merge);
        }

        merged
            .into_iter()
            .map(|(merged_windows, window)| {
                (
                    merged_windows
                        .into_iter()
                        .collect::<Vec<Window>>()
                        .into_boxed_slice(),
                    window,
                )
            })
            .collect::<Vec<(Box<[Window]>, Window)>>()
            .into_boxed_slice()
    }
}

struct Windows<V> {
    events: BTreeMap<NaiveDateTime, Vec<Event<V>>>,
    start_date_time_windows: BTreeMap<NaiveDateTime, HashSet<Window>>,
    end_date_time_windows: BTreeMap<NaiveDateTime, HashSet<Window>>,
}

impl<V> Default for Windows<V> {
    fn default() -> Self {
        Self {
            events: BTreeMap::default(),
            start_date_time_windows: BTreeMap::default(),
            end_date_time_windows: BTreeMap::default(),
        }
    }
}

impl<V> Windows<V> {
    fn add_window(&mut self, window: &Window) {
        self.start_date_time_windows
            .entry(window.start_date_time)
            .or_default()
            .insert(window.clone());
        self.end_date_time_windows
            .entry(window.end_date_time)
            .or_default()
            .insert(window.clone());
    }

    fn add_event(&mut self, event: Event<V>) {
        let event_date_time = event.event_date_time.unwrap();
        self.events.entry(event_date_time).or_default().push(event);
    }

    fn windows(&self) -> impl Iterator<Item = &Window> {
        self.start_date_time_windows.values().flatten()
    }

    /// Replaces from_window to to_windows and migrates all the interanl data
    /// appropriately.
    fn replace(&mut self, from_window: &Window, to_window: &Window) {
        // Replace old windows with new merged ones.
        let start_windows = self
            .start_date_time_windows
            .get_mut(&from_window.start_date_time)
            .unwrap();
        start_windows.remove(&from_window);
        if start_windows.is_empty() {
            self.start_date_time_windows
                .remove(&from_window.start_date_time);
        }
        self.start_date_time_windows
            .entry(to_window.start_date_time)
            .or_default()
            .insert(to_window.clone());

        let end_windows = self
            .end_date_time_windows
            .get_mut(&from_window.end_date_time)
            .unwrap();
        end_windows.remove(&from_window);
        if end_windows.is_empty() {
            self.end_date_time_windows
                .remove(&from_window.end_date_time);
        }
        self.end_date_time_windows
            .entry(to_window.end_date_time)
            .or_default()
            .insert(to_window.clone());
    }

    /// Return all the events for all the windows that have expired due to the
    /// new watermark_date_time.
    fn events<'a>(
        &'a mut self,
        watermark_date_time: NaiveDateTime,
    ) -> impl Iterator<Item = (Window, Box<[Event<V>]>)> + 'a
    where
        V: Clone,
    {
        let split_date_time = watermark_date_time + Duration::nanoseconds(1);
        let end_date_time_windows = self.end_date_time_windows.split_off(&split_date_time);

        let expired_end_date_time_windows =
            std::mem::replace(&mut self.end_date_time_windows, end_date_time_windows);

        expired_end_date_time_windows
            .into_values()
            .flatten()
            .map(|window| {
                // Collect the window events.
                let events: Box<[Event<V>]> = self
                    .events
                    .range(window.start_date_time..window.end_date_time)
                    .flat_map(|(_, events)| events)
                    .cloned()
                    .collect();

                // Take the opportunity to remove expired data from the other
                // members of Windows.
                let start_date_time_windows = self
                    .start_date_time_windows
                    .get_mut(&window.start_date_time)
                    .unwrap();
                start_date_time_windows.remove(&window);
                if start_date_time_windows.is_empty() {
                    self.start_date_time_windows.remove(&window.start_date_time);
                }

                // Remove all events that are older than the start of the first
                // window.
                match self.start_date_time_windows.keys().next() {
                    Some(first_date_time) => {
                        self.events = self.events.split_off(first_date_time);
                    }
                    None => {
                        self.events.clear();
                    }
                }

                (window.clone(), events)
            })
    }
}

struct WindowsProcessor<K, KS, V, A, M> {
    key_windows: HashMap<K, Windows<V>>,

    key_selector: KS,
    assigner: Arc<A>,
    merger: Arc<M>,
}

impl<K, KS, V, A, M> WindowsProcessor<K, KS, V, A, M>
where
    K: Eq + Hash + Clone + 'static,
    KS: Fn(&Event<V>) -> K,
    V: Clone,
    A: WindowAssigner<V>,
    M: WindowMerger,
{
    fn new(key_selector: KS, assigner: A, merger: M) -> Self {
        Self {
            key_windows: Default::default(),
            key_selector,
            assigner: Arc::new(assigner),
            merger: Arc::new(merger),
        }
    }

    fn process(
        &mut self,
        event: Event<V>,
    ) -> impl Iterator<Item = (K, Window, Box<[Event<V>]>)> + '_ {
        let key = (self.key_selector)(&event);
        let watermark_date_time = event.watermark_date_time.unwrap();

        let windows = self.key_windows.entry(key.clone()).or_default();

        // Determine which windows the event is supposed to be in and add it to
        // the windows container for that event's key.
        self.assigner
            .assign(&event)
            .for_each(|ref window| windows.add_window(window));

        windows.add_event(event);

        // It is possible that a new window created by the event means that
        // windows can be merged.
        self.merger
            .merge(windows.windows())
            .into_iter()
            .flat_map(|(from_windows, to_window)| {
                from_windows
                    .into_iter()
                    .map(move |from_window| (from_window, to_window))
            })
            .for_each(|(from_window, to_window)| {
                windows.replace(from_window, to_window);
            });

        // Iterate over all windows with the specified timestamp to see if any
        // windows have expired and can be returned.
        self.key_windows.iter_mut().flat_map(move |(key, windows)| {
            windows
                .events(watermark_date_time)
                .map(|(window, events)| (key.clone(), window, events))
        })
    }
}

struct WindowedDataStreamKeyState<V, KST> {
    user_key_state: KST,

    windows: Arc<Mutex<Windows<V>>>,
}

pub struct WindowedDataStream<V, KS, K, A, M>
where
    KS: Fn(&Event<V>) -> K,
    K: Hash + Eq + Send + Clone + 'static,
{
    data_stream: DataStream<V>,
    windows_processor: WindowsProcessor<K, KS, V, A, M>,
}

impl<V, KS, K, A, M> WindowedDataStream<V, KS, K, A, M>
where
    V: Clone + Send + Sync + 'static,
    KS: Fn(&Event<V>) -> K + Send + Sync + 'static,
    K: Hash + Eq + Send + Sync + Clone + 'static,
    A: WindowAssigner<V> + Send + Sync + 'static,
    M: WindowMerger + Send + Sync + 'static,
{
    pub fn process<O, FN, F>(self, process: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Box<[Event<V>]>, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.process_state(
            move |key, events, _, _, sender: Sender<O>| process(key, events, sender),
            (),
            |_| (),
        )
    }

    pub fn process_state<O, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state_fn: KSTF,
    ) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Box<[Event<V>]>, GST, KST, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
        KSTF: Fn(&K) -> KST + Send + Sync + 'static,
        KST: Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<O>>(1);

        self.data_stream.nodes.borrow_mut().spawn(async move {
            let mut key_states: HashMap<K, KST> = HashMap::default();

            let mut receiver = self.data_stream.receiver;
            let mut windows_processor = self.windows_processor;
            while let Some(event) = receiver.recv().await {
                for (key, window, events) in windows_processor.process(event) {
                    let key_state = key_states
                        .entry(key.clone())
                        .or_insert_with_key(&key_state_fn);
                    let sender = Sender {
                        sender: sender.clone(),
                    };

                    process(key, events, global_state.clone(), key_state.clone(), sender).await;
                }
            }
        });

        DataStream {
            nodes: Rc::clone(&self.data_stream.nodes),
            receiver,
        }
    }
}

pub struct Environment {
    nodes: Rc<RefCell<JoinSet<()>>>,
}

impl Default for Environment {
    fn default() -> Self {
        Self {
            nodes: Rc::new(RefCell::new(JoinSet::new())),
        }
    }
}

impl Environment {
    pub async fn execute(self) {
        let mut nodes = self.nodes.take();
        while let Some(result) = nodes.join_next().await {
            result.unwrap();
        }
    }

    pub fn add_source<S, V>(&self, source: S) -> DataStream<V>
    where
        S: Source<V> + 'static,
    {
        let (sender, receiver) = channel::<Event<V>>(1);

        let sender = Sender { sender };
        self.nodes.borrow_mut().spawn(source.run(sender));

        DataStream {
            nodes: Rc::clone(&self.nodes),
            receiver,
        }
    }
}

pub struct StdoutSink {}

impl<V> Sink<V> for StdoutSink
where
    V: Debug + Send + 'static,
{
    fn run<'a>(self, mut receiver: Receiver<V>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            while let Some(event) = receiver.receive().await {
                println!("value: {:?}", event.value);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use std::ops::Range;

    struct IncrementingSource<I> {
        iter: I,
    }

    impl<I, V> Source<V> for IncrementingSource<I>
    where
        V: Send + 'static,
        I: Iterator<Item = V> + Send + 'static,
    {
        fn run<'a>(self, mut sender: Sender<V>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                for event in self.iter.map(Event::new) {
                    sender.send(event).await;
                }
            })
        }
    }

    struct SliceValueSource<'a, V>(&'a [V]);

    impl<'a, V> Source<V> for SliceValueSource<'a, V>
    where
        V: Clone + Send + Sync + 'static,
    {
        fn run<'b>(self, mut sender: Sender<V>) -> BoxFuture<'b, ()> {
            let mut values = self.0.to_vec();
            Box::pin(async move {
                for event in values.drain(..).map(Event::new) {
                    sender.send(event).await;
                }
            })
        }
    }

    struct SliceValueAssertSink<'a, V>(&'a [V]);

    impl<'a, V> Sink<V> for SliceValueAssertSink<'a, V>
    where
        V: Debug + Clone + Send + PartialEq + 'static,
    {
        fn run<'b>(self, mut receiver: Receiver<V>) -> BoxFuture<'b, ()> {
            let mut values = self.0.to_vec();
            Box::pin(async move {
                let mut iter = values.drain(..).into_iter();
                while let Some(event) = receiver.receive().await {
                    assert_eq!(iter.next().unwrap(), event.value);
                }
            })
        }
    }

    struct SliceEventSource<V>(Box<[Event<V>]>);

    impl<V> Source<V> for SliceEventSource<V>
    where
        V: Clone + Send + Sync + 'static,
    {
        fn run<'b>(self, mut sender: Sender<V>) -> BoxFuture<'b, ()> {
            let events = self.0.to_vec();
            Box::pin(async move {
                for event in events.to_vec() {
                    sender.send(event).await;
                }
            })
        }
    }

    struct SliceEventAssertSink<V>(Box<[Event<V>]>);

    impl<V> Sink<V> for SliceEventAssertSink<V>
    where
        V: Debug + Clone + Send + PartialEq + 'static,
    {
        fn run<'b>(self, mut receiver: Receiver<V>) -> BoxFuture<'b, ()> {
            let mut events = self.0.to_vec();
            Box::pin(async move {
                let mut iter = events.drain(..);
                while let Some(event) = receiver.receive().await {
                    let expected = iter.next().unwrap();
                    assert_eq!(expected.processing_date_time, event.processing_date_time);
                    assert_eq!(expected.event_date_time, event.event_date_time);
                    assert_eq!(expected.watermark_date_time, event.watermark_date_time);
                    assert_eq!(expected.value, event.value);
                }

                assert!(iter.next().is_none());
            })
        }
    }

    struct IncrementingAssertSink<V> {
        range: Range<V>,
    }

    impl<V> IncrementingAssertSink<V>
    where
        V: PartialEq,
        Range<V>: Iterator<Item = V>,
    {
        fn create(range: Range<V>) -> Self {
            Self { range }
        }
    }

    impl<V> Sink<V> for IncrementingAssertSink<V>
    where
        V: Debug + Send + std::cmp::PartialEq + 'static,
        Range<V>: Iterator<Item = V>,
    {
        fn run<'a>(mut self, mut receiver: Receiver<V>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                while let Some(event) = receiver.receive().await {
                    assert_eq!(self.range.next().unwrap(), event.value);
                }

                assert!(self.range.next().is_none());
            })
        }
    }

    #[tokio::test]
    async fn data_stream_source_to_sink() {
        let env = Environment::default();

        let source = IncrementingSource { iter: (0..5) };
        let sink = IncrementingAssertSink::create(0..5);

        env.add_source(source).add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn data_stream_filter() {
        let env = Environment::default();

        let source = IncrementingSource { iter: (0..10) };
        let sink = IncrementingAssertSink::create(4..10);

        let stream = env.add_source(source);

        async fn above_three(event: &Event<usize>) -> bool {
            event.value > 3
        }

        stream.filter(above_three).add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn data_stream_process() {
        let env = Environment::default();

        let source = IncrementingSource { iter: (0..10) };
        let sink = IncrementingAssertSink::create(10..20);

        let stream = env.add_source(source);

        stream
            .process(|event, mut sender| async move {
                let value = event.value;
                sender.send(event.with_value(value + 10)).await;
            })
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn keyed_process_state() {
        let env = Environment::default();

        let source = SliceValueSource(&["a", "b", "a", "b"]);
        let sink = SliceValueAssertSink(&[(0, 0), (1, 0), (2, 1), (3, 1)]);

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .process_state(
                |_key, event, global_state, key_state: Arc<Mutex<CountState>>, mut sender| async move {
                    let mut global_state = global_state.lock().await;
                    let mut key_state = key_state.lock().await;

                    sender
                        .send(event.with_value((global_state.count, key_state.count)))
                        .await;
                    global_state.count += 1;
                    key_state.count += 1;
                },
                Arc::new(Mutex::new(CountState { count: 0 })),
                |_key| Arc::new(Mutex::new(CountState { count: 0 })),
            )
            .add_sink(sink);

        env.execute().await;
    }

    fn naive_date_time(hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2009, 10, 11).unwrap(),
            NaiveTime::from_hms_opt(hour, minute, 0).unwrap(),
        )
    }

    fn new_event<V>(value: V, hour: u32, minute: u32) -> Event<V> {
        let date_time = naive_date_time(hour, minute);
        Event {
            processing_date_time: date_time,
            event_date_time: Some(date_time),
            watermark_date_time: Some(date_time),
            value,
        }
    }

    #[test]
    fn event_time_session_window_processor_assigner() {
        let processor = EventTimeSessionWindowProcessor::with_timeout(Duration::minutes(10));

        let event = new_event(0, 12, 10);
        let windows: Box<[Window]> = processor.assign(&event).collect();

        assert_eq!(1, windows.len());

        let window = &windows[0];

        assert_eq!(window.start_date_time, event.event_date_time.unwrap());
        assert_eq!(
            window.end_date_time,
            event.event_date_time.unwrap() + Duration::minutes(10)
        );
    }

    #[test]
    fn event_time_session_window_processor_merger_not_mergeable() {
        let windows = [
            Window {
                start_date_time: naive_date_time(0, 0),
                end_date_time: naive_date_time(0, 2),
            },
            Window {
                start_date_time: naive_date_time(1, 0),
                end_date_time: naive_date_time(1, 2),
            },
        ];

        let processor = EventTimeSessionWindowProcessor::with_timeout(Duration::minutes(10));
        let merged = processor.merge(windows.iter());
        assert_eq!(2, merged.len());

        let merge = merged
            .iter()
            .find(|(_from_windows, to_window)| to_window == &windows[0])
            .unwrap();
        assert_eq!(1, merge.0.len());
        assert_eq!(windows[0], merge.0[0]);

        let merge = merged
            .iter()
            .find(|(_from_windows, to_window)| to_window == &windows[1])
            .unwrap();
        assert_eq!(1, merge.0.len());
        assert_eq!(windows[1], merge.0[0]);
    }

    #[test]
    fn event_time_session_window_processor_merger_mergeable() {
        let windows = [
            Window {
                start_date_time: naive_date_time(0, 0),
                end_date_time: naive_date_time(0, 8),
            },
            Window {
                start_date_time: naive_date_time(0, 8),
                end_date_time: naive_date_time(0, 12),
            },
        ];

        let processor = EventTimeSessionWindowProcessor::with_timeout(Duration::minutes(10));
        let merged = processor.merge(windows.iter());
        assert_eq!(1, merged.len());

        let (from_windows, to_window) = &merged[0];

        assert_eq!(
            to_window,
            &Window {
                start_date_time: naive_date_time(0, 0),
                end_date_time: naive_date_time(0, 12),
            }
        );

        assert_eq!(2, from_windows.len());

        assert!(from_windows.iter().any(|window| window == &windows[0]));
        assert!(from_windows.iter().any(|window| window == &windows[1]));
    }

    #[derive(Clone)]
    struct CountState {
        count: usize,
    }

    #[tokio::test]
    async fn global_process_state() {
        let env = Environment::default();

        let source = IncrementingSource { iter: (10..20) };
        let sink = IncrementingAssertSink::create(0..10);

        let stream = env.add_source(source);

        stream
            .process_state(
                |event, global_state, mut sender| async move {
                    let mut global_state = global_state.lock().await;
                    sender
                        .send(Event {
                            value: global_state.count,
                            ..event
                        })
                        .await;
                    global_state.count += 1;
                },
                Arc::new(Mutex::new(CountState { count: 0 })),
            )
            .add_sink(sink);
        env.execute().await;
    }

    #[tokio::test]
    async fn windowed_process_separate_events() {
        let env = Environment::default();

        let source =
            SliceEventSource([new_event(0_usize, 12, 10), new_event(1_usize, 12, 30)].into());

        let sink = SliceEventAssertSink([new_event(vec![0_usize], 12, 10)].into());

        env.add_source(source)
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowProcessor::with_timeout(
                Duration::minutes(10),
            ))
            .process(|_key, events, mut sender| async move {
                let grouped_event = Event {
                    event_date_time: events[0].event_date_time,
                    processing_date_time: events[0].processing_date_time,
                    watermark_date_time: events[0].watermark_date_time,
                    value: events
                        .iter()
                        .map(|event| event.value)
                        .collect::<Vec<usize>>(),
                };

                sender.send(grouped_event).await;
            })
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn windowed_process_joined_events() {
        let env = Environment::default();

        let source = SliceEventSource(
            [
                new_event(0_usize, 12, 10),
                new_event(0_usize, 12, 12),
                new_event(1_usize, 12, 30),
            ]
            .into(),
        );

        let sink = SliceEventAssertSink([new_event(vec![0_usize, 0_usize], 12, 10)].into());

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowProcessor::with_timeout(
                Duration::minutes(10),
            ))
            .process(|_key, events, mut sender| async move {
                let grouped_event = Event {
                    event_date_time: events[0].event_date_time,
                    processing_date_time: events[0].processing_date_time,
                    watermark_date_time: events[0].watermark_date_time,
                    value: events
                        .iter()
                        .map(|event| event.value)
                        .collect::<Vec<usize>>(),
                };

                sender.send(grouped_event).await;
            })
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn windowed_process_state_separate_events() {
        let env = Environment::default();

        let source = SliceEventSource(
            [
                new_event(0_usize, 12, 10),
                new_event(0_usize, 12, 30),
                new_event(0_usize, 12, 40),
                new_event(1_usize, 12, 41),
                new_event(1_usize, 12, 42),
            ]
            .into(),
        );

        let sink = SliceEventAssertSink(
            [
                new_event((0, 0), 12, 10),
                new_event((1, 1), 12, 30),
                new_event((2, 2), 12, 40),
                new_event((3, 0), 12, 41),
                new_event((4, 1), 12, 42),
            ]
            .into(),
        );

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowProcessor::with_timeout(
                Duration::minutes(10),
            ))
            .process_state(
                |_key,
                 events,
                 global_state: Arc<Mutex<CountState>>,
                 key_state: Arc<Mutex<CountState>>,
                 mut sender| async move {
                    let mut global_state = global_state.lock().await;
                    let mut key_state = key_state.lock().await;
                    let grouped_event = Event {
                        event_date_time: events[0].event_date_time,
                        processing_date_time: events[0].processing_date_time,
                        watermark_date_time: events[0].watermark_date_time,
                        value: (global_state.count, key_state.count),
                    };

                    global_state.count += 1;
                    key_state.count += 1;

                    sender.send(grouped_event).await;
                },
                Arc::new(Mutex::new(CountState { count: 0 })),
                |_key| Arc::new(Mutex::new(CountState { count: 0 })),
            )
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn windowed_process_state_joined_events() {
        let env = Environment::default();

        let source = SliceEventSource(
            [
                new_event(0_usize, 12, 10),
                new_event(0_usize, 12, 12),
                new_event(0_usize, 12, 13),
                new_event(1_usize, 12, 41),
                new_event(1_usize, 12, 42),
                new_event(2_usize, 12, 53),
            ]
            .into(),
        );

        let sink =
            SliceEventAssertSink([new_event((0, 0), 12, 10), new_event((1, 0), 12, 41)].into());

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowProcessor::with_timeout(
                Duration::minutes(10),
            ))
            .process_state(
                |_key,
                 events,
                 global_state: Arc<Mutex<CountState>>,
                 key_state: Arc<Mutex<CountState>>,
                 mut sender| async move {
                    let mut global_state = global_state.lock().await;
                    let mut key_state = key_state.lock().await;
                    let grouped_event = Event {
                        event_date_time: events[0].event_date_time,
                        processing_date_time: events[0].processing_date_time,
                        watermark_date_time: events[0].watermark_date_time,
                        value: (global_state.count, key_state.count),
                    };

                    global_state.count += 1;
                    key_state.count += 1;

                    sender.send(grouped_event).await;
                },
                Arc::new(Mutex::new(CountState { count: 0 })),
                |_key| Arc::new(Mutex::new(CountState { count: 0 })),
            )
            .add_sink(sink);

        env.execute().await;
    }

    #[test]
    fn windows_multiple_events() {
        let mut windows = Windows::default();
        windows.add_window(&Window {
            start_date_time: naive_date_time(12, 0),
            end_date_time: naive_date_time(12, 2),
        });
        windows.add_window(&Window {
            start_date_time: naive_date_time(12, 10),
            end_date_time: naive_date_time(12, 20),
        });

        [
            new_event(0_usize, 12, 8),
            new_event(1_usize, 12, 10),
            new_event(2_usize, 12, 10),
            new_event(3_usize, 12, 12),
        ]
        .into_iter()
        .for_each(|event| {
            windows.add_event(event);
        });

        let watermark_date_time = naive_date_time(12, 9);

        let mut window_events_vec = Vec::new();
        for (window, events) in windows.events(watermark_date_time) {
            window_events_vec.push((window, events));
        }

        assert_eq!(1, window_events_vec.len());
        assert_eq!(
            Window {
                start_date_time: naive_date_time(12, 0),
                end_date_time: naive_date_time(12, 2),
            },
            window_events_vec[0].0
        );
        assert!(window_events_vec[0].1.is_empty());
        assert_eq!(2, windows.events.len());

        assert_eq!(
            3,
            windows
                .events
                .values()
                .map(|events| events)
                .flatten()
                .count()
        );
        assert_eq!(1, windows.start_date_time_windows.len());
        assert_eq!(1, windows.end_date_time_windows.len());

        let watermark_date_time = naive_date_time(12, 10);

        let mut window_events_vec = Vec::new();
        for (window, events) in windows.events(watermark_date_time) {
            window_events_vec.push((window, events));
        }

        assert_eq!(0, window_events_vec.len());

        let watermark_date_time = naive_date_time(12, 20);

        let mut window_events_vec = Vec::new();
        for (window, events) in windows.events(watermark_date_time) {
            window_events_vec.push((window, events));
        }

        assert_eq!(1, window_events_vec.len());
        assert_eq!(
            Window {
                start_date_time: naive_date_time(12, 10),
                end_date_time: naive_date_time(12, 20),
            },
            window_events_vec[0].0,
        );
        assert_eq!(3, window_events_vec[0].1.len());
        assert_eq!(new_event(1_usize, 12, 10), window_events_vec[0].1[0]);
        assert_eq!(new_event(2_usize, 12, 10), window_events_vec[0].1[1]);
        assert_eq!(new_event(3_usize, 12, 12), window_events_vec[0].1[2]);

        assert!(windows.events.is_empty());
        assert!(windows.start_date_time_windows.is_empty());
        assert!(windows.end_date_time_windows.is_empty());
    }

    #[test]
    fn windows_events() {
        let mut windows = Windows::default();
        windows.add_window(&Window {
            start_date_time: naive_date_time(12, 0),
            end_date_time: naive_date_time(12, 2),
        });
        windows.add_window(&Window {
            start_date_time: naive_date_time(12, 10),
            end_date_time: naive_date_time(12, 20),
        });
        [
            new_event(0_usize, 12, 8),
            new_event(1_usize, 12, 10),
            new_event(2_usize, 12, 10),
            new_event(3_usize, 12, 12),
        ]
        .into_iter()
        .for_each(|event| {
            windows.add_event(event);
        });

        let (window, events) = windows.events(naive_date_time(12, 10)).next().unwrap();
        assert_eq!(
            Window {
                start_date_time: naive_date_time(12, 0),
                end_date_time: naive_date_time(12, 2),
            },
            window
        );
        assert!(events.is_empty());

        let (window, events) = windows.events(naive_date_time(12, 20)).next().unwrap();
        assert_eq!(
            Window {
                start_date_time: naive_date_time(12, 10),
                end_date_time: naive_date_time(12, 20),
            },
            window
        );

        assert_eq!(3, events.len());
        assert_eq!(new_event(1_usize, 12, 10), events[0]);
        assert_eq!(new_event(2_usize, 12, 10), events[1]);
        assert_eq!(new_event(3_usize, 12, 12), events[2]);
    }

    struct NonAssigner;

    impl<V> WindowAssigner<V> for NonAssigner {
        type Iterator = std::iter::Empty<Window>;
        fn assign(&self, event: &Event<V>) -> Self::Iterator {
            std::iter::empty()
        }
    }

    struct NonMerger;

    impl WindowMerger for NonMerger {
        fn merge<'a>(
            &'a self,
            windows: impl Iterator<Item = &'a Window>,
        ) -> Box<[(Box<[Window]>, Window)]> {
            Box::new([])
        }
    }

    #[test]
    fn windows_processor_empty() {
        let mut processor =
            WindowsProcessor::new(|event: &Event<usize>| event.value, NonAssigner, NonMerger);

        assert!(processor
            .process(new_event(0_usize, 12, 10))
            .next()
            .is_none());
    }

    struct FixedAssigner {
        window: Window,
    }

    impl<V> WindowAssigner<V> for FixedAssigner {
        type Iterator = std::iter::Once<Window>;
        fn assign(&self, event: &Event<V>) -> Self::Iterator {
            std::iter::once(self.window.clone())
        }
    }

    #[test]
    fn windows_processor_different_key_expiry() {
        let window = Window {
            start_date_time: naive_date_time(12, 10),
            end_date_time: naive_date_time(12, 20),
        };
        let mut processor = WindowsProcessor::new(
            |event: &Event<usize>| event.value,
            FixedAssigner {
                window: window.clone(),
            },
            NonMerger,
        );

        assert!(processor
            .process(new_event(0_usize, 12, 10))
            .next()
            .is_none());

        assert!(processor
            .process(new_event(1_usize, 12, 11))
            .next()
            .is_none());

        let key_events: Vec<(usize, Window, Box<[Event<usize>]>)> =
            processor.process(new_event(2_usize, 12, 30)).collect();

        assert_eq!(3, key_events.len());

        let key_event = key_events
            .iter()
            .filter(|(key, _, _)| *key == 0)
            .next()
            .unwrap();
        assert_eq!(0_usize, key_event.0);
        assert_eq!(window, key_event.1);
        assert_eq!(1, key_event.2.len());
        assert_eq!(new_event(0_usize, 12, 10), key_event.2[0]);

        let key_event = key_events
            .iter()
            .filter(|(key, _, _)| *key == 1)
            .next()
            .unwrap();
        assert_eq!(1_usize, key_event.0);
        assert_eq!(window, key_event.1);
        assert_eq!(1, key_event.2.len());
        assert_eq!(new_event(1_usize, 12, 11), key_event.2[0]);

        // No events in the window because the event is outside the window.
        let key_event = key_events
            .iter()
            .filter(|(key, _, _)| *key == 2)
            .next()
            .unwrap();
        assert_eq!(2_usize, key_event.0);
        assert_eq!(window, key_event.1);
        assert_eq!(0, key_event.2.len());
    }
}
