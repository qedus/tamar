use chrono::{Duration, NaiveDateTime, Utc};
use futures::{future::BoxFuture, Future};
use std::{
    cell::RefCell,
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    rc::Rc,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, mpsc::channel, Mutex},
    task::JoinSet,
};

#[derive(Clone, Debug)]
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
        S: Sink<V> + Send + 'static,
    {
        self.nodes.borrow_mut().spawn(async move {
            let receiver = self.receiver;

            sink.run(Receiver { receiver }).await;
        });
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
        let process = Arc::new(process);
        self.process_state(
            move |event, _, sender: Sender<O>| {
                let process = Arc::clone(&process);
                async move { process(event, sender).await }
            },
            (),
        )
    }

    pub fn process_state<O, FN, F, GST>(self, process: FN, global_state: GST) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(Event<V>, Arc<Mutex<GST>>, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<O>>(1);

        self.nodes.borrow_mut().spawn(async move {
            let global_state = Arc::new(Mutex::new(global_state));
            let mut receiver = self.receiver;
            while let Some(event) = receiver.recv().await {
                let sender = Sender {
                    sender: sender.clone(),
                };
                let global_state = Arc::clone(&global_state);
                process(event, global_state, sender).await;
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
    V: Send + Sync + 'static,
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
        let process = Arc::new(process);
        self.process_state(
            move |key, event, _, _, sender: Sender<O>| {
                let process = Arc::clone(&process);
                async move { process(key, event, sender).await }
            },
            (),
            |_| (),
        )
    }

    pub fn process_state<O, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state: KSTF,
    ) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Event<V>, Arc<Mutex<GST>>, Arc<Mutex<KST>>, Sender<O>) -> F
            + Send
            + Sync
            + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Send + Sync + 'static,
        KSTF: Fn(K) -> KST + Send + Sync + 'static,
        KST: Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<O>>(1);

        self.data_stream.nodes.borrow_mut().spawn(async move {
            let global_state = Arc::new(Mutex::new(global_state));
            let mut key_states: HashMap<K, Arc<Mutex<KST>>> = HashMap::default();

            let mut receiver = self.data_stream.receiver;
            let selector = self.selector;
            while let Some(event) = receiver.recv().await {
                let key = selector(&event);
                let global_state = Arc::clone(&global_state);

                let key_state_fn_key = key.clone();
                let key_state_fn = || Arc::new(Mutex::new(key_state(key_state_fn_key)));
                let key_state = key_states.entry(key.clone()).or_insert_with(key_state_fn);
                let key_state = Arc::clone(key_state);
                let sender = Sender {
                    sender: sender.clone(),
                };
                process(key, event, global_state, key_state, sender).await;
            }
        });

        DataStream {
            nodes: Rc::clone(&self.data_stream.nodes),
            receiver,
        }
    }

    pub fn window<P>(self, processor: P) -> WindowedDataStream<V, KS, K, P, P> {
        let processor = Arc::new(processor);
        WindowedDataStream {
            keyed_data_stream: self,
            assigner: Arc::clone(&processor),
            merger: Arc::clone(&processor),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Window {
    start_date_time: NaiveDateTime,
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
    fn assign(&self, event: &Event<V>) -> Box<[Window]>;
}

pub trait WindowMerger {
    fn merge(&self, windows: &mut [Window]) -> Box<[(Window, Box<[Window]>)]>;
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
    fn assign(&self, event: &Event<V>) -> Box<[Window]> {
        Box::new([Window {
            start_date_time: event.event_date_time.unwrap(),
            end_date_time: event.event_date_time.unwrap() + self.timeout,
        }])
    }
}

impl WindowMerger for EventTimeSessionWindowProcessor {
    fn merge(&self, windows: &mut [Window]) -> Box<[(Window, Box<[Window]>)]> {
        windows.sort_by(|left, right| left.start_date_time.cmp(&right.start_date_time));

        let mut merged = Vec::with_capacity(windows.len());
        let mut current_merge: Option<(Window, HashSet<Window>)> = None;

        for window in windows.iter() {
            match current_merge {
                None => {
                    let mut merged_windows = HashSet::default();
                    merged_windows.insert(window.clone());
                    current_merge = Some((window.clone(), merged_windows));
                }
                Some(ref mut merge) => {
                    if merge.0.intersects(window) {
                        merge.0 = merge.0.union(window);
                        merge.1.insert(window.clone());
                    } else {
                        let mut merged_windows = HashSet::default();
                        merged_windows.insert(window.clone());
                        merged.push(
                            current_merge
                                .replace((window.clone(), merged_windows))
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
            .map(|(window, merged_windows)| {
                (
                    window,
                    merged_windows
                        .into_iter()
                        .collect::<Vec<Window>>()
                        .into_boxed_slice(),
                )
            })
            .collect::<Vec<(Window, Box<[Window]>)>>()
            .into_boxed_slice()
    }
}

struct WindowsKeyState<V, KST> {
    window_events: HashMap<Window, Vec<Arc<Event<V>>>>,
    key_state: Arc<Mutex<KST>>,
}

pub struct WindowedDataStream<V, KS, K, A, M>
where
    KS: Fn(&Event<V>) -> K,
    K: Hash + Eq + Send + Clone + 'static,
{
    keyed_data_stream: KeyedDataStream<V, KS, K>,
    assigner: Arc<A>,
    merger: Arc<M>,
}

impl<V, KS, K, A, M> WindowedDataStream<V, KS, K, A, M>
where
    V: Clone + Send + Sync + 'static,
    KS: Fn(&Event<V>) -> K + Send + 'static,
    K: Hash + Eq + Send + Clone + 'static,
    A: WindowAssigner<V> + Send + Sync + 'static,
    M: WindowMerger + Send + Sync + 'static,
{
    pub fn process<O, FN, F>(self, process: FN) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Box<[Event<V>]>, Sender<O>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let process = Arc::new(process);
        self.process_state(
            move |key, events, _, _, sender: Sender<O>| {
                let process = Arc::clone(&process);
                async move { process(key, events, sender).await }
            },
            (),
            |_| (),
        )
    }

    async fn process_windows<O, FN, F, GST, KST>(
        assigner: Arc<A>,
        merger: Arc<M>,
        process: Arc<FN>,
        key: K,
        event: Event<V>,
        global_state: Arc<Mutex<GST>>,
        key_state: Arc<Mutex<WindowsKeyState<V, KST>>>,
        sender: Sender<O>,
    ) where
        O: Send + 'static,
        FN: Fn(K, Box<[Event<V>]>, Arc<Mutex<GST>>, Arc<Mutex<KST>>, Sender<O>) -> F
            + Send
            + Sync
            + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Send + Sync + 'static,
        KST: Send + Sync + 'static,
    {
        let watermark_date_time = event.watermark_date_time.unwrap();
        let event = Arc::new(event);
        let mut windows_key_state = key_state.lock().await;

        for window in assigner.assign(&event).iter() {
            windows_key_state
                .window_events
                .entry(window.clone())
                .or_default()
                .push(Arc::clone(&event));
        }

        let mut windows = windows_key_state
            .window_events
            .keys()
            .cloned()
            .collect::<Vec<Window>>()
            .into_boxed_slice();

        // Merge windows.
        merger
            .merge(&mut windows)
            .iter()
            .flat_map(|(to_window, from_windows)| {
                from_windows
                    .iter()
                    .map(|from_window| (to_window.clone(), from_window))
            })
            .for_each(|(to_window, from_window)| {
                let mut events = windows_key_state.window_events.remove(from_window).unwrap();
                windows_key_state
                    .window_events
                    .entry(to_window)
                    .or_default()
                    .append(&mut events);
            });

        // Merged windows.
        let windows = windows_key_state
            .window_events
            .keys()
            .cloned()
            .collect::<Vec<Window>>()
            .into_boxed_slice();

        for window in windows.iter() {
            if window.end_date_time >= watermark_date_time {
                continue;
            }

            let key = key.clone();
            let events = windows_key_state
                .window_events
                .remove(window)
                .unwrap()
                .into_iter()
                .map(|event| Arc::try_unwrap(event).unwrap_or_else(|arc| (*arc).clone()))
                .collect::<Vec<Event<V>>>()
                .into_boxed_slice();

            let global_state = Arc::clone(&global_state);
            let key_state = Arc::clone(&windows_key_state.key_state);
            let sender = sender.clone();

            process(key, events, global_state, key_state, sender).await;
        }
    }

    pub fn process_state<O, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state: KSTF,
    ) -> DataStream<O>
    where
        O: Send + 'static,
        FN: Fn(K, Box<[Event<V>]>, Arc<Mutex<GST>>, Arc<Mutex<KST>>, Sender<O>) -> F
            + Send
            + Sync
            + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Send + Sync + 'static,
        KSTF: Fn(K) -> KST + Send + Sync + 'static,
        KST: Send + Sync + 'static,
    {
        let process = Arc::new(process);
        self.keyed_data_stream.process_state(
            move |key,
                  event,
                  global_state,
                  key_state: Arc<Mutex<WindowsKeyState<V, KST>>>,
                  sender| {
                let assigner = Arc::clone(&self.assigner);
                let merger = Arc::clone(&self.merger);
                let process = Arc::clone(&process);

                Self::process_windows(
                    assigner,
                    merger,
                    process,
                    key,
                    event,
                    global_state,
                    key_state,
                    sender,
                )
            },
            global_state,
            move |key| WindowsKeyState {
                window_events: HashMap::<Window, Vec<Arc<Event<V>>>>::new(),
                key_state: Arc::new(Mutex::new(key_state(key))),
            },
        )
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
        S: Source<V> + Send + Sync + 'static,
        V: Send + 'static,
    {
        let (sender, receiver) = channel::<Event<V>>(1);

        let mut nodes = self.nodes.borrow_mut();
        nodes.spawn(async move {
            let sender = Sender { sender };
            source.run(sender).await;
        });

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
                let mut iter = events.drain(..).into_iter();
                while let Some(event) = receiver.receive().await {
                    let expected = iter.next().unwrap();
                    assert_eq!(expected.processing_date_time, event.processing_date_time);
                    assert_eq!(expected.event_date_time, event.event_date_time);
                    assert_eq!(expected.watermark_date_time, event.watermark_date_time);
                    assert_eq!(expected.value, event.value);
                }
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
            })
        }
    }

    #[tokio::test]
    async fn filter() {
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
    async fn process() {
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
                CountState { count: 0 },
                |_key| CountState { count: 0 },
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
        let windows = processor.assign(&event);

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
        let mut windows = [
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
        let merged = processor.merge(&mut windows);
        assert_eq!(2, merged.len());

        let merge = merged
            .iter()
            .find(|(to_window, _from_windows)| to_window == &windows[0])
            .unwrap();
        assert_eq!(1, merge.1.len());
        assert_eq!(windows[0], merge.1[0]);

        let merge = merged
            .iter()
            .find(|(to_window, _from_windows)| to_window == &windows[1])
            .unwrap();
        assert_eq!(1, merge.1.len());
        assert_eq!(windows[1], merge.1[0]);
    }

    #[test]
    fn event_time_session_window_processor_merger_mergeable() {
        let mut windows = [
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
        let merged = processor.merge(&mut windows);
        assert_eq!(1, merged.len());

        let (to_window, from_windows) = &merged[0];

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
                CountState { count: 0 },
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
                CountState { count: 0 },
                |_key| CountState { count: 0 },
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
                CountState { count: 0 },
                |_key| CountState { count: 0 },
            )
            .add_sink(sink);

        env.execute().await;
    }
}
