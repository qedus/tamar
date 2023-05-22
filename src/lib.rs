use std::{
    cell::RefCell,
    cmp::PartialEq,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    ops::{Add, Bound::Included},
    rc::Rc,
    sync::Arc,
};

use chrono::{Duration, NaiveDateTime, Utc};
use futures::{future::BoxFuture, Future};
use tokio::{
    join,
    sync::{mpsc, mpsc::channel},
    task::JoinSet,
};

#[derive(Clone, PartialEq, Debug)]
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

    pub fn with_value<VO>(&self, value: VO) -> Event<VO> {
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

    pub fn map<VO, FN, F>(self, map: FN) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(Event<V>) -> F + Send + Sync + 'static,
        F: Future<Output = Event<VO>> + Send + 'static,
    {
        let map = Arc::new(map);
        self.process_state(
            move |event, _global_state, mut sender: Sender<VO>| {
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
            move |event, _global_state, mut sender: Sender<V>| {
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

    fn process<VO, FN, F>(self, process: FN) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(Event<V>, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.process_state(
            move |event, _global_state, sender: Sender<VO>| process(event, sender),
            (),
        )
    }

    pub fn process_state<VO, FN, F, GST>(self, process: FN, global_state: GST) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(Event<V>, GST, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<VO>>(1);

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

impl<V> DataStream<V>
where
    V: Send + Sync + Clone + 'static,
{
    pub fn split(self) -> (Self, Self) {
        let (left_sender, left_receiver) = channel::<Event<V>>(1);
        let (right_sender, right_receiver) = channel::<Event<V>>(1);

        self.nodes.borrow_mut().spawn(async move {
            let mut receiver = self.receiver;
            while let Some(event) = receiver.recv().await {
                let (left_result, right_result) = join!(
                    left_sender.send(event.clone()),
                    right_sender.send(event.clone())
                );

                if left_result.is_err() || right_result.is_err() {
                    return;
                }
            }
        });

        (
            Self {
                nodes: Rc::clone(&self.nodes),
                receiver: left_receiver,
            },
            Self {
                nodes: Rc::clone(&self.nodes),
                receiver: right_receiver,
            },
        )
    }
}

pub struct KeyedDataStream<V, KS, K>
where
    KS: Fn(&Event<V>) -> K,
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

    pub fn map<VO, FN, F>(self, map: FN) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(K, Event<V>) -> F + Send + Sync + 'static,
        F: Future<Output = Event<VO>> + Send + 'static,
    {
        let map = Arc::new(map);
        self.process_state(
            move |key, event, _global_state, _key_state, mut sender: Sender<VO>| {
                let map = Arc::clone(&map);
                async move {
                    let event = map(key, event).await;
                    sender.send(event).await;
                }
            },
            (),
            |_key| (),
        )
    }

    pub fn filter<FN>(self, filter: FN) -> DataStream<V>
    where
        FN: for<'a> AsyncDoubleArgFn<K, &'a Event<V>, Output = bool> + Send + Sync + 'static,
    {
        let filter = Arc::new(filter);
        self.process_state(
            move |key, event, _global_state, _key_state, mut sender: Sender<V>| {
                let filter = Arc::clone(&filter);
                async move {
                    if filter(key, &event).await {
                        sender.send(event).await;
                    }
                }
            },
            (),
            |_key| (),
        )
    }

    fn process<VO, FN, F>(self, process: FN) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(K, Event<V>, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.process_state(
            move |key, event, _global_state, _key_state, sender: Sender<VO>| {
                process(key, event, sender)
            },
            (),
            |_key| (),
        )
    }

    pub fn process_state<VO, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state_fn: KSTF,
    ) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(K, Event<V>, GST, KST, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
        KSTF: Fn(&K) -> KST + Send + Sync + 'static,
        KST: Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::<Event<VO>>(1);

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

    pub fn window<WF>(self, factory: WF) -> WindowedDataStream<V, KS, WF> {
        let data_stream = self.data_stream;
        let selector = self.selector;
        WindowedDataStream {
            data_stream,
            selector,
            factory,
        }
    }
}

impl<V, KS, K> KeyedDataStream<V, KS, K>
where
    V: Send + Sync + Clone + 'static,
    KS: Fn(&Event<V>) -> K + Clone,
{
    pub fn split(self) -> (Self, Self) {
        let (left_data_stream, right_data_stream) = self.data_stream.split();

        (
            Self {
                data_stream: left_data_stream,
                selector: self.selector.clone(),
            },
            Self {
                data_stream: right_data_stream,
                selector: self.selector.clone(),
            },
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct Window {
    // Contains all events that are >= start_date_time.
    start_date_time: NaiveDateTime,

    // Contains all events that are < end_date_time.
    end_date_time: NaiveDateTime,
}

type WindowEvents<V> = Box<[(Window, Box<[Event<V>]>)]>;

pub trait WindowProcessor<V> {
    fn add_event<'a>(&mut self, event: Event<V>) -> BoxFuture<'a, ()>;
    fn trigger<'a>(&mut self, watermark_date_time: NaiveDateTime)
        -> BoxFuture<'a, WindowEvents<V>>;
}

pub trait WindowAggregator<VO> {
    fn add_event<'a, VI, ACC>(&mut self, accumulator: &ACC, event: Event<VI>) -> BoxFuture<'a, ()>
    where
        ACC: Fn(VI) -> VO,
        VO: Add<Output = VO>;
    fn trigger<'a>(
        &mut self,
        watermark_date_time: NaiveDateTime,
    ) -> BoxFuture<'a, WindowEvents<VO>>;
}

pub trait WindowFactory {
    type Processor<V>: WindowProcessor<V>
    where
        V: Send + 'static;
    type Aggregator<V>: WindowAggregator<V>
    where
        V: Send + 'static;

    fn processor<V>(&self) -> Self::Processor<V>
    where
        V: Send + 'static;
    fn aggregator<V>(&self) -> Self::Aggregator<V>
    where
        V: Send + 'static;
}

struct EventTimeWindowMemoryStore<V> {
    timeout: Duration,

    // Key is the event_date_time of the event.
    events: BTreeMap<NaiveDateTime, Vec<Event<V>>>,

    // Key is the end_date_time of the window and value is the start_date_time.
    windows: BTreeMap<NaiveDateTime, NaiveDateTime>,
}

impl<V> EventTimeWindowMemoryStore<V> {
    fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            events: Default::default(),
            windows: Default::default(),
        }
    }

    fn add_event(&mut self, event: Event<V>) -> Window {
        let event_date_time = event.event_date_time.unwrap();
        self.events.entry(event_date_time).or_default().push(event);

        let mut windows_iter = self
            .windows
            .range((event_date_time - self.timeout)..)
            .map(|(end_date_time, start_date_time)| Window {
                end_date_time: *end_date_time,
                start_date_time: *start_date_time,
            })
            .fuse();
        let first_window = windows_iter.next();
        let second_window = windows_iter.next();

        // Are there any windows to possibly join with?
        if first_window.is_none() {
            // Create a window that is the smallest possible size to contain the event. We
            // can't have a window that starts and ends on event_date_time as we have
            // defined a window >= start_date_time and < end_date_time.
            let window = Window {
                start_date_time: event_date_time,
                end_date_time: event_date_time + Duration::nanoseconds(1),
            };
            self.windows
                .insert(window.end_date_time, window.start_date_time);
            return window;
        }

        let first_window = first_window.unwrap();

        // Is the first window before the event?
        // We know it is going to be within the timeout period due to the range
        // query above.
        if first_window.end_date_time < event_date_time {
            self.windows.remove(&first_window.end_date_time);

            // If there are no other windows to merge then expand the first
            // window to fit.
            if second_window.is_none() {
                let window = Window {
                    start_date_time: first_window.start_date_time,
                    end_date_time: event_date_time + Duration::nanoseconds(1),
                };
                self.windows
                    .insert(window.end_date_time, window.start_date_time);
                return window;
            }

            let second_window = second_window.unwrap();

            // Is the second window outside our timeout?
            if second_window.start_date_time > event_date_time + self.timeout {
                let window = Window {
                    start_date_time: first_window.start_date_time,
                    end_date_time: event_date_time + Duration::nanoseconds(1),
                };
                self.windows
                    .insert(window.end_date_time, window.start_date_time);
                return window;
            }

            // Merge the first and second windows.
            self.windows.remove(&second_window.end_date_time);
            self.windows
                .insert(second_window.end_date_time, first_window.start_date_time);
            return Window {
                start_date_time: first_window.start_date_time,
                end_date_time: second_window.end_date_time,
            };
        }

        // Does the first window overlap the event?
        if first_window.end_date_time > event_date_time
            && first_window.start_date_time <= event_date_time
        {
            // Just use the same window.
            return Window {
                start_date_time: first_window.start_date_time,
                end_date_time: first_window.end_date_time,
            };
        }

        // Is the first window too far in the future?
        if first_window.start_date_time > event_date_time + self.timeout {
            let window = Window {
                start_date_time: event_date_time,
                end_date_time: event_date_time + Duration::nanoseconds(1),
            };
            self.windows
                .insert(window.end_date_time, window.start_date_time);
            return window;
        }

        self.windows
            .insert(first_window.end_date_time, event_date_time);
        Window {
            start_date_time: event_date_time,
            end_date_time: first_window.end_date_time,
        }
    }

    /// Triggers windows to be returned, if any are available. The trigger is
    /// contingent on the following assumptions:
    /// - The trigger gets called for all events, not just those in the same key.
    /// - The watermark is always a fixed offset from event times.
    fn trigger(&mut self, watermark_date_time: NaiveDateTime) -> WindowEvents<V> {
        let windows = self
            .windows
            .split_off(&(watermark_date_time - self.timeout));
        let triggered_windows: Vec<Window> = std::mem::replace(&mut self.windows, windows)
            .into_iter()
            .map(|(end_date_time, start_date_time)| Window {
                start_date_time,
                end_date_time,
            })
            .collect();

        if triggered_windows.is_empty() {
            return Box::new([]);
        }

        let events = self.events.split_off(&(watermark_date_time - self.timeout));
        let mut triggered_events = std::mem::replace(&mut self.events, events)
            .into_values()
            .flatten();

        let mut triggered_sessions: Vec<Vec<Event<V>>> = Vec::new();

        for window in &triggered_windows {
            for event in triggered_events.by_ref() {
                if triggered_sessions.is_empty() {
                    triggered_sessions.push(vec![event]);
                    continue;
                }

                if event.event_date_time.unwrap() > window.end_date_time {
                    triggered_sessions.push(vec![event]);
                    break;
                }

                triggered_sessions.last_mut().unwrap().push(event);
            }
        }

        triggered_windows
            .into_iter()
            .zip(
                triggered_sessions
                    .into_iter()
                    .map(|events| events.into_boxed_slice()),
            )
            .collect::<Vec<(Window, Box<[Event<V>]>)>>()
            .into_boxed_slice()
    }
}

pub struct EventTimeSessionWindowProcessor<V> {
    store: EventTimeWindowMemoryStore<V>,
}

impl<V> WindowProcessor<V> for EventTimeSessionWindowProcessor<V>
where
    V: Send + 'static,
{
    fn add_event<'a>(&mut self, event: Event<V>) -> BoxFuture<'a, ()> {
        self.store.add_event(event);

        // async not needed so return empty response.
        Box::pin(async {})
    }

    fn trigger<'a>(
        &mut self,
        watermark_date_time: NaiveDateTime,
    ) -> BoxFuture<'a, WindowEvents<V>> {
        let window_events = self.store.trigger(watermark_date_time);
        Box::pin(async move { window_events })
    }
}

fn event_reducer<V>(left: Event<V>, right: Event<V>) -> Event<V>
where
    V: Add<Output = V>,
{
    let processing_date_time = left.processing_date_time.max(right.processing_date_time);
    let event_date_time = left.event_date_time.max(right.event_date_time);
    let watermark_date_time = left.watermark_date_time.max(right.watermark_date_time);
    let value = left.value + right.value;

    Event {
        processing_date_time,
        event_date_time,
        watermark_date_time,
        value,
    }
}

pub struct EventTimeSessionWindowAggregator<VO> {
    store: EventTimeWindowMemoryStore<VO>,
}

impl<VO> WindowAggregator<VO> for EventTimeSessionWindowAggregator<VO>
where
    VO: Send + 'static,
{
    fn add_event<'a, VI, ACC>(&mut self, accumulator: &ACC, event: Event<VI>) -> BoxFuture<'a, ()>
    where
        ACC: Fn(VI) -> VO,
        VO: Add<Output = VO>,
    {
        let event_copy = event.with_value(());
        let value = accumulator(event.value);
        let event = event_copy.with_value(value);

        let window = self.store.add_event(event);

        let mut event_date_times = self
            .store
            .events
            .range((
                Included(window.start_date_time),
                Included(window.end_date_time),
            ))
            .map(|(event_date_time, _event)| event_date_time)
            .cloned()
            .collect::<Vec<NaiveDateTime>>();

        let mut events: Vec<Event<VO>> = Vec::with_capacity(event_date_times.len());
        for event_date_time in event_date_times.drain(..) {
            events.extend(self.store.events.remove(&event_date_time).unwrap());
        }

        if let Some(aggregated_event) = events.drain(..).reduce(event_reducer) {
            self.store.add_event(aggregated_event);
        }

        // Async not needed so return empty response.
        Box::pin(async {})
    }

    fn trigger<'a>(
        &mut self,
        watermark_date_time: NaiveDateTime,
    ) -> BoxFuture<'a, WindowEvents<VO>> {
        let window_events = self.store.trigger(watermark_date_time);
        Box::pin(async move { window_events })
    }
}

#[derive(Clone)]
pub struct EventTimeSessionWindowFactory {
    timeout: Duration,
}

impl EventTimeSessionWindowFactory {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl WindowFactory for EventTimeSessionWindowFactory {
    type Processor<V> = EventTimeSessionWindowProcessor<V> where V: Send + 'static;
    type Aggregator<V> = EventTimeSessionWindowAggregator<V> where V: Send + 'static;

    fn processor<V>(&self) -> Self::Processor<V>
    where
        V: Send + 'static,
    {
        Self::Processor {
            store: EventTimeWindowMemoryStore::with_timeout(self.timeout),
        }
    }

    fn aggregator<V>(&self) -> Self::Aggregator<V>
    where
        V: Send + 'static,
    {
        Self::Aggregator {
            store: EventTimeWindowMemoryStore::with_timeout(self.timeout),
        }
    }
}

pub struct WindowedDataStream<VI, KS, WF> {
    data_stream: DataStream<VI>,
    selector: KS,
    factory: WF,
}

impl<VI, KS, K, WF> WindowedDataStream<VI, KS, WF>
where
    VI: Send + Sync + 'static,
    KS: Fn(&Event<VI>) -> K + Send + Sync + 'static,
    K: Hash + Eq + Send + Sync + Clone + 'static,
    WF: WindowFactory + Send + 'static,
{
    pub fn process<VO, FN, F>(self, process: FN) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(K, Box<[Event<VI>]>, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        WF::Processor<VI>: Send,
    {
        self.process_state(
            move |key, events, _global_state, _key_state, sender: Sender<VO>| {
                process(key, events, sender)
            },
            (),
            |_key| (),
        )
    }

    pub fn process_state<VO, FN, F, GST, KSTF, KST>(
        self,
        process: FN,
        global_state: GST,
        key_state_fn: KSTF,
    ) -> DataStream<VO>
    where
        VO: Send + 'static,
        FN: Fn(K, Box<[Event<VI>]>, GST, KST, Sender<VO>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
        GST: Clone + Send + Sync + 'static,
        KSTF: Fn(&K) -> KST + Send + Sync + 'static,
        KST: Clone + Send + Sync + 'static,
        WF::Processor<VI>: Send,
    {
        let (sender, receiver) = channel::<Event<VO>>(1);

        self.data_stream.nodes.borrow_mut().spawn(async move {
            let mut key_states: HashMap<K, KST> = HashMap::default();
            let mut processors: HashMap<K, WF::Processor<VI>> = HashMap::default();

            let mut receiver = self.data_stream.receiver;
            let selector = self.selector;
            let factory = self.factory;

            while let Some(event) = receiver.recv().await {
                let key = selector(&event);
                let processor = processors.entry(key.clone()).or_insert(factory.processor());

                let watermark_date_time = event.watermark_date_time.unwrap();
                processor.add_event(event).await;

                for (key, processor) in processors.iter_mut() {
                    for (_window, events) in processor
                        .trigger(watermark_date_time)
                        .await
                        .into_vec()
                        .drain(..)
                    {
                        let key_state = key_states
                            .entry(key.clone())
                            .or_insert_with_key(&key_state_fn);
                        let sender = Sender {
                            sender: sender.clone(),
                        };

                        process(
                            key.clone(),
                            events,
                            global_state.clone(),
                            key_state.clone(),
                            sender,
                        )
                        .await;
                    }
                }
            }
        });

        DataStream {
            nodes: Rc::clone(&self.data_stream.nodes),
            receiver,
        }
    }

    pub fn aggregate<VO, ACC>(self, accumulator: ACC) -> DataStream<VO>
    where
        VO: Add<Output = VO> + Send + 'static,
        ACC: Fn(VI) -> VO + Send + Sync + 'static,
        WF::Aggregator<VO>: Send,
    {
        let (sender, receiver) = channel::<Event<VO>>(1);

        self.data_stream.nodes.borrow_mut().spawn(async move {
            let mut aggregators: HashMap<K, WF::Aggregator<VO>> = HashMap::default();

            let mut receiver = self.data_stream.receiver;
            let selector = self.selector;
            let factory = self.factory;

            while let Some(event) = receiver.recv().await {
                let key = selector(&event);
                let aggregator = aggregators
                    .entry(key.clone())
                    .or_insert(factory.aggregator());

                let watermark_date_time = event.watermark_date_time.unwrap();
                aggregator.add_event::<VI, ACC>(&accumulator, event).await;

                for aggregator in aggregators.values_mut() {
                    let window_events = aggregator.trigger(watermark_date_time).await;
                    for event in window_events
                        .into_vec()
                        .into_iter()
                        .flat_map(|(_window, events)| events.into_vec())
                    {
                        if sender.send(event).await.is_err() {
                            // Pipe closed so return.
                            return;
                        }
                    }
                }
            }
        });

        DataStream {
            nodes: Rc::clone(&self.data_stream.nodes),
            receiver,
        }
    }
}

impl<VI, KS, K, WF> WindowedDataStream<VI, KS, WF>
where
    VI: Send + Sync + Clone + 'static,
    KS: Fn(&Event<VI>) -> K + Clone,
    WF: Clone,
{
    pub fn split(self) -> (Self, Self) {
        let (left_data_stream, right_data_stream) = self.data_stream.split();

        (
            Self {
                data_stream: left_data_stream,
                selector: self.selector.clone(),
                factory: self.factory.clone(),
            },
            Self {
                data_stream: right_data_stream,
                selector: self.selector.clone(),
                factory: self.factory.clone(),
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
    use std::ops::Range;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    use tokio::sync::Mutex;

    use super::*;

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

    struct SliceValueSource<V, const N: usize>([V; N]);

    impl<V, const N: usize> Source<V> for SliceValueSource<V, N>
    where
        V: Send + 'static,
    {
        fn run<'b>(self, mut sender: Sender<V>) -> BoxFuture<'b, ()> {
            let values = self.0;
            Box::pin(async move {
                for event in values.into_iter().map(Event::new) {
                    sender.send(event).await;
                }
            })
        }
    }

    struct SliceValueAssertSink<V, const N: usize>([V; N]);

    impl<V, const N: usize> Sink<V> for SliceValueAssertSink<V, N>
    where
        V: Send + Debug + PartialEq + 'static,
    {
        fn run<'b>(self, mut receiver: Receiver<V>) -> BoxFuture<'b, ()> {
            let values = self.0;
            Box::pin(async move {
                let mut iter = values.into_iter();
                while let Some(event) = receiver.receive().await {
                    assert_eq!(iter.next().unwrap(), event.value);
                }
            })
        }
    }

    struct SliceEventSource<V, const N: usize>([Event<V>; N]);

    impl<V, const N: usize> Source<V> for SliceEventSource<V, N>
    where
        V: Send + 'static,
    {
        fn run<'b>(self, mut sender: Sender<V>) -> BoxFuture<'b, ()> {
            let events = self.0;
            Box::pin(async move {
                for event in events.into_iter() {
                    sender.send(event).await;
                }
            })
        }
    }

    struct SliceEventAssertSink<V, const N: usize>([Event<V>; N]);

    impl<V, const N: usize> Sink<V> for SliceEventAssertSink<V, N>
    where
        Event<V>: PartialEq,
        V: Send + Debug + 'static,
    {
        fn run<'b>(self, mut receiver: Receiver<V>) -> BoxFuture<'b, ()> {
            let events = self.0;
            Box::pin(async move {
                let mut iter = events.into_iter();
                while let Some(event) = receiver.receive().await {
                    let expected = iter.next().unwrap();
                    assert_eq!(expected, event);
                }

                let next = iter.next();
                assert!(next.is_none(), "expected none event: {:?}", next.unwrap());
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
        V: Debug + Send + PartialEq + 'static,
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

    #[derive(Clone)]
    struct CountState {
        count: usize,
    }

    #[tokio::test]
    async fn keyed_process_state() {
        let env = Environment::default();

        let source = SliceValueSource(["a", "b", "a", "b"]);
        let sink = SliceValueAssertSink([(0, 0), (1, 0), (2, 1), (3, 1)]);

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
    fn event_time_session_window_memory_store() {
        let mut store = EventTimeWindowMemoryStore::with_timeout(Duration::minutes(10));

        let window_events = store.trigger(naive_date_time(09, 40));
        assert_eq!(window_events.len(), 0);

        let event = new_event(0, 09, 50);
        let window = store.add_event(event);
        assert_eq!(
            window,
            Window {
                start_date_time: naive_date_time(09, 50),
                end_date_time: naive_date_time(09, 50) + Duration::nanoseconds(1),
            }
        );
        let window_events = store.trigger(naive_date_time(09, 50));
        assert_eq!(window_events.len(), 0);

        let event = new_event(0, 10, 00);
        let window = store.add_event(event);
        assert_eq!(
            window,
            Window {
                start_date_time: naive_date_time(09, 50),
                end_date_time: naive_date_time(10, 00) + Duration::nanoseconds(1),
            }
        );
        let window_events = store.trigger(naive_date_time(10, 00));
        assert_eq!(window_events.len(), 0);

        let event = new_event(0, 10, 05);
        let window = store.add_event(event);
        assert_eq!(
            window,
            Window {
                start_date_time: naive_date_time(09, 50),
                end_date_time: naive_date_time(10, 05) + Duration::nanoseconds(1),
            }
        );
        let window_events = store.trigger(naive_date_time(10, 05));
        assert_eq!(window_events.len(), 0);

        let event = new_event(0, 10, 25);
        let window = store.add_event(event);
        assert_eq!(
            window,
            Window {
                start_date_time: naive_date_time(10, 25),
                end_date_time: naive_date_time(10, 25) + Duration::nanoseconds(1),
            }
        );
        let window_events = store.trigger(naive_date_time(10, 25));
        assert_eq!(window_events.len(), 1);
        let window = &window_events[0].0;
        assert_eq!(
            window,
            &Window {
                start_date_time: naive_date_time(09, 50),
                end_date_time: naive_date_time(10, 05) + Duration::nanoseconds(1),
            }
        );
        let events = &window_events[0].1;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], new_event(0, 09, 50));
        assert_eq!(events[1], new_event(0, 10, 00));
        assert_eq!(events[2], new_event(0, 10, 05));
    }

    #[tokio::test]
    async fn event_time_session_window_aggregator_trigger() {
        let mut agg = EventTimeSessionWindowAggregator {
            store: EventTimeWindowMemoryStore::with_timeout(Duration::minutes(10)),
        };

        let acc = |_event| 2;

        let event = new_event("A", 10, 00);
        agg.add_event(&acc, event).await;
        let window_events = agg.trigger(naive_date_time(10, 00)).await;
        assert_eq!(window_events.len(), 0);

        let event = new_event("B", 10, 05);
        agg.add_event(&acc, event).await;
        let window_events = agg.trigger(naive_date_time(10, 05)).await;
        assert_eq!(window_events.len(), 0);

        let window_events = agg.trigger(naive_date_time(10, 20)).await;
        assert_eq!(window_events.len(), 1);
        assert_eq!(
            window_events[0].0,
            Window {
                start_date_time: naive_date_time(10, 00),
                end_date_time: naive_date_time(10, 05) + Duration::nanoseconds(1),
            }
        );
        dbg!(&window_events);
        assert_eq!(window_events[0].1.len(), 1);
        assert_eq!(window_events[0].1[0], new_event(4, 10, 05));
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

        let source = SliceEventSource([new_event(0_usize, 12, 10), new_event(1_usize, 12, 30)]);

        let sink = SliceEventAssertSink([new_event(vec![0_usize], 12, 10)]);

        env.add_source(source)
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
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

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(0_usize, 12, 12),
            new_event(1_usize, 12, 30),
        ]);

        let sink = SliceEventAssertSink([new_event(vec![0_usize, 0_usize], 12, 10)]);

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
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

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(0_usize, 12, 30),
            new_event(0_usize, 12, 40),
            new_event(1_usize, 12, 55),
            new_event(1_usize, 12, 56),
            new_event(2_usize, 13, 20),
        ]);

        let sink = SliceEventAssertSink([
            new_event((0, 0, 0), 12, 10),
            new_event((1, 1, 0), 12, 30),
            new_event((2, 0, 1), 12, 55),
        ]);

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
                Duration::minutes(10),
            ))
            .process_state(
                |key,
                 events,
                 global_state: Arc<Mutex<CountState>>,
                 key_state: Arc<Mutex<CountState>>,
                 mut sender| async move {
                    let mut global_state = global_state.lock().await;
                    let mut key_state = key_state.lock().await;
                    let grouped_event = Event {
                        processing_date_time: events[0].processing_date_time,
                        event_date_time: events[0].event_date_time,
                        watermark_date_time: events[0].watermark_date_time,
                        value: (global_state.count, key_state.count, key),
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

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(0_usize, 12, 12),
            new_event(0_usize, 12, 13),
            new_event(1_usize, 12, 41),
            new_event(1_usize, 12, 42),
            new_event(2_usize, 12, 53),
        ]);

        let sink = SliceEventAssertSink([new_event((0, 0), 12, 10), new_event((1, 0), 12, 41)]);

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
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
    async fn windowed_aggregate_separate_events() {
        let env = Environment::default();

        let source = SliceEventSource([new_event(0_usize, 12, 10), new_event(1_usize, 12, 30)]);

        let sink = SliceEventAssertSink([new_event(0_usize, 12, 10)]);

        env.add_source(source)
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
                Duration::minutes(10),
            ))
            .aggregate(|value| value)
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn windowed_aggregate_joined_events() {
        let env = Environment::default();

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(0_usize, 12, 12),
            new_event(1_usize, 12, 30),
        ]);

        let sink = SliceEventAssertSink([new_event(4_usize, 12, 12)]);

        let stream = env.add_source(source);

        stream
            .key_by(|event| event.value)
            .window(EventTimeSessionWindowFactory::with_timeout(
                Duration::minutes(10),
            ))
            .aggregate(|_value| 2_usize)
            .add_sink(sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn split_data_stream() {
        let env = Environment::default();

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(1_usize, 12, 12),
            new_event(2_usize, 12, 30),
            new_event(3_usize, 12, 32),
        ]);
        let stream = env.add_source(source);

        let (left_stream, right_stream) = stream.split();

        let left_sink =
            SliceEventAssertSink([new_event(0_usize, 12, 10), new_event(2_usize, 12, 30)]);
        async fn left_filter(event: &Event<usize>) -> bool {
            event.value == 0 || event.value == 2
        }
        left_stream.filter(left_filter).add_sink(left_sink);

        let right_sink =
            SliceEventAssertSink([new_event(1_usize, 12, 12), new_event(3_usize, 12, 32)]);
        async fn right_filter(event: &Event<usize>) -> bool {
            event.value == 1 || event.value == 3
        }
        right_stream.filter(right_filter).add_sink(right_sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn split_keyed_data_stream() {
        let env = Environment::default();

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(1_usize, 12, 12),
            new_event(2_usize, 12, 30),
            new_event(3_usize, 12, 32),
        ]);
        let stream = env.add_source(source).key_by(|event| -> String {
            if event.value < 2 {
                "a".into()
            } else {
                "b".into()
            }
        });

        let (left_stream, right_stream) = stream.split();

        let left_sink = SliceEventAssertSink([
            new_event("a".into(), 12, 10),
            new_event("a".into(), 12, 12),
            new_event("b".into(), 12, 30),
            new_event("b".into(), 12, 32),
        ]);

        async fn left_map(key: String, event: Event<usize>) -> Event<String> {
            event.with_value(key)
        }
        left_stream.map(left_map).add_sink(left_sink);

        let right_sink = SliceEventAssertSink([
            new_event("az".into(), 12, 10),
            new_event("az".into(), 12, 12),
            new_event("bz".into(), 12, 30),
            new_event("bz".into(), 12, 32),
        ]);

        async fn right_map(key: String, event: Event<usize>) -> Event<String> {
            let value = format!("{}z", key);
            event.with_value(value)
        }
        right_stream.map(right_map).add_sink(right_sink);

        env.execute().await;
    }

    #[tokio::test]
    async fn split_windowed_data_stream() {
        let env = Environment::default();

        let source = SliceEventSource([
            new_event(0_usize, 12, 10),
            new_event(1_usize, 12, 12),
            new_event(2_usize, 12, 30),
            new_event(3_usize, 12, 32),
        ]);
        let stream = env
            .add_source(source)
            .key_by(|event| -> String {
                if event.value < 2 {
                    "a".into()
                } else {
                    "b".into()
                }
            })
            .window(EventTimeSessionWindowFactory::with_timeout(
                Duration::minutes(10),
            ));

        let (left_stream, right_stream) = stream.split();

        let left_sink = SliceEventAssertSink([new_event(2, 12, 12)]);

        left_stream.aggregate(|_value| 1).add_sink(left_sink);

        let right_sink = SliceEventAssertSink([new_event(2, 12, 12)]);

        right_stream.aggregate(|_value| 1).add_sink(right_sink);

        env.execute().await;
    }
}
