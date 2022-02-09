use crate::message::{Body, IMessage, IMessageData};
use crate::utils::get_uuid_as_string;
use futures::future::{join_all, BoxFuture};
use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::future::{Future};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};
use futures::task::ArcWake;
use futures::TryFutureExt;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;



pub trait AsyncFn<IT, OT> {
    fn call(&self, args: IT) -> BoxFuture<'static, OT>;
}

impl<T, F, IT, OT> AsyncFn<IT, OT> for T
    where
        F: Future<Output=OT> + 'static + Send + Sync,
        T: Fn(IT) -> F,
{
    fn call(&self, args: IT) -> BoxFuture<'static, OT> {
        Box::pin(self(args))
    }
}


type MsgArg<T> = Arc<IMessage<T>>;
type BsIn<SyncLife, T> = Arc<EventBusInner<SyncLife, T>>;

pub struct FnMessage<SyncLife, T>
    where
        SyncLife: 'static + Send + Sync,
        T: IMessageData + 'static + Send + Sync + Clone
{
    pub eb: BsIn<SyncLife, T>,
    pub msg: MsgArg<T>,
}

type BoxFnMessage<SyncLife, T> = Box<dyn AsyncFn<FnMessage<SyncLife, T>, ()> + 'static + Send + Sync>;

type BoxFnMessageImmutable<SyncLife, T> = Box<dyn AsyncFn<FnMessage<SyncLife, T>, ()> + 'static + Send + Sync>;


// 事件总线配置
#[derive(Debug, Clone)]
pub struct EventBusOptions {
    // 事件总线占用cpu数
    event_bus_pool_size: usize,
    // 事件总线队列大小
    event_bus_queue_size: usize,
}

impl Default for EventBusOptions {
    fn default() -> Self {
        let cpus = num_cpus::get() / 2;
        let cpus = if cpus < 1 { 1 } else { cpus };
        //let vertx_port: u16 = 0;
        EventBusOptions {
            event_bus_pool_size: cpus,
            event_bus_queue_size: 2000,
            //vertx_host: String::from("127.0.0.1"),
            //vertx_port,
        }
    }
}

pub struct Consumers<
    SyncLife: 'static + Send + Sync,
    T: IMessageData + 'static + Send + Sync + Clone,
> {
    id: String,
    consumers: BoxFnMessage<SyncLife, T>,
}

impl<SyncLife: 'static + Send + Sync, T: IMessageData + 'static + Send + Sync + Clone> PartialEq
for Consumers<SyncLife, T>
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<SyncLife: 'static + Send + Sync, T: IMessageData + 'static + Send + Sync + Clone> Eq
for Consumers<SyncLife, T>
{}

impl<SyncLife: 'static + Send + Sync, T: IMessageData + 'static + Send + Sync + Clone> Hash
for Consumers<SyncLife, T>
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// 事件总线主要结构体
pub struct EventBus<
    SyncLife: 'static + Send + Sync,
    T: IMessageData + 'static + Send + Sync + Clone,
> {
    options: EventBusOptions,
    // 消费者
    sender: Sender<IMessage<T>>,
    // cluster_manager: Arc<Option<SyncLife>>,
    // event_bus_port: u16,
    inner: Arc<EventBusInner<SyncLife, T>>,
    // self_arc: Weak<EventBus<SyncLife>>,
}

pub struct EventBusInner<
    SyncLife: 'static + Send + Sync,
    T: IMessageData + 'static + Send + Sync + Clone,
> {
    consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<SyncLife, T>>>>>>,
    // 所有消费者
    all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<SyncLife, T>>>>>,
    callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife, T>>>>,
    // 消费者
    receiver: Arc<Mutex<Receiver<IMessage<T>>>>,
    // 消息处理线程
    receiver_joiner: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub runtime: Arc<Runtime>,
}

//static EVENT_BUS_INSTANCE: OnceCell<EventBus<()>> = OnceCell::new();

// 初始化事件总线以及启动事件总线
impl<SyncLife: 'static + Send + Sync, T: IMessageData + 'static + Send + Sync + Clone>
EventBus<SyncLife, T>
{
    // 生成新的事件总线
    pub fn new(options: EventBusOptions) -> EventBus<SyncLife, T> {
        // 根据配置创建队列
        let (sender, receiver): (Sender<IMessage<T>>, Receiver<IMessage<T>>) =
            channel(options.event_bus_queue_size);
        // 创建事件总线
        let pool_size = options.event_bus_pool_size;
        EventBus {
            options,
            sender,
            // self_arc: me.clone(),
            inner: Arc::new(EventBusInner {
                consumers: Arc::new(Mutex::new(HashMap::new())),
                all_consumers: Arc::new(Mutex::new(Default::default())),
                callback_functions: Arc::new(Mutex::new(HashMap::new())),
                receiver: Arc::new(Mutex::new(receiver)),
                receiver_joiner: Arc::new(Mutex::new(None)),
                runtime: Arc::new(
                    Builder::new_multi_thread()
                        .thread_name("event-bus-thread")
                        .worker_threads(pool_size)
                        .enable_all()
                        .build()
                        .unwrap(),
                ),
            }),
        }
    }

    // // 初始化事件总线
    // pub fn init(options: EventBusOptions) -> &'static EventBus<()> {
    //     EVENT_BUS_INSTANCE.get_or_init(|| {
    //         EventBus::<()>::new(options)
    //     })
    // }
    // // 获取事件总线单例
    // pub fn get_instance() -> &'static EventBus<()> {
    //     EVENT_BUS_INSTANCE.get().expect("logger is not initialized")
    // }

    // 启动事件总线
    pub async fn start(&self) {
        let inner = self.inner.clone();
        let receiver = self.inner.receiver.clone();
        let runtime = self.inner.runtime.clone();
        let receiver_joiner = self.inner.receiver_joiner.clone();
        let is_running = receiver_joiner.lock().await.is_none();
        // let eb = self.inner.upgrade().unwrap();
        let eb_sender = self.sender.clone();
        let consumers = self.inner.consumers.clone();
        let all_consumers = self.inner.all_consumers.clone();
        let callback_functions = self.inner.callback_functions.clone();

        if is_running {
            let receiver_joiner_handle1 = receiver_joiner.clone();
            // _handle1
            let inner_handle1 = inner.clone();
            let runtime_handle1 = runtime.clone();
            // let eb = self.inner.upgrade().unwrap();
            let eb_sender_handle1 = eb_sender.clone();
            let consumers_handle1 = consumers.clone();
            let all_consumers_handle1 = all_consumers.clone();
            let callback_functions_handle1 = callback_functions.clone();

            let joiner: tokio::task::JoinHandle<()> = runtime.spawn(async move {
                'message_loop: loop {
                    info!("loop");
                    let msg = receiver.lock().await.recv().await;
                    info!("get msg");
                    match msg {
                        Some(msg_data) => {
                            let inner_handle2 = inner_handle1.clone();
                            let _runtime_handle2 = runtime_handle1.clone();
                            // let eb = self.inner.upgrade().unwrap();
                            let eb_sender_handle2 = eb_sender_handle1.clone();
                            let consumers_handle2 = consumers_handle1.clone();
                            let all_consumers_handle2 = all_consumers_handle1.clone();
                            let callback_functions_handle2 = callback_functions_handle1.clone();
                            let msg_arc = Arc::new(msg_data);
                            let msg_arc_handle = msg_arc.clone();

                            runtime_handle1.spawn(async move {
                                let msg_data_arc = msg_arc_handle.clone();

                                debug!(
                                    "get message from event bus: {:?}",
                                    msg_data_arc.body().await.as_ref()
                                );

                                if !(msg_data_arc.is_reply().await) {
                                    let address = msg_data_arc.send_address().await;
                                    if address.is_some() {
                                        if consumers_handle2
                                            .lock()
                                            .await
                                            .contains_key(address.as_ref().unwrap())
                                        {
                                            <EventBus<SyncLife, T>>::call_func(
                                                inner_handle2,
                                                consumers_handle2,
                                                all_consumers_handle2,
                                                callback_functions_handle2,
                                                eb_sender_handle2,
                                                msg_data_arc,
                                                address.as_ref().unwrap(),
                                            )
                                                .await;
                                        }
                                    }
                                } else {
                                    <EventBus<SyncLife, T>>::call_replay(
                                        inner_handle2,
                                        msg_data_arc.clone(),
                                        callback_functions_handle2.clone(),
                                    )
                                        .await;
                                }

                                // if address.is_some() {
                                //
                                // } else {
                                //
                                // }
                            });
                        }
                        None => {
                            error!("error: receive message from event bus failed");
                            break 'message_loop;
                        }
                    }
                }
                *receiver_joiner_handle1.lock().await = None;
            });
            *receiver_joiner.lock().await = Some(joiner);
        }
    }

    #[inline]
    pub async fn send(&self, address: &str, request: Body) {
        let _addr = address.to_owned();

        let msg: IMessage<T> = IMessage::new(T::build_send_data(address, request));
        // let message = VertxMessage {
        //     address: Some(addr),
        //     replay: None,
        //     body: Arc::new(request),
        //     ..Default::default()
        // };
        let local_sender = self.sender.clone();
        local_sender.send(msg).await.unwrap();
        info!("发送成功");
    }

    #[inline]
    pub async fn request<OP, OT>(&self, address: &str, request: Body, op: OP)
        where
            OT: Future<Output=()> + 'static + Sync + Send,
            OP: Fn(FnMessage<SyncLife, T>) -> OT + 'static + Sync + Send,
    {
        let _addr = address.to_owned();
        let replay_address = format!("__EventBus.reply.{}", uuid::Uuid::new_v4().to_string());
        let msg: IMessage<T> = IMessage::new(T::build_request_data(
            address,
            replay_address.as_str(),
            request,
        ));
        let local_cons = self.inner.callback_functions.clone();
        local_cons.lock().await.insert(replay_address, Box::new(op));
        let local_sender = self.sender.clone();
        local_sender.send(msg).await.unwrap();
    }

    #[inline]
    pub async fn publish(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let msg: IMessage<T> = IMessage::new(T::build_publish_data(
            address,
            request,
        ));
        let local_sender = self.sender.clone();
        local_sender.send(msg).await.unwrap();
    }


    // 设置消费者
    #[inline]
    pub async fn consumer<OP: 'static, OT>(&self, address: &str, op: OP) -> String
        where
            OT: Future<Output=()> + 'static + Sync + Send,
            OP: Fn(FnMessage<SyncLife, T>) -> OT + 'static + Sync + Send,
    {
        let mut uuid = get_uuid_as_string();
        let cons = self.inner.all_consumers.clone();
        'uuid: loop {
            let mut cons_lock = cons.lock().await;
            if !cons_lock.contains_key(&uuid) {
                cons_lock.insert(
                    uuid.clone(),
                    Arc::new(Consumers {
                        id: uuid.clone(),
                        consumers: Box::new(op),
                    }),
                );
                break 'uuid;
            }
            uuid = get_uuid_as_string();
        }
        let consumers = self.inner.consumers.clone();
        let kk = cons.clone().lock_owned().await.get(&uuid).unwrap().clone();
        if consumers.lock().await.contains_key(address) {
            consumers.lock().await.get_mut(address).unwrap().insert(kk);
        } else {
            consumers
                .lock()
                .await
                .insert(address.to_string(), HashSet::from([kk]));
        }
        uuid
    }

    // 判断是否存在消费者
    #[inline]
    pub async fn contains_consumer(&self, address: &str) -> bool {
        let consumers = self.inner.consumers.clone();
        return consumers.lock().await.contains_key(address);
    }

    #[inline]
    async fn call_replay(
        eb: Arc<EventBusInner<SyncLife, T>>,
        msg_data: Arc<IMessage<T>>,
        callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife, T>>>>,
    ) {
        let msg = msg_data.clone();
        let address = msg.replay_address().await.clone();
        if let Some(address) = address {
            let mut map = callback_functions.lock().await;
            let callback = map.remove(&address);
            if let Some(caller) = callback {
                // Pin::from().await;
                caller.call(FnMessage {
                    eb,
                    msg,
                }).await
            }
        }
    }

    #[inline]
    async fn call_func(
        eb: Arc<EventBusInner<SyncLife, T>>,
        consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<SyncLife, T>>>>>>,
        _all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<SyncLife, T>>>>>,
        _callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife, T>>>>,
        eb_sender: Sender<IMessage<T>>,
        msg_data: Arc<IMessage<T>>,
        address: &str,
    ) {
        let consumers_tmp = consumers.clone();
        let hashmap = consumers_tmp.lock().await;
        let msg_arc2 = msg_data.clone();
        if msg_data.is_publish().await {
            let consumers_list = hashmap.get(address).unwrap().iter();
            let mut kk_tmp: Vec<JoinHandle<()>> = vec![];
            for fun_item in consumers_list {
                let fun_item_tmp = Arc::clone(fun_item);
                let message_clone = Arc::clone(&msg_data);
                let eb_tmp = Arc::clone(&eb);
                let kk2 = eb.runtime.spawn(async move {
                    fun_item_tmp.consumers.call(FnMessage {
                        eb: eb_tmp,
                        msg: message_clone,
                    }).await;
                });
                kk_tmp.push(kk2);
            }
            join_all(kk_tmp).await;
        } else {
            let fun_call = &hashmap
                .get(address)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .consumers;
            fun_call.call(FnMessage {
                eb: eb.clone(),
                msg: msg_data,
            }).await;
            if msg_arc2.is_reply().await {
                let kk: IMessage<T> = (&*msg_arc2).clone();
                eb_sender.send(kk).await.unwrap();
            }
        }
    }
}

#[cfg(test)]
mod test_event_bus {
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn new_bus() {
        // <SyncLife: 'static + Send + Sync>
        println!("测试启动");
    }
}
