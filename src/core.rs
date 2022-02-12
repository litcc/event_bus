use crate::async_utils::{AsyncFn, AsyncFnOnce};
use crate::message::{Body, IMessage, IMessageData};
use crate::utils::get_uuid_as_string;
use futures::future::{join_all, BoxFuture};
use futures::task::ArcWake;
use futures::TryFutureExt;
use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

type MsgArg<T> = Arc<IMessage<T>>;
type BsIn<'a, T> = Arc<EventBusInner<'a, T>>;

pub struct FnMessage<'a, T>
    where
        T: IMessageData + Send + Sync + Clone,
{
    pub eb: BsIn<'a, T>,
    pub msg: MsgArg<T>,
}

type BoxFnMessage<'a, T> =
Box<dyn AsyncFn<'a, FnMessage<'a,T>, ()> + Send + Sync>;

type BoxFnMessageImmutable<'a, T> =
Box<dyn AsyncFnOnce<'a, FnMessage<'a,T>, ()> + Send + Sync>;

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
    'a,
    T: IMessageData + Send + Sync + Clone,
> {
    id: String,
    consumers: BoxFnMessage<'a, T>,
}

impl<'a, T> PartialEq
for Consumers<'a, T>
    where
        T: IMessageData + Send + Sync + Clone
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<'a, T> Eq
for Consumers<'a, T>
    where
        T: IMessageData + Send + Sync + Clone
{}

impl<'a, T> Hash
for Consumers<'a, T>
    where
        T: IMessageData + Send + Sync + Clone
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// 事件总线主要结构体
pub struct EventBus<
    'a,
    T: IMessageData + Send + Sync + Clone,
> {
    options: EventBusOptions,
    // 消费者
    sender: Sender<IMessage<T>>,
    // cluster_manager: Arc<Option<SyncLife>>,
    // event_bus_port: u16,
    inner: Arc<EventBusInner<'a, T>>,
    // self_arc: Weak<EventBus<SyncLife>>,
}

pub struct EventBusInner<
    'a,
    T: IMessageData + Send + Sync + Clone,
> {
    sender: Sender<IMessage<T>>,
    consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<'a, T>>>>>>,
    // 所有消费者
    all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<'a, T>>>>>,
    callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<'a, T>>>>,
    // 消费者
    receiver: Arc<Mutex<Receiver<IMessage<T>>>>,
    // 消息处理线程
    receiver_joiner: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub runtime: Arc<Runtime>,
}

//static EVENT_BUS_INSTANCE: OnceCell<EventBus<()>> = OnceCell::new();

// 初始化事件总线以及启动事件总线
impl<'a, T> EventBus<'a, T>
    where
        T: IMessageData + Send + Sync + Clone
{
    // 生成新的事件总线
    pub fn new<'b>(options: EventBusOptions) -> EventBus<'b, T> {
        // 根据配置创建队列
        let (sender, receiver): (Sender<IMessage<T>>, Receiver<IMessage<T>>) =
            channel(options.event_bus_queue_size);
        // 创建事件总线
        let pool_size = options.event_bus_pool_size;
        EventBus {
            options,
            sender: sender.clone(),
            // self_arc: me.clone(),
            inner: Arc::new(EventBusInner {
                sender,
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

    // 启动事件总线
    pub async fn start<'h>(&self)
        where Self: 'h
    {
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
                                            <EventBus<'h, T>>::call_func(
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
                                    <EventBus<'h,T>>::call_replay(
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


    ///
    /// 设置消费者
    #[inline]
    pub fn consumer<OP, OT>(&self, address: &str, op: OP) -> String
        where
            Self: 'a,
            OT: Future<Output=()> + 'static + Sync + Send,
            OP: Fn(FnMessage<'a, T>) -> OT + 'static + Sync + Send,
    {
        self.inner.consumer(address, Box::new(op))
    }

    /// 向消费者发送消息
    #[inline]
    pub fn send(&self, address: &str, request: Body)
        where
            Self: 'a
    {
        self.inner.send(address, request)
    }

    /// 向消费者发送数据并且等待相应数据
    /*#[inline]
    pub async fn request<OP, OT>(&self, address: &str, ref request: Body) -> Option<Arc<Body>>
    {
        let request_clone = request.clone();
        let address_clone = address.to_owned();
        let inner = self.inner.clone();
        let kk = crate::async_utils::suspend_coroutine( move |result| async move {
            let aa = result.clone();
            inner.request(address_clone.as_str(), request_clone,  move |eb| async move   {
                let qwe1 = eb.msg.body().await.clone();

                aa.resume(Some(qwe1));
            });
        }).await;
        kk
    }*/
    #[inline]
    pub fn publish(&self, address: &str, request: Body)
        where
            Self: 'a
    {
        self.inner.publish(address, request)
    }

    #[inline]
    async fn call_func<'k>(
        eb: Arc<EventBusInner<'k, T>>,
        consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<'k, T>>>>>>,
        _all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<'k, T>>>>>,
        _callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<'k, T>>>>,
        eb_sender: Sender<IMessage<T>>,
        msg_data: Arc<IMessage<T>>,
        address: &str,
    ) {
        let consumers_tmp = consumers.clone();
        let hashmap = consumers_tmp.lock().await;
        let msg_arc2 = msg_data.clone();
        if msg_data.is_publish().await {
            let consumers_list = hashmap.get(address).unwrap().iter();
            let mut kk_tmp = vec![];
            for fun_item in consumers_list {
                let fun_item_tmp = Arc::clone(fun_item);
                let message_clone = Arc::clone(&msg_data);
                let eb_tmp = Arc::clone(&eb);
                let kk2 = fun_item_tmp.consumers.async_call(FnMessage {
                    eb: eb_tmp,
                    msg: message_clone,
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
            fun_call
                .async_call(FnMessage {
                    eb: eb.clone(),
                    msg: msg_data,
                })
                .await;
            if msg_arc2.is_reply().await {
                let kk: IMessage<T> = (&*msg_arc2).clone();
                eb_sender.send(kk).await.unwrap();
            }
        }
    }

    #[inline]
    async fn call_replay<'k>(
        eb: Arc<EventBusInner<'k, T>>,
        msg_data: Arc<IMessage<T>>,
        callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<'k, T>>>>,
    ) {
        let msg = msg_data.clone();
        let address = msg.replay_address().await.clone();
        if let Some(address) = address {
            let mut map = callback_functions.lock().await;
            let callback = map.remove(&address);
            if let Some(caller) = callback {
                caller.async_once_call(FnMessage { eb, msg }).await
            }
        }
    }
}


impl<'a, T> EventBusInner<'a, T>
    where
        T: IMessageData + Send + Sync + Clone
{
    // 设置消费者
    #[inline]
    fn consumer<OP, OT>(&self, address: &str, op: BoxFnMessage<'a, T>) -> String
        where
            Self: 'a,
            OT: Future<Output=()> + 'static + Sync + Send,
            OP: Fn(FnMessage<'a,T>) -> OT + 'static + Sync + Send,
    {
        self.runtime.block_on(async move {
            let mut uuid = get_uuid_as_string();
            let cons = self.all_consumers.clone();
            'uuid: loop {
                let mut cons_lock = cons.lock().await;
                if !cons_lock.contains_key(&uuid) {
                    cons_lock.insert(
                        uuid.clone(),
                        Arc::new(Consumers {
                            id: uuid.clone(),
                            consumers: op,
                        }),
                    );
                    break 'uuid;
                }
                uuid = get_uuid_as_string();
            }
            let consumers = self.consumers.clone();
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
        })
    }


    #[inline]
    fn send(&self, address: &str, request: Body) {
        self.runtime.block_on(async move {
            let msg: IMessage<T> = IMessage::new(T::build_send_data(address, request));
            let local_sender = self.sender.clone();
            local_sender.send(msg).await.unwrap();
            debug!("发送成功");
        });
    }


    #[inline]
    fn request<OP, OT>(&self, address: &str, request: Body, op: OP)
        where
            Self: 'a,
            OT: Future<Output=()> + 'static + Sync + Send,
            OP: FnOnce(FnMessage<'a, T>) -> OT + 'static + Sync + Send + Copy,
    {
        self.runtime.block_on(async move {
            let _addr = address.to_owned();
            let replay_address = format!("__EventBus.reply.{}", uuid::Uuid::new_v4().to_string());
            let msg: IMessage<T> = IMessage::new(T::build_request_data(
                address,
                replay_address.as_str(),
                request,
            ));
            let local_cons = self.callback_functions.clone();
            local_cons.lock().await.insert(replay_address, Box::new(op));
            let local_sender = self.sender.clone();
            local_sender.send(msg).await.unwrap();
        });
    }


    ///
    /// 向所有指定消费者发送消息
    #[inline]
    fn publish(&self, address: &str, request: Body)
        where
            Self: 'a,
    {
        self.runtime.block_on(async {
            let msg: IMessage<T> = IMessage::new(T::build_publish_data(address, request));
            let local_sender = self.sender.clone();
            local_sender.send(msg).await.unwrap();
        });
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
