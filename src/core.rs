use crate::async_utils::{AsyncFn, AsyncFnOnce};
use crate::message::{Body, IMessage, IMessageData, IMessageReplayFuture};
use crate::utils::get_uuid_as_string;
use futures::future::join_all;
use log::{debug, error, trace};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;

use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;

pub struct FnMessage<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    pub eb: Arc<EventBusInner<T>>,
    pub msg: IMessage,
}

type BoxFnMessage<T> = Box<dyn AsyncFn<'static, FnMessage<T>, ()> + 'static + Send + Sync>;

type BoxFnMessageImmutable<T> =
    Box<dyn AsyncFnOnce<'static, FnMessage<T>, ()> + 'static + Send + Sync>;

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

pub struct Consumers<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    id: String,
    consumers: BoxFnMessage<T>,
}

impl<T> PartialEq for Consumers<T>
where
    T: IMessageData + Send + Sync + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for Consumers<T> where T: IMessageData + Send + Sync + Clone {}

impl<T> Hash for Consumers<T>
where
    T: IMessageData + Send + Sync + Clone,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// 事件总线主要结构体
pub struct EventBus<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    options: EventBusOptions,
    // 消费者
    // sender: Sender<IMessage>,
    // cluster_manager: Arc<Option<SyncLife>>,
    // event_bus_port: u16,
    inner: Arc<EventBusInner<T>>,
    // self_arc: Weak<EventBus<SyncLife>>,
}

pub struct EventBusInner<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    sender: Sender<IMessage>,
    consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<T>>>>>>,
    // 所有消费者
    all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<T>>>>>,
    callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<T>>>>,
    // 消费者
    receiver: Arc<Mutex<Receiver<IMessage>>>,
    // 消息处理线程
    receiver_joiner: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub runtime: Arc<Runtime>,
}

impl<T> EventBus<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    // 生成新的事件总线
    pub fn new(options: EventBusOptions) -> EventBus<T> {
        // 根据配置创建队列
        let (sender, receiver): (Sender<IMessage>, Receiver<IMessage>) =
            channel(options.event_bus_queue_size);
        // 创建事件总线
        let pool_size = options.event_bus_pool_size;
        EventBus {
            options,
            // sender: sender.clone(),
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

    pub async fn start(&self) {
        let inner = self.inner.clone();
        let receiver = self.inner.receiver.clone();
        let runtime = self.inner.runtime.clone();
        let receiver_joiner = self.inner.receiver_joiner.clone();
        let is_running = receiver_joiner.lock().await.is_none();

        if is_running {
            let receiver_joiner_handle1 = receiver_joiner.clone();
            let inner_handle1 = inner.clone();

            let loop_closure = async move {
                let inner_handle2 = inner_handle1.clone();
                'message_loop: loop {
                    trace!("[msg loop] start");
                    let msg = receiver.lock().await.recv().await;
                    trace!("[msg loop] get msg");
                    match msg {
                        Some(msg_data) => {
                            let msg_clone = msg_data.clone();
                            let msg_arc_handle = msg_clone.clone();
                            let inner_handle3 = inner_handle2.clone();
                            inner_handle2.runtime.spawn(async move {
                                let eb_inner = inner_handle3;
                                let msg_data_arc = msg_arc_handle;
                                let consumers_handle2 = eb_inner.consumers.clone();
                                let all_consumers_handle2 = eb_inner.all_consumers.clone();
                                let callback_functions_handle2 =
                                    eb_inner.callback_functions.clone();
                                let eb_sender_handle2 = eb_inner.sender.clone();

                                debug!(
                                    "get message from event bus: {:?}",
                                    msg_data_arc.body().await.as_ref()
                                );

                                if msg_data_arc.is_reply().await {
                                    <EventBus<T>>::call_replay(
                                        eb_inner,
                                        msg_data_arc.clone(),
                                        callback_functions_handle2.clone(),
                                    )
                                    .await;
                                } else {
                                    let address = msg_data_arc.send_address().await;
                                    if address.is_some() {
                                        if consumers_handle2
                                            .lock()
                                            .await
                                            .contains_key(address.as_ref().unwrap())
                                        {
                                            <EventBus<T>>::call_func(
                                                eb_inner,
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
                                }
                            });
                        }
                        None => {
                            error!("error: receive message from event bus failed");
                            break 'message_loop;
                        }
                    }
                }
                *receiver_joiner_handle1.lock().await = None;
            };

            let joiner = runtime.spawn(loop_closure);
            *receiver_joiner.lock().await = Some(joiner);
        }
    }

    async fn call_func(
        eb: Arc<EventBusInner<T>>,
        consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<T>>>>>>,
        _all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<T>>>>>,
        _callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<T>>>>,
        eb_sender: Sender<IMessage>,
        msg_data: IMessage,
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
                let message_clone = msg_data.clone();
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
            let call_arg = FnMessage {
                eb: eb.clone(),
                msg: msg_arc2.clone(),
            };
            if msg_arc2.can_reply().await {
                let imessage = msg_arc2.clone();
                fun_call.async_call(call_arg).await;
                imessage.clone().replay_future.unwrap().await_future().await;
                if imessage.is_reply().await {
                    let kk: IMessage = imessage.clone();
                    eb_sender.send(kk).await.unwrap();
                }
            } else {
                fun_call.async_call(call_arg).await;
            }
        }
    }

    async fn call_replay(
        eb: Arc<EventBusInner<T>>,
        msg_data: IMessage,
        callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<T>>>>,
    ) {
        let msg = msg_data.clone();
        let address = msg.replay_address().await.clone();
        if let Some(address) = address {
            let mut map = callback_functions.lock().await;
            let callback = map.remove(&address);

            if let Some(caller) = callback {
                let data = FnMessage { eb, msg };
                caller.async_call_once(data).await
            }
        }
    }

    #[inline]
    pub async fn consumer<OP, OT>(&self, address: &str, op: OP) -> String
    where
        OT: Future<Output = ()> + 'static + Sync + Send,
        OP: Fn(FnMessage<T>) -> OT + 'static + Sync + Send,
    {
        let kdf = Box::new(op);
        self.inner.consumer(address, kdf).await
    }

    /// 向消费者发送消息
    #[inline]
    pub fn send(&self, address: &str, request: Body) {
        self.inner.send(address, request)
    }

    /// 向消费者发送数据并且等待相应数据
    #[inline]
    pub async fn request(&self, address: &str, ref request: Body) -> Option<Body> {
        let request_clone = request.clone();
        let address_clone = address.to_owned();
        let inner = self.inner.clone();
        let kk = crate::async_utils::suspend_coroutine(move |result| async move {
            let aa = result.clone();
            inner
                .request(
                    address_clone.as_str(),
                    request_clone,
                    move |eb| async move {
                        debug!("request 收到回复");
                        let body = eb.msg.body().await.clone();
                        let body_clone = (&*body).clone();
                        aa.resume(Some(body_clone));
                    },
                )
                .await;
        })
        .await;
        kk
    }
    #[inline]
    pub fn publish(&self, address: &str, request: Body) {
        self.inner.publish(address, request)
    }
}

impl<T> EventBusInner<T>
where
    T: 'static + IMessageData + Send + Sync + Clone,
{
    // 设置消费者
    #[inline]
    async fn consumer(&self, address: &str, op: BoxFnMessage<T>) -> String {
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
    }

    #[inline]
    fn send(&self, address: &str, request: Body) {
        let addr = address.to_string();
        let sender = self.sender.clone();
        self.runtime.spawn(async move {
            let msg: IMessage = IMessage::new(T::build_send_data(&addr, request));
            sender.send(msg).await.unwrap();
            // debug!("发送成功");
        });
    }

    #[inline]
    async fn request<OP, OT>(&self, address: &str, request: Body, op: OP)
    where
        OT: Future<Output = ()> + 'static + Sync + Send,
        OP: FnOnce(FnMessage<T>) -> OT + 'static + Sync + Send,
    {
        let _addr = address.to_owned();
        let replay_address = format!("__EventBus.reply.{}", uuid::Uuid::new_v4().to_string());
        let msg: IMessage = IMessage::new_replay(
            T::build_request_data(address, replay_address.as_str(), request),
            IMessageReplayFuture::new(),
        );
        let local_cons = self.callback_functions.clone();
        // let request = Box::new();
        local_cons.lock().await.insert(replay_address, Box::new(op));
        let local_sender = self.sender.clone();
        // local_sender.send(msg).await.unwrap();
        local_sender.send(msg).await.unwrap();
    }

    ///
    /// 向所有指定消费者发送消息
    #[inline]
    fn publish(&self, address: &str, request: Body) {
        let addr = address.to_string();
        let sender = self.sender.clone();

        self.runtime.spawn(async move {
            let msg: IMessage = IMessage::new(T::build_publish_data(&addr, request));
            sender.send(msg).await.unwrap();
        });
    }
}

#[cfg(test)]
mod test_event_bus {
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn new_bus() {
        println!("测试启动");
    }
}
