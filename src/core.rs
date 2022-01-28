use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use std::sync::{Arc};

use tokio::sync::{Mutex};
use crate::message::{Body, Message};
use log::{debug, error, info};
use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use crate::utils::{get_uuid_as_string};
use futures::future::join_all;

// lazy_static! {
//     pub static ref RUNTIME: Runtime = Builder::new_multi_thread().enable_all().build().unwrap();
//     static ref TCPS: Arc<HashMap<String, Arc<TcpStream>>> = Arc::new(HashMap::new());
//     static ref DO_INVOKE: AtomicBool = AtomicBool::new(true);
// }

type BoxFnMessage<CM> = Box<dyn Fn(&mut Message, Arc<EventBusInner<CM>>) + Send + Sync>;
type BoxFnMessageImmutable<CM> = Box<dyn Fn(&Message, Arc<EventBusInner<CM>>) + Send + Sync>;

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

pub struct Consumers<SyncLife: 'static + Send + Sync> {
    id: String,
    consumers: BoxFnMessage<SyncLife>,

}

impl<SyncLife: 'static + Send + Sync> PartialEq for Consumers<SyncLife> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<SyncLife: 'static + Send + Sync> Eq for Consumers<SyncLife> {}


impl<SyncLife: 'static + Send + Sync> Hash for Consumers<SyncLife> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}


// 事件总线主要结构体
pub struct EventBus<SyncLife: 'static + Send + Sync> {
    options: EventBusOptions,
    // 消费者
    sender: Sender<Message>,
    // cluster_manager: Arc<Option<SyncLife>>,
    // event_bus_port: u16,
    inner: Arc<EventBusInner<SyncLife>>,
    // self_arc: Weak<EventBus<SyncLife>>,
}

pub struct EventBusInner<SyncLife: 'static + Send + Sync> {
    consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<SyncLife>>>>>>,
    // 所有消费者
    all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<SyncLife>>>>>,
    callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife>>>>,
    // 消费者
    receiver: Arc<Mutex<Receiver<Message>>>,
    // 消息处理线程
    receiver_joiner: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub runtime: Arc<Runtime>,
}


static EVENT_BUS_INSTANCE: OnceCell<EventBus<()>> = OnceCell::new();

// 初始化事件总线以及启动事件总线
impl<SyncLife: 'static + Send + Sync> EventBus<SyncLife> {
    // 生成新的事件总线
    fn new(options: EventBusOptions) -> EventBus<SyncLife> {
        // 根据配置创建队列
        let (sender, receiver): (Sender<Message>, Receiver<Message>) =
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

    // 初始化事件总线
    pub fn init(options: EventBusOptions) -> &'static EventBus<()> {
        EVENT_BUS_INSTANCE.get_or_init(|| {
            EventBus::<()>::new(options)
        })
    }
    // 获取事件总线单例
    pub fn get_instance() -> &'static EventBus<()> {
        EVENT_BUS_INSTANCE.get().expect("logger is not initialized")
    }


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
                        Some(mut msg_data) => {
                            let inner_handle2 = inner_handle1.clone();
                            let runtime_handle2 = runtime_handle1.clone();
                            // let eb = self.inner.upgrade().unwrap();
                            let eb_sender_handle2 = eb_sender_handle1.clone();
                            let consumers_handle2 = consumers_handle1.clone();
                            let all_consumers_handle2 = all_consumers_handle1.clone();
                            let callback_functions_handle2 = callback_functions_handle1.clone();


                            runtime_handle1.spawn(async move  {
                                debug!("get message from event bus: {:?}", msg_data);
                                let address = msg_data.address.clone().unwrap();
                                if msg_data.address.is_some() {
                                    if consumers_handle2.lock().await.contains_key(&address) {
                                        <EventBus::<SyncLife>>::call_func(
                                            inner_handle2,
                                            consumers_handle2.clone(),
                                            all_consumers_handle2.clone(),
                                            callback_functions_handle2.clone(),
                                            eb_sender_handle2,
                                            &mut msg_data,
                                            &address,
                                        ).await;
                                    }
                                } else {
                                    <EventBus<SyncLife>>::call_replay(
                                        inner_handle2,
                                        &msg_data,
                                        callback_functions_handle2.clone(),
                                    ).await;
                                }
                            });
                        }
                        None => {
                            error!("error: receive message from event bus failed");
                            break 'message_loop;
                        }
                    }
                };
                *receiver_joiner_handle1.lock().await = None;
            });
            *receiver_joiner.lock().await = Some(joiner);
        }
    }


    #[inline]
    pub async fn send(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr),
            replay: None,
            body: Arc::new(request),
            ..Default::default()
        };
        let local_sender = self.sender.clone();
        local_sender.send(message).await.unwrap();
        info!("发送成功");
    }

    #[inline]
    pub async fn request<OP>(&self, address: &str, request: Body, op: OP)
        where
            OP: Fn(&Message, Arc<EventBusInner<SyncLife>>) + Send + 'static + Sync,
    {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr),
            replay: Some(format!(
                "__EventBus.reply.{}",
                uuid::Uuid::new_v4().to_string()
            )),
            body: Arc::new(request),
            ..Default::default()
        };
        let local_cons = self.inner.callback_functions.clone();
        local_cons.lock().await.insert(message.replay.clone().unwrap(), Box::new(op));
        let local_sender = self.sender.clone();
        local_sender.send(message).await.unwrap();
    }

    #[inline]
    pub async fn publish(&self, address: &str, request: Body) {
        let addr = address.to_owned();
        let message = Message {
            address: Some(addr),
            replay: None,
            body: Arc::new(request),
            publish: true,
            ..Default::default()
        };
        let local_sender = self.sender.clone();
        local_sender.send(message).await.unwrap();
    }


    // 设置消费者
    #[inline]
    pub async fn consumer<OP>(&self, address: &str, op: OP) -> String
        where
            OP: Fn(&mut Message, Arc<EventBusInner<SyncLife>>) + Send + 'static + Sync + Copy,
    {
        let mut uuid = get_uuid_as_string();
        let cons = self.inner.all_consumers.clone();
        'uuid: loop {
            let mut cons_lock = cons.lock().await;
            if !cons_lock.contains_key(&uuid) {
                cons_lock.insert(uuid.clone(), Arc::new(Consumers {
                    id: uuid.clone(),
                    consumers: Box::new(op),
                }));
                break 'uuid;
            }
            uuid = get_uuid_as_string();
        }
        let consumers = self.inner.consumers.clone();
        let kk = cons.clone().lock_owned().await.get(&uuid).unwrap().clone();
        if consumers.lock().await.contains_key(address) {
            consumers.lock().await.get_mut(address).unwrap().insert(kk);
        } else {
            consumers.lock().await.insert(address.to_string(), HashSet::from([kk]));
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
        eb: Arc<EventBusInner<SyncLife>>,
        msg_data: &Message,
        callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife>>>>,
    ) {
        let msg = msg_data.clone();
        let address = msg.replay.clone();
        if let Some(address) = address {
            let mut map = callback_functions.lock().await;
            let callback = map.remove(&address);
            if let Some(caller) = callback {
                caller.call((&msg, eb));
            }
        }
    }

    #[inline]
    async fn call_func(
        eb: Arc<EventBusInner<SyncLife>>,
        consumers: Arc<Mutex<HashMap<String, HashSet<Arc<Consumers<SyncLife>>>>>>,
        _all_consumers: Arc<Mutex<HashMap<String, Arc<Consumers<SyncLife>>>>>,
        _callback_functions: Arc<Mutex<HashMap<String, BoxFnMessageImmutable<SyncLife>>>>,
        _eb_sender: Sender<Message>,
        msg_data: &mut Message,
        address: &str,
    ) {
        let consumers_tmp = consumers.clone();
        let hashmap = consumers_tmp.lock().await;
        // let msg_data_tmp = Arc::clone(&msg_data);
        if msg_data.publish {
            let consumers_list = hashmap.get(address).unwrap().iter();
            let mut kk_tmp: Vec<JoinHandle<()>> = vec![];
            for fun_item in consumers_list {
                let fun_item_tmp = fun_item.clone();
                let mut message_clone = msg_data.clone();
                let eb_tmp = Arc::clone(&eb);
                let kk2 = eb.runtime.spawn_blocking(move || {
                    fun_item_tmp.clone().consumers.call((&mut message_clone, eb_tmp.clone()));
                });
                kk_tmp.push(kk2);
            }
            join_all(kk_tmp).await;
        } else {
            let fun_call = &hashmap.get(address).unwrap().iter().next().unwrap().consumers;
            fun_call.call((msg_data, eb.clone()));
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
