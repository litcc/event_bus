#![feature(fn_traits)]
#![feature(type_alias_impl_trait)]
#![feature(async_closure)]

use event_bus::core::{AsyncFn, EventBus};
use event_bus::message::{Body, VertxMessage};
use log::{info, Level, LevelFilter, Metadata, Record};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

struct MyLogger;

impl log::Log for MyLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

static MY_LOGGER: MyLogger = MyLogger;

// type FutureResult = impl Future<Output=()> + Send + 'static;
//
// type BoxFnMessage = Box<dyn Fn(String) -> FutureResult>;

#[tokio::main]
async fn main() {
    log::set_logger(&MY_LOGGER).unwrap();
    log::set_max_level(LevelFilter::Trace);

    // let df: BoxFnMessage = Box::new(async move |aasdf: String| {
    //     info!("你好,{}",aasdf);
    // });
    //
    // df.call(("asdfasdf".to_string(), )).await;

    let b = Arc::new(EventBus::<(), VertxMessage>::new(Default::default()));

    //b.start();
    // let mut bus = event_bus::core::get_instance();
    b.start().await;
    info!("启动EventBus成功");
    info!("发送消息");
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let kk = b.clone();
    let kk3 = b.clone();

    b.consumer("1", async move |_| info!("测试1"));

    b.consumer("1", async move |_| info!("测试1兄弟1"));

    b.consumer("1", async move |_| {
        info!("订阅1 收到消息");
    });

    b.consumer("2", async move |msg| {
        info!("订阅2 收到消息");

        info!("订阅2 进行回复");
        msg.msg.reply(Body::String("测试2".to_string())).await;
    });

    // tokio::spawn(async move {
    //     // loop {
    //     kk.send("1", Body::String("1".to_string())).await;
    //     tokio::time::sleep(Duration::from_secs(2000)).await;
    //     // }
    // });
    //
    //
    // tokio::spawn(async move {
    //     let mut _bus2 = EventBus::<()>::get_instance();
    //     loop {
    //     _bus2.publish("1", Body::String("3".to_string())).await;
    //     tokio::time::sleep(Duration::from_millis(1000)).await;
    //     }
    // });

    let kk234 = kk3.clone();
    tokio::spawn(async move {


        tokio::time::sleep(Duration::from_millis(1000)).await;
        // }
    });

    tokio::time::sleep(Duration::from_secs(120)).await;
}
