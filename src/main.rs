#![feature(async_closure)]
#![feature(fn_traits)]
#![feature(type_alias_impl_trait)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use log::{info, Level, LevelFilter, Metadata, Record};
use event_bus::core::{AsyncFn, EventBus, suspend_coroutine};
use event_bus::message::{Body, VertxMessage};
// extern crate event_bus;

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


    b.consumer("1", async move |_| {
        info!("测试1")
    }).await;

    b.consumer("1", async move |_| {
        info!("测试1兄弟1")
    }).await;

    b.consumer("1", async move |_| {
        info!("订阅1 收到消息");
    }).await;


    b.consumer("2", async move |msg| {
        info!("订阅2 收到消息");

        info!("订阅2 进行回复");
        msg.msg.reply(Body::String("测试2".to_string())).await;
    }).await;

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

        // loop {
        let kk2343 = kk234.clone();
        info!("开始执行耗时任务");
        let kk23423423 = suspend_coroutine(async move |result| {
            info!("执行耗时任务中");
            // let oo = result.clone();
            // kk2343.request("2", Body::String("2".to_string()), as   ync move |eb| {
            //     info!("新线程：收到订阅2回复");
            //     // let qwe1 = *eb.msg.body().await.clone();
            //     // oo.lock().await.resume(Some(qwe1));
            // }).await;
            // tokio::time::sleep(Duration::from_millis(2000)).await;

            result.resume(Some("".to_string())).await;
        }).await;
        info!("执行耗时任务结束: {:?}",kk23423423);

        tokio::time::sleep(Duration::from_millis(1000)).await;
        // }
    });


    tokio::time::sleep(Duration::from_secs(120)).await;
}
