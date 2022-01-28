#![feature(get_mut_unchecked)]



use std::time::Duration;
use log::{info, Level, LevelFilter, Metadata, Record};
use event_bus::core::{EventBus};
use event_bus::message::Body;
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


#[tokio::main]
async fn main() {
    log::set_logger(&MY_LOGGER).unwrap();
    log::set_max_level(LevelFilter::Trace);

    /*let b = EventBus::<(),>::init(Default::default());

    //b.start();
    // let mut bus = event_bus::core::get_instance();
    b.start().await;
    info!("启动EventBus成功");
    info!("发送消息");
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let kk = b.clone();
    let kk3 = b.clone();


    b.consumer("1", move |_, _| {
        info!("测试1")
    }).await;

    b.consumer("1", |_, _| {
        info!("测试1兄弟1")
    }).await;

    b.consumer("1", |_, event_bus| {
        event_bus.clone().runtime.spawn(async move {
            info!("测试1兄弟2");
        });
    }).await;


    b.consumer("2", |msg, event_bus| {
        info!("收到请求：测试1兄弟2");
        msg.reply(Body::String("测试2".to_string()));

        // event_bus.clone().runtime.spawn(async move {
        //
        // });
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

    tokio::spawn(async move {
        // loop {
            kk3.request("2", Body::String("2".to_string()),|_,_|{
                info!("kk3收到回复");
            }).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
        // }
    });


    tokio::time::sleep(Duration::from_secs(120)).await;*/
}
