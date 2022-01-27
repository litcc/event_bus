use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use log::{debug, info, Level, LevelFilter, Metadata, Record};
use event_bus::core::{EventBus, EventBusOptions};
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


    let b = EventBus::<()>::init(Default::default());

    //b.start();
    // let mut bus = event_bus::core::get_instance();
    b.lock().await.start().await;
    info!("启动EventBus成功");
    info!("发送消息");
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let kk = Arc::clone(&b);
    let kk3 = Arc::clone(&b);


    b.lock().await.consumer("1", move  |_, _| {

        info!("测试1")
    }).await;

    b.lock().await.consumer("1",|_, _|{
        info!("测试1兄弟1")
    }).await;

    b.lock().await.consumer("1",|_, _|{
        info!("测试1兄弟2")
    }).await;

    tokio::spawn(async move {
        // loop {
            kk.lock().await.send("1", Body::String("1".to_string())).await;
            tokio::time::sleep(Duration::from_secs(2000)).await;
        // }
    });


    tokio::spawn(async move {
        let mut _bus2 = EventBus::<()>::get_instance();
        // loop {
            _bus2.lock().await.publish("1", Body::String("3".to_string())).await;
            tokio::time::sleep(Duration::from_millis(2000)).await;
        // }
    });

    tokio::spawn(async move {
        loop {
            kk3.lock().await.send("2", Body::String("2".to_string())).await;
            tokio::time::sleep(Duration::from_millis(100000)).await;
        }
    }).await.unwrap();
}
