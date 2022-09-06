use event_bus::core::EventBus;
use event_bus::message::{Body, VertxMessage};
use log::{info, LevelFilter};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;


// type FutureResult = impl Future<Output=()> + Send + 'static;
//
// type BoxFnMessage = Box<dyn Fn(String) -> FutureResult>;

pub fn random_string(num: u32) -> String {
    // println!("num: {} ", num);
    let str = "123456789abcdef".chars().collect::<Vec<char>>();
    let mut ret_str = String::new();
    for _i in 0..num {
        let nums = rand::thread_rng().gen_range(0..str.len());
        let k = str[nums];
        ret_str.push(k);
        // println!("添加: {} , 字符串总共: {}", k, ret_str);
    }
    return ret_str;
}

#[tokio::main]
async fn main() {
    // log::set_logger(&MY_LOGGER).unwrap();
    log::set_max_level(LevelFilter::Trace);

    // let df: BoxFnMessage = Box::new(async move |aasdf: String| {
    //     info!("你好,{}",aasdf);
    // });
    //
    // df.call(("asdfasdf".to_string(), )).await;

    let b = Arc::new(EventBus::<VertxMessage>::new(Default::default()));

    //b.start();
    // let mut bus = event_bus::core::get_instance();
    b.start().await;
    info!("启动EventBus成功");
    info!("发送消息");
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let kk3 = b.clone();

    b.consumer("1", move |_| async {
        info!("订阅1-1 收到消息");
    })
    .await;

    b.consumer("1", move |_| async { info!("订阅1-2 收到消息") })
        .await;

    b.consumer("1", move |_| async {
        info!("订阅1-3 收到消息");
    })
    .await;

    b.consumer("2", move |msg| async move {

        let fdf = {
            let mut rng = rand::thread_rng();
            rng.gen_range(1..20)
        };
        tokio::time::sleep(Duration::from_secs(fdf)).await;
        msg.msg.reply(Body::String(fdf.to_string())).await;
    })
    .await;

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

    for _i in 1..1000 {
        let kk234 = kk3.clone();
        tokio::spawn(async move {
            let id = random_string(5);
            info!("send {}", id);
            let df = kk234.request("2", Body::String("请求".to_string())).await;
            if df.is_some() {
                info!("get {} {:?}", id, df.unwrap());
            }
            // }
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
    // tokio::time::sleep(Duration::from_secs(120)).await;
}
