use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;

use futures::task::AtomicWaker;
use log::{debug, info, trace};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Mutex;

use crate::message::Body::{Byte, ByteArray, Short};
//借鉴与vertx-rust

#[derive(Debug, Clone)] // , Copy
pub enum Body {
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    ByteArray(Vec<u8>),
    Boolean(bool),
    Char(char),
    Null,
    Ping,
}

// impl Clone for Body {
//     fn clone(&self) -> Self {
//         match self {
//             Body::ByteArray(bytes) => Body::ByteArray(Vec::clone(bytes)),
//             Body::String(str) => Body::String(String::clone(str)),
//             Body::Byte(byte) => Body::Byte(byte.clone()),
//             Body::Int(int) => Body::Int(int.clone()),
//             Body::Long(long) => Body::Long(long.clone()),
//             Body::Float(float) => Body::Float(float.clone()),
//             Body::Double(double) => Body::Double(double.clone()),
//             Body::Short(short) => Body::Short(short.clone()),
//             Body::Boolean(bool) => Body::Boolean(bool.clone()),
//             Body::Char(char) => Body::Char(char.clone()),
//             Body::Null => Body::Null,
//             Body::Ping => Body::Ping,
//         }
//     }
// }

impl Body {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Body::Null)
    }

    #[inline]
    pub fn as_bool(&self) -> Result<bool, &str> {
        match self {
            Body::Boolean(s) => Ok(*s),
            _ => Err("Body type is not a bool"),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Result<f64, &str> {
        match self {
            Body::Double(s) => Ok(*s),
            _ => Err("Body type is not a f64"),
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Result<f32, &str> {
        match self {
            Body::Float(s) => Ok(*s),
            _ => Err("Body type is not a f32"),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, &str> {
        match self {
            Body::Long(s) => Ok(*s),
            _ => Err("Body type is not a i64"),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, &str> {
        match self {
            Body::Int(s) => Ok(*s),
            _ => Err("Body type is not a i32"),
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Result<i16, &str> {
        match self {
            Short(s) => Ok(*s),
            _ => Err("Body type is not a i16"),
        }
    }

    #[inline]
    pub fn as_u8(&self) -> Result<u8, &str> {
        match self {
            Byte(s) => Ok(*s),
            _ => Err("Body type is not a u8"),
        }
    }

    #[inline]
    pub fn as_string(&self) -> Result<&String, &str> {
        match self {
            Body::String(s) => Ok(s),
            _ => Err("Body type is not a String"),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&Vec<u8>, &str> {
        match self {
            ByteArray(s) => Ok(s),
            _ => Err("Body type is not a Byte Array"),
        }
    }
}

impl Into<Vec<u8>> for Body {
    fn into(self) -> Vec<u8> {
        self.as_bytes().unwrap().to_vec()
    }
}

impl Into<Body> for Vec<u8> {
    fn into(self) -> Body {
        Body::ByteArray(self)
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::Null
    }
}

//Sized
// #[derive(Clone)]
pub struct IMessage {
    pub(crate) data: Arc<Mutex<Box<dyn IMessageData + 'static + Send + Sync>>>,
    pub(crate) replay_future: Option<IMessageReplayFuture>,
}

impl Clone for IMessage {
    fn clone(&self) -> Self {
        if self.replay_future.is_some() {
            IMessage {
                data: self.data.clone(),
                replay_future: Some(self.replay_future.as_ref().unwrap().clone()),
            }
        } else {
            IMessage {
                data: self.data.clone(),
                replay_future: None,
            }
        }
    }
}

#[derive(Clone)]
pub struct IMessageReplayFuture {
    pub(crate) is_reply: Arc<AtomicBool>,
    pub(crate) waker: Arc<AtomicWaker>,
}

// impl Clone for IMessageReplayFuture
// {
//     fn clone(&self) -> Self {
//         IMessageReplayFuture {
//             is_reply: Arc::clone(&self.is_reply),
//             waker: Arc::clone(&self.waker),
//         }
//     }
// }

impl Future for IMessageReplayFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        debug!("IMessageReplayFuture poll");
        self.waker.register(cx.waker());

        if self.is_reply.load(Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl IMessageReplayFuture {
    pub fn new() -> Self {
        IMessageReplayFuture {
            is_reply: Arc::new(AtomicBool::new(false)),
            waker: Arc::new(futures::task::AtomicWaker::new()),
        }
    }

    pub async fn await_future(&mut self) {
        self.await;
    }
}

impl std::fmt::Debug for IMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kk = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move { self.data.lock().await.to_string() });
        f.debug_tuple("").field(&kk).finish()
    }
}

impl std::fmt::Display for IMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kk = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move { self.data.lock().await.to_string() });
        write!(f, "({})", kk)
    }
}

impl IMessage {
    pub fn new(data: Box<dyn IMessageData + 'static + Send + Sync>) -> Self {
        Self {
            data: Arc::new(Mutex::new(data)),
            replay_future: None,
        }
    }

    pub fn new_replay(
        data: Box<dyn IMessageData + 'static + Send + Sync>,
        replay_future: IMessageReplayFuture,
    ) -> Self {
        Self {
            data: Arc::new(Mutex::new(data)),
            replay_future: Some(replay_future),
        }
    }

    // pub(crate) fn set_replay_waker(&self,) -> Arc<Mutex<Box<dyn IMessageData + 'static + Send + Sync>>> {
    //     self.data.clone()
    // }

    #[inline]
    pub(crate) async fn send_address(&self) -> Option<String> {
        let addr = &self.data.lock().await.send_address();
        return addr.clone();
    }

    #[inline]
    pub(crate) async fn replay_address(&self) -> Option<String> {
        let replay_address = self.data.lock().await.replay_address();
        let send_address = self.data.lock().await.send_address();
        match replay_address {
            Some(ref addr) => {
                if addr.starts_with("__EventBus.reply.") {
                    Some(addr.to_string())
                } else {
                    None
                }
            }
            None => match send_address {
                Some(ref addr_s) => {
                    if addr_s.starts_with("__EventBus.reply.") {
                        Some(addr_s.to_string())
                    } else {
                        None
                    }
                }
                None => None,
            },
        }
    }

    #[inline]
    pub async fn body(&self) -> Arc<Body> {
        self.data.lock().await.body()
    }

    #[inline]
    pub async fn reply(&self, data: Body) {
        if self.can_reply().await {
            self.data.lock().await.as_mut().reply(data);
            if let Some(fut) = self.replay_future.clone() {
                fut.is_reply.store(true, Relaxed);
                fut.waker.wake();
                // fut.is_reply.store(true, Relaxed);
                // fut.waker.wake();
                debug!("内部执行回复完成");
            }
        } else {
            debug!("无法回复");
        }
    }

    #[inline]
    pub(crate) async fn headers(&self) -> Arc<HashMap<String, Body>> {
        self.data.lock().await.headers()
    }

    #[inline]
    pub(crate) async fn is_publish(&self) -> bool {
        self.data.lock().await.is_publish()
    }

    #[inline]
    pub(crate) async fn is_reply(&self) -> bool {
        let address = self.data.lock().await.send_address();
        let replay_address = self.data.lock().await.replay_address();
        debug!("replay_address address: {:?}", replay_address);
        debug!("send_address address: {:?}", address);
        match address {
            Some(ref addr) => {
                if addr.starts_with("__EventBus.reply.") {
                    debug!("is_reply: true");
                    true
                } else {
                    debug!("is_reply: false");
                    false
                }
            }
            None => {
                debug!("is_reply: false");
                false
            }
        }
    }

    #[inline]
    pub(crate) async fn can_reply(&self) -> bool {
        let replay_address = self.data.lock().await.replay_address();
        let send_address = self.data.lock().await.send_address();
        // info!("replay_address address: {:?}", replay_address);
        // info!("send_address address: {:?}", send_address);
        match replay_address {
            Some(ref addr) => {
                if addr.starts_with("__EventBus.reply.") {
                    debug!("can_reply: true");
                    true
                } else {
                    debug!("can_reply: false");
                    false
                }
            }
            None => {
                debug!("can_reply: false");
                false
            }
        }
    }

    #[inline]
    pub(crate) async fn to_string(&self) -> String {
        self.data.lock().await.to_string()
    }
}

pub trait IMessageData {
    fn body(&self) -> Arc<Body>;
    fn reply(&mut self, data: Body);
    fn send_address(&self) -> Option<String>;
    fn replay_address(&self) -> Option<String>;
    fn headers(&self) -> Arc<HashMap<String, Body>>;
    fn is_publish(&self) -> bool;
    fn to_string(&self) -> String;
    fn build_send_data(address: &str, body: Body) -> Box<dyn IMessageData + 'static + Send + Sync>
    where
        Self: Sized;
    fn build_request_data(
        address: &str,
        replay_address: &str,
        body: Body,
    ) -> Box<dyn IMessageData + 'static + Send + Sync>
    where
        Self: Sized;
    fn build_publish_data(
        address: &str,
        body: Body,
    ) -> Box<dyn IMessageData + 'static + Send + Sync>
    where
        Self: Sized;
}

#[derive(Clone, Default, Debug)]
pub struct VertxMessage {
    // 目标地址
    pub(crate) address: Option<String>,
    // 回复地址
    pub(crate) replay: Option<String>,
    // 二进制正文内容
    pub(crate) body: Arc<Body>,
    // 协议版本
    #[allow(dead_code)]
    pub(crate) protocol_version: i32,
    // 系统编解码器 ID
    #[allow(dead_code)]
    pub(crate) system_codec_id: i32,
    // 回复消息端口
    pub(crate) port: i32,
    // 回复消息ip
    pub(crate) host: String,
    // 头信息
    #[allow(dead_code)]
    pub(crate) headers: i32,
    // 是否发送到所有对应地址的消费者
    pub(crate) publish: bool,
}

impl IMessageData for VertxMessage {
    fn body(&self) -> Arc<Body> {
        self.body.clone()
    }

    #[inline]
    fn reply(&mut self, data: Body) {
        self.body = Arc::new(data);
        self.address = self.replay.clone();
        self.replay = None;
    }

    #[inline]
    fn send_address(&self) -> Option<String> {
        self.address.clone()
    }

    #[inline]
    fn replay_address(&self) -> Option<String> {
        self.replay.clone()
    }

    #[inline]
    fn headers(&self) -> Arc<HashMap<String, Body>> {
        Arc::new(HashMap::new())
    }

    #[inline]
    fn is_publish(&self) -> bool {
        self.publish
    }

    #[inline]
    fn to_string(&self) -> String {
        // format!("{}", serde_json::to_string(self).unwrap())
        "".to_string()
    }

    fn build_send_data(address: &str, body: Body) -> Box<dyn IMessageData + 'static + Send + Sync> {
        Box::new(VertxMessage {
            address: Some(address.to_owned()),
            replay: None,
            body: Arc::new(body),
            ..Default::default()
        })
    }

    fn build_request_data(
        address: &str,
        replay_address: &str,
        body: Body,
    ) -> Box<dyn IMessageData + 'static + Send + Sync> {
        Box::new(VertxMessage {
            address: Some(address.to_owned()),
            replay: Some(replay_address.to_owned()),
            body: Arc::new(body),
            ..Default::default()
        })
    }

    fn build_publish_data(
        address: &str,
        body: Body,
    ) -> Box<dyn IMessageData + 'static + Send + Sync> {
        Box::new(VertxMessage {
            address: Some(address.to_owned()),
            replay: None,
            body: Arc::new(body),
            publish: true,
            ..Default::default()
        })
    }
}

//Implementation of deserialize byte array to message
impl From<Vec<u8>> for VertxMessage {
    #[inline]
    fn from(msg: Vec<u8>) -> Self {
        let mut idx = 1;
        let system_codec_id = i8::from_be_bytes(msg[idx..idx + 1].try_into().unwrap()) as i32;
        idx += 2;
        let len_addr = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let address = String::from_utf8(msg[idx..idx + len_addr].to_vec()).unwrap();
        idx += len_addr;
        let len_replay = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let mut replay = None;
        if len_replay > 0 {
            let replay_str = String::from_utf8(msg[idx..idx + len_replay].to_vec()).unwrap();
            idx += len_replay;
            replay = Some(replay_str);
        }
        let port = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let len_host = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let host = String::from_utf8(msg[idx..idx + len_host].to_vec()).unwrap();
        idx += len_host;
        let headers = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let body;
        match system_codec_id {
            0 => body = Body::Null,
            1 => body = Body::Ping,
            2 => body = Body::Byte(u8::from_be_bytes(msg[idx..idx + 1].try_into().unwrap())),
            3 => {
                body = Body::Boolean(i8::from_be_bytes(msg[idx..idx + 1].try_into().unwrap()) == 1)
            }
            4 => body = Body::Short(i16::from_be_bytes(msg[idx..idx + 2].try_into().unwrap())),
            5 => body = Body::Int(i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap())),
            6 => body = Body::Long(i64::from_be_bytes(msg[idx..idx + 8].try_into().unwrap())),
            7 => body = Body::Float(f32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap())),
            8 => body = Body::Double(f64::from_be_bytes(msg[idx..idx + 8].try_into().unwrap())),
            9 => {
                let len_body = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
                idx += 4;
                let body_array = msg[idx..idx + len_body].to_vec();
                body = Body::String(String::from_utf8(body_array).unwrap())
            }
            10 => {
                body =
                    Body::Char(
                        char::from_u32(
                            i16::from_be_bytes(msg[idx..idx + 2].try_into().unwrap()) as u32
                        )
                        .unwrap(),
                    )
            }
            12 => {
                let len_body = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
                idx += 4;
                let body_array = msg[idx..idx + len_body].to_vec();
                body = Body::ByteArray(body_array)
            }
            _ => panic!("system_codec_id: {} not supported", system_codec_id),
        }

        VertxMessage {
            address: Some(address),
            replay,
            port,
            host,
            headers,
            body: Arc::new(body),
            system_codec_id,
            ..Default::default()
        }
    }
}

impl VertxMessage {
    //Serialize message to byte array
    #[inline]
    pub fn to_vec(&self) -> Result<Vec<u8>, &str> {
        let mut data = Vec::with_capacity(2048);
        data.push(1);

        match self.body.deref() {
            Body::Int(_) => {
                data.push(5);
            }
            Body::Long(_) => {
                data.push(6);
            }
            Body::Float(_) => {
                data.push(7);
            }
            Body::Double(_) => {
                data.push(8);
            }
            Body::String(_) => {
                data.push(9);
            }
            Body::ByteArray(_) => {
                data.push(12);
            }
            Body::Boolean(_) => {
                data.push(3);
            }
            Body::Null => {
                data.push(0);
            }
            Body::Byte(_) => {
                data.push(2);
            }
            Body::Short(_) => {
                data.push(4);
            }
            Body::Char(_) => {
                data.push(10);
            }
            Body::Ping => {
                data.push(1);
            }
        };

        data.push(0);
        if let Some(address) = &self.address {
            data.extend_from_slice(&(address.len() as i32).to_be_bytes());
            data.extend_from_slice(address.as_bytes());
        }
        match &self.replay {
            Some(addr) => {
                data.extend_from_slice(&(addr.len() as i32).to_be_bytes());
                data.extend_from_slice(addr.as_bytes());
            }
            None => {
                data.extend_from_slice(&(0_i32).to_be_bytes());
            }
        }
        data.extend_from_slice(&self.port.to_be_bytes());
        data.extend_from_slice(&(self.host.len() as i32).to_be_bytes());
        data.extend_from_slice(self.host.as_bytes());
        data.extend_from_slice(&(4_i32).to_be_bytes());

        match self.body.deref() {
            Body::Int(b) => {
                data.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Long(b) => {
                data.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Float(b) => {
                data.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Double(b) => {
                data.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::String(b) => {
                data.extend_from_slice(&(b.len() as i32).to_be_bytes());
                data.extend_from_slice(b.as_bytes());
            }
            Body::ByteArray(b) => {
                data.extend_from_slice(&(b.len() as i32).to_be_bytes());
                data.extend_from_slice(b.as_slice());
            }
            Body::Boolean(b) => {
                if *b {
                    data.extend_from_slice((1_i8).to_be_bytes().as_slice())
                } else {
                    data.extend_from_slice((0_i8).to_be_bytes().as_slice())
                }
            }
            Body::Byte(b) => {
                data.push(*b);
            }
            Body::Short(b) => {
                data.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Char(b) => {
                data.extend_from_slice((((*b) as u32) as i16).to_be_bytes().as_slice());
            }
            _ => {}
        };

        let len = ((data.len()) as i32).to_be_bytes();
        for idx in 0..4 {
            data.insert(idx, len[idx]);
        }
        Ok(data)
    }
}
