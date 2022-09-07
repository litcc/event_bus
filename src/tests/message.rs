use bytes::BytesMut;
use prost::Message;
use crate::message::DefaultMessage;

#[tokio::test]
async fn it_works() {
    let kk: DefaultMessage = DefaultMessage::new();
    let bytes11 = BytesMut::try_from(kk);
    println!("{:?}", bytes11);
    let kkk = bytes11.unwrap();
    let data1 = DefaultMessage::try_from(kkk);
    println!("{:?}", data1);
}