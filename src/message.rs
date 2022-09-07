use std::collections::HashMap;
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError};
pub use prost::Message;
pub use prost;

include!(concat!(env!("OUT_DIR"), "/event_bus.message.rs"));


impl DefaultMessage {
    pub fn new() -> DefaultMessage {
        let mut message = DefaultMessage {
            address: None,
            replay: Some("asdfsdf".to_owned()),
            headers: HashMap::default(),
            body: Some(Vec::<u8>::default()),
        };
        println!("{:?}", message);

        message
    }

}

impl TryFrom<DefaultMessage>  for BytesMut{
    type Error = EncodeError;
    fn try_from(value: DefaultMessage) -> Result<Self, Self::Error> {
        let mut bytes11 = BytesMut::new();
        Message::encode(&value,&mut bytes11)?;
        Ok(bytes11)
    }
}


impl TryFrom<BytesMut>  for DefaultMessage{
    type Error = DecodeError;

    fn try_from(value: BytesMut) -> Result<DefaultMessage, DecodeError> {
        let data: DefaultMessage = Message::decode(value)?;
        Ok(data)
    }
}



