use wincode::{ReadResult, SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead, PartialEq, Debug)]
pub enum Message {
    PING,
    PONG,
    ACK,
    ERROR(String),
}

impl Message {
    pub fn serialize(&self) -> Vec<u8> {
        wincode::serialize(self).expect("Failed to serialize message")
    }

    pub fn deserialize(data: &[u8]) -> ReadResult<Message> {
        wincode::deserialize(data)
    }
}
