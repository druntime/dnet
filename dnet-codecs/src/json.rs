//! JSON codec using [serde_json].

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

use dnet_base::{Decode, Encode};

pub use serde_json::Error;

/// JSON codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec {}

impl Encode for JsonCodec {
    type Error = self::Error;

    fn encode<W, T>(&mut self, writer: W, message: &T) -> Result<(), Self::Error>
    where
        W: Write,
        T: Serialize,
    {
        serde_json::to_writer(writer, message)
    }
}

impl Decode for JsonCodec {
    type Error = self::Error;

    fn decode<R, T>(&mut self, reader: R) -> Result<T, Self::Error>
    where
        R: Read,
        for<'de> T: Deserialize<'de>,
    {
        serde_json::from_reader(reader)
    }
}

#[cfg(test)]
mod tests {
    use dnet_base::{Decode, Encode};

    use crate::JsonCodec;

    #[test]
    fn test_codec() {
        let message: (i16, String) = (10, "Hello World!".to_string());
        let mut buffer = vec![];
        let mut codec = JsonCodec::default();
        codec.encode(&mut buffer, &message).unwrap();
        let decoded = codec.decode(&buffer[..]).unwrap();
        assert_eq!(message, decoded);
    }
}
