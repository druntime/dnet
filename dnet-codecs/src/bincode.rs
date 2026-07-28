//! Binary codec using [bincode].
//!
//! [bincode]: https://docs.rs/bincode/

use bincode::config::Configuration;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

use dnet_base::{Decode, Encode};

pub use bincode::error::DecodeError;
pub use bincode::error::EncodeError;

/// Binary (bincode) codec.
#[derive(Clone, Copy, Default)]
pub struct BincodeCodec {
    /// Bincode configuration.
    pub configuration: Configuration,
}

impl Encode for BincodeCodec {
    type Error = self::EncodeError;

    fn encode<W, T>(&mut self, mut writer: W, message: &T) -> Result<(), Self::Error>
    where
        W: Write,
        T: Serialize,
    {
        bincode::serde::encode_into_std_write(message, &mut writer, self.configuration).map(|_| ())
    }
}

impl Decode for BincodeCodec {
    type Error = self::DecodeError;

    fn decode<R, T>(&mut self, mut reader: R) -> Result<T, Self::Error>
    where
        R: Read,
        for<'de> T: Deserialize<'de>,
    {
        bincode::serde::decode_from_std_read(&mut reader, self.configuration)
    }
}

#[cfg(test)]
mod tests {
    use dnet_base::{Decode, Encode};

    use crate::BincodeCodec;

    #[test]
    fn test_codec() {
        let message: (i16, String) = (10, "Hello World!".to_string());
        let mut buffer = vec![];
        let mut codec = BincodeCodec::default();
        codec.encode(&mut buffer, &message).unwrap();
        let decoded = codec.decode(&buffer[..]).unwrap();
        assert_eq!(message, decoded);
    }
}
