//! Length-delimiting codec - this codec writes [u32] length message
//! at the beginning of each message.

use std::fmt::{Debug, Display};

use bytes::{BufMut, BytesMut};
use dnet_base::{Decode, Encode};
use serde::Serialize;

use crate::io::framed::{DecodeWithMessageLength, NotEnoughData};

/// Length-delimiting codec error
#[derive(Debug)]
pub enum Error<SerializationError> {
    /// Message is too long.
    MessageTooLong,

    /// Not enough data in codec's decoding buffer to be able to decode a message.
    NotEnoughData,

    /// IO error.
    IoError(std::io::Error),

    /// (De)serialization error.
    SerializationError(SerializationError),
}

impl<SerializationError> Display for Error<SerializationError>
where
    SerializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MessageTooLong => write!(f, "message is too long"),
            Error::NotEnoughData => write!(f, "not enough bytes to decode a message"),
            Error::IoError(error) => write!(f, "IO error occurred: {error}"),
            Error::SerializationError(error) => write!(f, "serialization error occurred: {error}"),
        }
    }
}

impl<SerializationError> std::error::Error for Error<SerializationError> where
    SerializationError: Debug + Display
{
}

impl<SerializationError> From<std::io::Error> for Error<SerializationError> {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

impl<SerializationError> NotEnoughData for Error<SerializationError> {
    fn not_enough_data(&self) -> bool {
        matches!(self, Error::NotEnoughData)
    }
}

/// Codec encoding [u32] message length at the beginning of the message and
/// using it to delimit messages.
pub struct Codec<Inner>
where
    Inner: crate::Codec,
{
    /// Should codec skip to the next message if it encounters too long message while decoding.
    ///
    /// If [`false`] decoding after encountering too long message will keep failing
    /// with [Error::MessageTooLong] error - it may be desired if mismatched
    /// max message length configurations on both sides of the connection are not allowed.
    ///
    /// Default is [`true`].
    pub skipping_enabled: bool,
    inner: Inner,
    max_message_length: u32,
    encoding_buffer: BytesMut,
    decoding_buffer: Vec<u8>,
    expected_message_length: Option<u32>,
    to_skip: u32,
}

impl<Inner> Codec<Inner>
where
    Inner: crate::Codec,
{
    /// Create new length-delimiting codec wrapping provided codec.
    pub fn new(inner: Inner, max_message_length: u32) -> Self {
        Self {
            skipping_enabled: true,
            inner,
            max_message_length,
            encoding_buffer: BytesMut::new(),
            decoding_buffer: vec![],
            expected_message_length: None,
            to_skip: 0,
        }
    }

    fn expected_message_length<SerializationError>(
        &mut self,
    ) -> Result<u32, Error<SerializationError>> {
        if let Some(expected_message_length) = self.expected_message_length {
            Ok(expected_message_length)
        } else if self.decoding_buffer.len() >= 4 {
            let new_length = u32::from_be_bytes(self.decoding_buffer[0..4].try_into().unwrap());
            if new_length <= self.max_message_length {
                self.expected_message_length = Some(new_length);
                Ok(new_length)
            } else {
                if self.skipping_enabled {
                    self.to_skip = new_length + 4;
                }
                Err(Error::MessageTooLong)
            }
        } else {
            Err(Error::NotEnoughData)
        }
    }

    fn shift_decoding_buffer(&mut self, target_index: usize) {
        let old_length = self.decoding_buffer.len();
        let new_length = old_length - target_index;
        self.decoding_buffer
            .copy_within(target_index..old_length, 0);
        self.decoding_buffer.truncate(new_length);
    }
}

impl<Inner> Encode for Codec<Inner>
where
    Inner: crate::Codec,
{
    type Error = Error<<Inner as Encode>::Error>;

    fn encode<W, T>(&mut self, mut writer: W, message: &T) -> Result<(), Self::Error>
    where
        W: std::io::Write,
        T: Serialize,
    {
        self.encoding_buffer.clear();
        self.encoding_buffer.put_u32(0);
        let length_length = self.encoding_buffer.len();
        self.inner
            .encode((&mut self.encoding_buffer).writer(), message)
            .map_err(Error::SerializationError)?;
        let message_length = self.encoding_buffer.len() - length_length;
        if message_length > self.max_message_length as usize {
            Err(Error::MessageTooLong)
        } else {
            let size_slice = &mut self.encoding_buffer[0..length_length];
            let message_length = message_length as u32;
            size_slice.swap_with_slice(&mut message_length.to_be_bytes());
            writer.write_all(&self.encoding_buffer)?;
            Ok(())
        }
    }
}

impl<Inner> Decode for Codec<Inner>
where
    Inner: crate::Codec,
{
    type Error = Error<<Inner as Decode>::Error>;

    fn decode<R, T>(&mut self, data: R) -> Result<T, Self::Error>
    where
        R: std::io::Read,
        for<'de> T: serde::Deserialize<'de>,
    {
        self.decode_with_message_length(data)
            .map(|(message, _)| message)
    }
}

impl<Inner> DecodeWithMessageLength for Codec<Inner>
where
    Inner: crate::Codec,
{
    fn decode_with_message_length<R, T>(&mut self, mut data: R) -> Result<(T, usize), Self::Error>
    where
        R: std::io::Read,
        for<'de> T: serde::Deserialize<'de>,
    {
        data.read_to_end(&mut self.decoding_buffer)?;

        if self.to_skip > 0 {
            let current_length = self.decoding_buffer.len() as u32;
            if self.to_skip > current_length {
                self.to_skip -= current_length;
                self.decoding_buffer.clear();
                return Err(Error::NotEnoughData);
            } else {
                self.shift_decoding_buffer(self.to_skip as usize);
                self.to_skip = 0;
            }
        }

        let message_end = self.expected_message_length()? as usize + 4;
        if self.decoding_buffer.len() >= message_end {
            let message = self
                .inner
                .decode(&self.decoding_buffer[4..message_end])
                .map_err(Error::SerializationError)?;
            self.expected_message_length = None;
            self.shift_decoding_buffer(message_end);
            Ok((message, message_end))
        } else {
            Err(Error::NotEnoughData)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, io::BufReader};

    use dnet_base::{Decode, Encode};
    use dnet_codecs::{json, JsonCodec};

    use crate::io::length_delimited::Codec;

    #[test]
    fn test_codec() {
        let message_1: (i16, String) = (10, "Hello World!".to_string());
        let message_2: String = "Second message".to_string();
        let mut buffer = vec![];
        let mut codec = Codec::new(JsonCodec::default(), 100);
        codec.encode(&mut buffer, &message_1).unwrap();
        codec.encode(&mut buffer, &message_2).unwrap();
        let mut reader = BufReader::new(&buffer[..]);
        let decoded = codec.decode(&mut reader).unwrap();
        assert_eq!(message_1, decoded);
        let decoded: String = codec.decode(&mut reader).unwrap();
        assert_eq!(message_2, decoded);
    }

    #[test]
    fn test_message_too_long_encode() {
        let mut buffer = vec![];

        let mut codec = Codec::new(JsonCodec::default(), 10);
        let result = codec.encode(&mut buffer, &"Hello!");
        assert!(matches!(result, Ok(())));
        let result = codec.encode(&mut buffer, &"Hello World!");
        assert!(matches!(result, Err(super::Error::MessageTooLong)));
    }

    #[test]
    fn test_message_too_long_decode() {
        let mut buffer = vec![];

        let mut encoder = Codec::new(JsonCodec::default(), 100);
        let mut decoder = Codec::new(JsonCodec::default(), 10);
        decoder.skipping_enabled = false;
        let decoder = RefCell::new(decoder);

        encoder.encode(&mut buffer, &"Hello!").unwrap();
        encoder.encode(&mut buffer, &"Hello World!").unwrap();
        encoder.encode(&mut buffer, &"Bye!").unwrap();

        let mut reader = BufReader::new(&buffer[..]);
        let mut decode = || {
            let result: Result<String, super::Error<json::Error>> =
                decoder.borrow_mut().decode(&mut reader);
            result
        };

        assert_eq!(decode().unwrap(), "Hello!");
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));

        decoder.borrow_mut().skipping_enabled = true;
        assert!(matches!(decode(), Err(super::Error::MessageTooLong)));
        assert_eq!(decode().unwrap(), "Bye!");
        assert!(matches!(decode(), Err(super::Error::NotEnoughData)));
    }
}
