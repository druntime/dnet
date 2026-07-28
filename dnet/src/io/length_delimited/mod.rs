//! Transport using message length encoded as [u32] at the beginning of the message
//! to delimit messages.

use std::{
    fmt::{Debug, Display},
    pin::Pin,
    task::{Context, Poll},
};

use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

pub mod codec;
pub use codec::Codec;

use crate::io::Buffered;

use super::{framed, FramedTransport};

/// Default maximum message length.
pub const DEFAULT_MAX_MESSAGE_LENGTH: u32 = 65536;

/// Length-delimited transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Message was too long.
    MessageTooLong,

    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),

    /// IO error.
    IoError(std::io::Error),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MessageTooLong => write!(f, "message was too long"),
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
            Error::IoError(error) => write!(f, "IO error occurred: {error}"),
        }
    }
}

impl<SerializationError, DeserializationError> std::error::Error
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Debug + Display,
    DeserializationError: Debug + Display,
{
}

/// Transport using message length encoded as [u32] at the beginning of the message
/// to delimit messages.
///
/// Wraps over struct implementing [AsyncRead] and [AsyncWrite].
#[pin_project]
pub struct LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: FramedTransport<T, codec::Codec<Codec>, Incoming, Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn new(transport: T, codec: Codec) -> Self {
        LengthDelimitedTransport::new_with_max_message_length(
            transport,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
    }

    /// Create new transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn new_with_max_message_length(
        transport: T,
        codec: Codec,
        max_message_length: u32,
    ) -> Self {
        let codec = codec::Codec::new(codec, max_message_length);

        #[allow(unused_mut)]
        let mut inner = FramedTransport::new(transport, codec);

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            inner.with_logger_mut(|logger| logger.override_kind::<Self>());
        }

        LengthDelimitedTransport { inner }
    }
}

impl<T, Codec, Incoming, Outgoing> LengthDelimitedTransport<Buffered<T>, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn buffered(transport: T, codec: Codec) -> Self {
        Self::buffered_with_max_message_length(transport, codec, DEFAULT_MAX_MESSAGE_LENGTH)
    }

    /// Create new buffered transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn buffered_with_max_message_length(
        transport: T,
        codec: Codec,
        max_message_length: u32,
    ) -> Self {
        Self::new_with_max_message_length(
            BufReader::new(BufWriter::new(transport)),
            codec,
            max_message_length,
        )
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(map_error)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        self.project().inner.start_send(item).map_err(map_error)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(map_error)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(map_error)
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx).map_err(map_error_inner)
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream
    for LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for LengthDelimitedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "LengthDelimited";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        self.inner.with_logger(f)
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        self.inner.with_logger_mut(f)
    }
}

pub(crate) fn map_error<S, D>(
    error: crate::Error<framed::Error<codec::Error<S>, codec::Error<D>>>,
) -> crate::Error<self::Error<S, D>> {
    match error {
        crate::Error::Closed => crate::Error::Closed,
        crate::Error::Other(other) => crate::Error::Other(map_error_inner(other)),
    }
}

pub(crate) fn map_error_inner<S, D>(
    error: framed::Error<codec::Error<S>, codec::Error<D>>,
) -> self::Error<S, D> {
    match error {
        super::framed::Error::SerializationError(error) => match error {
            codec::Error::MessageTooLong => self::Error::MessageTooLong,
            codec::Error::NotEnoughData => unreachable!("not enough data error should be consumed"),
            codec::Error::IoError(error) => self::Error::IoError(error),
            codec::Error::SerializationError(error) => self::Error::SerializationError(error),
        },
        super::framed::Error::DeserializationError(error) => match error {
            codec::Error::MessageTooLong => self::Error::MessageTooLong,
            codec::Error::NotEnoughData => unreachable!("not enough data error should be consumed"),
            codec::Error::IoError(error) => self::Error::IoError(error),
            codec::Error::SerializationError(error) => self::Error::DeserializationError(error),
        },
        super::framed::Error::IoError(error) => self::Error::IoError(error),
    }
}
