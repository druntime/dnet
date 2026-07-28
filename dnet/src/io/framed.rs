//! Transport using codec errors implementing [NotEnoughData] to delimit messages
//! (recognize when more bytes are needed to successfully decode a message).

use std::{
    fmt::{Debug, Display},
    io::ErrorKind,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, BytesMut};
use dnet_base::{Decode, Encode};
use futures::{ready, stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use serde::Serialize;
use tokio::io::{self, AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio_util::io::{poll_read_buf, poll_write_buf};

use crate::io::Buffered;

/// Extension of [Decode] trait for codecs that can decode messages and
/// return the size of the message along the message itself.
///
/// Used for logging.
pub trait DecodeWithMessageLength: Decode {
    /// Decode message and return the size of the message along the message itself.
    fn decode_with_message_length<R, T>(&mut self, data: R) -> Result<(T, usize), Self::Error>
    where
        R: std::io::Read,
        for<'de> T: serde::Deserialize<'de>;
}

/// Codecs implementing this trait can be used in [FramedTransport].
pub trait Framing: DecodeWithMessageLength {}

impl<T> Framing for T
where
    T: Decode + DecodeWithMessageLength,
    <T as Decode>::Error: NotEnoughData,
{
}

/// Trait for codec decoding errors that can be used in [FramedTransport].
///
/// It signals there was not enough data (bytes) available to successfully decode a
/// message.
pub trait NotEnoughData {
    /// Not enough data (bytes) to decode a message.
    ///
    /// **NOTE**: Codecs implementing this trait need to hold an internal buffer
    /// to store already received data (sent to [Decode::decode] method) - it will
    /// NOT be sent again during the next try - only new data will be provided.
    fn not_enough_data(&self) -> bool;
}

/// Framed transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
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

/// Transport using codec errors implementing [NotEnoughData] to delimit messages.
///
/// Wraps over struct implementing [AsyncRead] and [AsyncWrite].
#[pin_project]
pub struct FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: T,
    send_buffer: BytesMut,
    receive_buffer: BytesMut,
    codec: Codec,
    terminated: bool,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    pub fn new(transport: T, codec: Codec) -> Self {
        FramedTransport {
            inner: transport,
            codec,
            send_buffer: BytesMut::new(),
            receive_buffer: BytesMut::new(),
            terminated: false,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

impl<T, Codec, Incoming, Outgoing> FramedTransport<Buffered<T>, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    pub fn buffered(transport: T, codec: Codec) -> Self {
        #[allow(unused_mut)]
        let mut transport = Self::new(BufReader::new(BufWriter::new(transport)), codec);

        #[cfg(feature = "logging")]
        transport.logger.override_kind_buffered::<Self>();

        transport
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing> for FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        if self.terminated {
            let error = crate::Error::Closed;

            #[cfg(feature = "logging")]
            self.logger.log_sending_failure(&error);

            Err(error)
        } else {
            let me = self.project();

            #[cfg(feature = "logging")]
            let send_buffer_len_before = me.send_buffer.len();

            let result = me
                .codec
                .encode(me.send_buffer.writer(), &item)
                .map_err(Error::SerializationError)
                .map_err(crate::Error::Other);

            #[cfg(feature = "logging")]
            me.logger.log_message_preparation::<Outgoing, _, _>(
                send_buffer_len_before,
                me.send_buffer.len(),
                &result,
            );

            result
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut me = self.project();

        let result = loop {
            if me.send_buffer.has_remaining() {
                if let Err(error) = ready!(poll_write_buf(me.inner.as_mut(), cx, me.send_buffer)) {
                    break Poll::Ready(Err(error));
                }
            } else {
                me.send_buffer.clear();
                break me.inner.poll_flush(cx);
            }
        }
        .map_err(map_io_error);

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_shutdown(cx).map_err(map_io_error);

        #[cfg(feature = "logging")]
        me.logger.log_close(&result);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();

        #[cfg(feature = "logging")]
        let mut message_length = None;

        let result = loop {
            #[cfg(not(feature = "logging"))]
            let result = me.codec.decode(&me.receive_buffer[..]);

            #[cfg(feature = "logging")]
            let result = me
                .codec
                .decode_with_message_length(&me.receive_buffer[..])
                .map(|(message, length)| {
                    message_length = Some(length);
                    message
                });

            me.receive_buffer.clear();
            match result {
                Ok(message) => {
                    break Poll::Ready(Some(Ok(message)));
                }
                Err(error) => {
                    if !error.not_enough_data() {
                        break Poll::Ready(Some(Err(Error::DeserializationError(error))));
                    }
                }
            }

            match ready!(poll_read_buf(me.inner.as_mut(), cx, &mut me.receive_buffer)) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        *me.terminated = true;
                        break Poll::Ready(None);
                    }
                }
                Err(error) => {
                    break Poll::Ready(if is_closed(&error) {
                        None
                    } else {
                        Some(Err(Error::IoError(error)))
                    })
                }
            }
        };

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, message_length);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for FramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "Framed";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        f(&self.logger)
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        f(&mut self.logger)
    }
}

fn map_io_error<SerializationError, DeserializationError>(
    error: io::Error,
) -> crate::Error<Error<SerializationError, DeserializationError>> {
    if is_closed(&error) {
        crate::Error::Closed
    } else {
        crate::Error::Other(Error::IoError(error))
    }
}

fn is_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::NotConnected
    )
}
