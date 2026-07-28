//! [Framed](framed) transport for communication over [Tokio](https://tokio.rs/)
//! TCP implementation.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "logging")]
use dnet_base::Logging;
use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

use crate::io::{
    framed::{self, Framing, NotEnoughData},
    Buffered, FramedTransport,
};

/// Framed TCP transport error.
pub type Error<Codec> = framed::Error<<Codec as Encode>::Error, <Codec as Decode>::Error>;

/// Framed transport for communication over
/// [Tokio](https://tokio.rs/)'s TCP implementation.
///
/// See also: [FramedTransport].
#[pin_project]
pub struct TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: FramedTransport<T, Codec, Incoming, Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided TCP stream.
    pub fn new(tcp_stream: T, codec: Codec) -> Self {
        #[allow(unused_mut)]
        let mut inner = FramedTransport::new(tcp_stream, codec);

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| logger.override_kind::<Self>());

        TcpFramedTransport { inner }
    }
}

impl<T, Codec, Incoming, Outgoing> TcpFramedTransport<Buffered<T>, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered transport wrapping a provided TCP stream.
    pub fn buffered(tcp_stream: T, codec: Codec) -> Self {
        #[allow(unused_mut)]
        let mut transport = Self::new(BufReader::new(BufWriter::new(tcp_stream)), codec);

        #[cfg(feature = "logging")]
        transport.with_logger_mut(|logger| logger.override_kind_buffered::<Self>());

        transport
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<Codec>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<Codec>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for TcpFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "TCP";

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
