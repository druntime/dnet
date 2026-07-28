//! [Framed](framed) transport implementation for QUIC.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use dnet_base::{Decode, Encode, Receive};
use futures::{stream::FusedStream, FutureExt, Sink, SinkExt, Stream};
use pin_project::{pin_project, pinned_drop};
use quinn::{RecvStream, SendStream};
use serde::Serialize;
use tokio::{
    io::{join, sink, AsyncRead, AsyncWrite},
    spawn,
};

use crate::{
    io::{
        framed::{self, Framing, NotEnoughData},
        Buffered, FramedTransport, Pending,
    },
    quic::{Bidirectional, UnidirectionalReceive, UnidirectionalSend, Wrapper},
};

/// Framed QUIC transport error.
pub type Error<Codec> = framed::Error<<Codec as Encode>::Error, <Codec as Decode>::Error>;

/// [Framed](framed) QUIC transport.
#[pin_project(PinnedDrop)]
pub struct QuicFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: FramedTransport<T, Codec, Wrapper<Incoming>, Wrapper<Outgoing>>,
    stopper: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,
}

impl<Codec, Incoming, Outgoing> QuicFramedTransport<Bidirectional, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new [framed] QUIC transport wrapping a provided [RecvStream] and [SendStream].
    pub async fn new(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let stopper = Some(stopper(&send_stream));
        let mut inner = FramedTransport::new(join(recv_stream, send_stream), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        send_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;
        wait_for_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        #[cfg(feature = "logging")]
        logger.log_open_success();

        Ok(QuicFramedTransport {
            inner,
            stopper,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<Codec, Incoming, Outgoing> QuicFramedTransport<UnidirectionalSend, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new [unidirectional](UnidirectionalSend) [framed] QUIC transport wrapping a provided [SendStream].
    pub async fn unidirectional_send(
        send_stream: SendStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let stopper = Some(stopper(&send_stream));
        let mut inner = FramedTransport::new(join(Pending, send_stream), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        send_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        Ok(QuicFramedTransport {
            inner,
            stopper,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicFramedTransport<UnidirectionalReceive, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new [unidirectional](UnidirectionalReceive) [framed] QUIC transport wrapping a provided [RecvStream].
    pub async fn unidirectional_receive(
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let mut inner = FramedTransport::new(join(recv_stream, sink()), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        wait_for_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        #[cfg(feature = "logging")]
        logger.log_open_success();

        Ok(QuicFramedTransport {
            inner,
            stopper: None,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicFramedTransport<Buffered<Bidirectional>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered [framed] QUIC transport wrapping a provided [RecvStream] and [SendStream].
    pub async fn buffered(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let stopper = Some(stopper(&send_stream));
        let mut inner = FramedTransport::buffered(join(recv_stream, send_stream), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        send_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;
        wait_for_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        #[cfg(feature = "logging")]
        logger.log_open_success();

        Ok(QuicFramedTransport {
            inner,
            stopper,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicFramedTransport<Buffered<UnidirectionalSend>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered [unidirectional](UnidirectionalSend) [framed] QUIC transport wrapping a provided [SendStream].
    pub async fn buffered_unidirectional_send(
        send_stream: SendStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let stopper = Some(stopper(&send_stream));
        let mut inner = FramedTransport::buffered(join(Pending, send_stream), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        send_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        Ok(QuicFramedTransport {
            inner,
            stopper,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicFramedTransport<Buffered<UnidirectionalReceive>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered [unidirectional](UnidirectionalReceive) [framed] QUIC transport wrapping a provided [RecvStream].
    pub async fn buffered_unidirectional_receive(
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let mut inner = FramedTransport::buffered(join(recv_stream, sink()), codec);

        #[cfg(feature = "logging")]
        let logger = dnet_base::Logger::new::<Self>();

        wait_for_open_message::<_, Incoming, Outgoing, Codec>(&mut inner).await?;

        #[cfg(feature = "logging")]
        logger.log_open_success();

        Ok(QuicFramedTransport {
            inner,
            stopper: None,

            #[cfg(feature = "logging")]
            logger,
        })
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for QuicFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<Codec>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_ready(cx);

        #[cfg(feature = "logging")]
        me.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        let result = me.inner.start_send(Wrapper::Message(item));

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
            Err(error) => me.logger.log_sending_failure(error),
        }

        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_flush(cx);

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_close(cx);

        #[cfg(feature = "logging")]
        me.logger.log_close(&result);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for QuicFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<Codec>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        let result = loop {
            match ready!(me.inner.as_mut().poll_next(cx)) {
                Some(Ok(Wrapper::Open)) => continue,
                Some(Ok(Wrapper::Message(message))) => break Poll::Ready(Some(Ok(message))),
                Some(Err(error)) => break Poll::Ready(Some(Err(error))),
                None => break Poll::Ready(None),
            }
        };

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for QuicFramedTransport<T, Codec, Incoming, Outgoing>
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

#[pinned_drop]
impl<T, Codec, Incoming, Outgoing> PinnedDrop for QuicFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn drop(self: Pin<&mut Self>) {
        let stopper = self.project().stopper.take();
        if let Some(stopper) = stopper {
            spawn(stopper);
        }
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for QuicFramedTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec + Framing,
    <Codec as Decode>::Error: NotEnoughData,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "QUIC";

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

fn stopper(send_stream: &SendStream) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(send_stream.stopped().map(|_| ()))
        as Pin<Box<dyn Future<Output = ()> + Send + 'static>>
}

async fn send_open_message<T, Incoming, Outgoing, Codec>(
    inner: &mut T,
) -> Result<(), crate::Error<Error<Codec>>>
where
    Codec: crate::Codec,
    T: crate::Transport<Wrapper<Incoming>, Wrapper<Outgoing>, Error<Codec>> + Unpin,
{
    inner.send(Wrapper::Open).await?;
    Ok(())
}

async fn wait_for_open_message<T, Incoming, Outgoing, Codec>(
    inner: &mut T,
) -> Result<(), crate::Error<Error<Codec>>>
where
    Codec: crate::Codec,
    T: crate::Transport<Wrapper<Incoming>, Wrapper<Outgoing>, Error<Codec>> + Unpin,
{
    let open_message = inner.receive().await?;
    if matches!(open_message, Wrapper::Open) {
        Ok(())
    } else {
        let error = io::Error::new(io::ErrorKind::InvalidData, "expected open message");
        Err(crate::Error::Other(Error::<Codec>::IoError(error)))
    }
}
