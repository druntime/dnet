//! Transport for communication over
//! [futures](https://github.com/rust-lang/futures-rs) channels.
//!
//! Useful for testing and debugging.
//!
//! ## Example
//!
//! ```ignore
//! let (mut left, mut right) = transports();
//!
//! left.send("Hello World!").await.unwrap();
//! right.send(123).await.unwrap();
//!
//! use dnet::Receive;
//! assert_eq!(right.receive().await.unwrap(), "Hello World!");
//! assert_eq!(left.receive().await.unwrap(), 123);
//! ```

use std::{
    fmt::Display,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    channel::mpsc::{unbounded, SendError, UnboundedReceiver, UnboundedSender},
    sink::SinkMapErr,
    stream::{FusedStream, Map},
    Sink, SinkExt, Stream, StreamExt,
};
use pin_project::pin_project;

use crate::merge::{merge, MergedTransport};

/// Channel transport error.
#[derive(Debug)]
pub enum Error {
    /// Channel is full.
    ChannelIsFull,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel is full")
    }
}

impl std::error::Error for Error {}

type Receiver<T> = Map<UnboundedReceiver<T>, fn(T) -> Result<T, Error>>;
type Sender<T> = SinkMapErr<UnboundedSender<T>, fn(SendError) -> dnet_base::Error<Error>>;

/// Channel transport.
#[pin_project]
pub struct ChannelTransport<Incoming, Outgoing> {
    #[pin]
    inner: MergedTransport<Receiver<Incoming>, Sender<Outgoing>, Incoming, Outgoing, Error>,
}

impl<Incoming, Outgoing> Sink<Outgoing> for ChannelTransport<Incoming, Outgoing> {
    type Error = dnet_base::Error<Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        me.inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        me.inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        me.inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        me.inner.poll_close(cx)
    }
}

impl<Incoming, Outgoing> Stream for ChannelTransport<Incoming, Outgoing> {
    type Item = Result<Incoming, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        me.inner.poll_next(cx)
    }
}

impl<Incoming, Outgoing> FusedStream for ChannelTransport<Incoming, Outgoing> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<Incoming, Outgoing> From<(UnboundedSender<Outgoing>, UnboundedReceiver<Incoming>)>
    for ChannelTransport<Incoming, Outgoing>
{
    fn from(pair: (UnboundedSender<Outgoing>, UnboundedReceiver<Incoming>)) -> Self {
        let sender = pair
            .0
            .sink_map_err(map_error as fn(SendError) -> dnet_base::Error<Error>);
        let receiver = pair.1.map(map as fn(Incoming) -> Result<Incoming, Error>);

        #[allow(unused_mut)]
        let mut inner = merge(sender, receiver);
        #[cfg(feature = "logging")]
        {
            use dnet_base::logging::Logging;
            inner.with_logger_mut(|logger| logger.override_kind::<Self>())
        }

        ChannelTransport { inner }
    }
}

#[cfg(feature = "logging")]
impl<Incoming, Outgoing> dnet_base::Logging for ChannelTransport<Incoming, Outgoing> {
    const KIND: &'static str = "Channel";

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

/// Create two connected transports over two unbounded channels.
#[allow(clippy::type_complexity)]
pub fn transports<A, B>() -> (ChannelTransport<A, B>, ChannelTransport<B, A>) {
    let (left_sender, right_receiver) = unbounded();
    let (right_sender, left_receiver) = unbounded();

    let left = (left_sender, left_receiver).into();
    let right = (right_sender, right_receiver).into();

    (left, right)
}

fn map<T>(value: T) -> Result<T, Error> {
    Ok(value)
}

fn map_error(error: SendError) -> dnet_base::Error<Error> {
    if error.is_full() {
        dnet_base::Error::Other(Error::ChannelIsFull)
    } else {
        dnet_base::Error::Closed
    }
}

#[cfg(test)]
mod tests {
    use dnet_tests::{dtest, dtest_configure};

    use super::transports;

    dtest_configure!();

    #[dtest]
    async fn test_transport() {
        let (left, right) = transports();
        dnet_tests::test_transport(left, right).await;
    }

    #[dtest]
    async fn test_unit_message() {
        let (left, right) = transports();
        dnet_tests::test_unit_message(left, right).await;
    }

    #[dtest]
    async fn test_stream() {
        let (left, right) = transports();
        dnet_tests::test_stream(left, right).await;
    }
}
