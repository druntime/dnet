#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! `dnet` base features.

#[cfg(feature = "logging")]
pub mod logging;
#[cfg(feature = "logging")]
pub use logging::{Logger, Logging};

use std::{
    fmt::Display,
    io::{Read, Write},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    future::FusedFuture,
    ready,
    stream::{FusedStream, Next},
    Future, FutureExt, Sink, Stream, StreamExt,
};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

/// Trait for encoders.
pub trait Encode {
    /// Error type.
    type Error: std::error::Error;

    /// Encode message into writer.
    fn encode<W, T>(&mut self, writer: W, message: &T) -> Result<(), Self::Error>
    where
        W: Write,
        T: Serialize;
}

/// Trait for decoders.
pub trait Decode {
    /// Error type.
    type Error: std::error::Error;

    /// Decode message from reader.
    fn decode<R, T>(&mut self, data: R) -> Result<T, Self::Error>
    where
        R: Read,
        for<'de> T: Deserialize<'de>;
}

/// Trait for `dnet` codecs.
pub trait Codec: Encode + Decode {}

impl<T> Codec for T where T: Encode + Decode {}

/// Transport error.
#[derive(Debug, PartialEq, Eq)]
pub enum Error<Other> {
    /// Occurs when transport is closed.
    Closed,

    /// Other non-predefined transport-specific error.
    Other(Other),
}

impl<Other> Error<Other> {
    /// Was error caused by transport being closed.
    pub fn closed(&self) -> bool {
        matches!(self, Error::Closed)
    }
}

impl<Other> Display for Error<Other>
where
    Other: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "transport closed"),
            Self::Other(other) => write!(f, "{other}"),
        }
    }
}

impl<Other> std::error::Error for Error<Other> where Other: std::error::Error {}

/// Convenience trait for receiving messages.
pub trait Receive<Message, Error> {
    /// Receive message from transport.
    fn receive(&mut self) -> Recv<'_, Self>;
}

impl<T, Message, Error> Receive<Message, Error> for T
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
{
    /// Receive message from transport.
    fn receive(&mut self) -> Recv<'_, Self> {
        let next = self.next();
        Recv {
            next,
            terminated: false,
        }
    }
}

/// Future returned by [receive] method.
///
/// [receive]: self::Receive::receive
pub struct Recv<'a, T>
where
    T: ?Sized,
{
    next: Next<'a, T>,
    terminated: bool,
}

impl<T, Message, Error> Future for Recv<'_, T>
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
{
    type Output = Result<Message, self::Error<Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.next.poll_unpin(cx) {
            Poll::Ready(item) => {
                self.terminated = true;
                if let Some(item) = item {
                    Poll::Ready(item.map_err(|error| self::Error::Other(error)))
                } else {
                    Poll::Ready(Err(self::Error::Closed))
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, Message, Error> FusedFuture for Recv<'_, T>
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

/// Utility trait for creating message stream with filtered-out errors.
pub trait Messages<T, Message, Error>
where
    Self: Sized,
{
    /// Message stream with filtered-out errors.
    ///
    /// Calls a callback when error occurs.
    fn messages_with_error_callback<F>(self, error_callback: F) -> MessageStream<Self, F>
    where
        F: FnMut(Error);

    /// Message stream with filtered-out errors.
    fn messages(self) -> MessageStream<Self, fn(Error) -> ()> {
        self.messages_with_error_callback(|_| {})
    }
}

impl<T, Message, Error> Messages<T, Message, Error> for T
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
{
    fn messages_with_error_callback<F>(self, error_callback: F) -> MessageStream<Self, F>
    where
        F: FnMut(Error),
    {
        MessageStream {
            stream: self,
            error_callback,
            terminated: false,
        }
    }
}

/// Stream of messages.
///
/// Returned by [messages] function.
///
/// [messages]: self::Messages::messages
#[pin_project]
pub struct MessageStream<T, F> {
    #[pin]
    stream: T,
    error_callback: F,
    terminated: bool,
}

impl<T, F, Message, Error> Stream for MessageStream<T, F>
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
    F: FnMut(Error),
{
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(result) = ready!(self.stream.poll_next_unpin(cx)) {
                match result {
                    Ok(message) => return Poll::Ready(Some(message)),
                    Err(error) => {
                        (self.error_callback)(error);
                        continue;
                    }
                }
            } else {
                self.terminated = true;
                return Poll::Ready(None);
            }
        }
    }
}

impl<T, F, Message, Error> FusedStream for MessageStream<T, F>
where
    T: Stream<Item = Result<Message, Error>> + Unpin,
    F: FnMut(Error),
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[doc(hidden)]
/// Logging trait variant that extents `Logging` only when `logging` feature is enabled. 
pub mod conditional {
    #[cfg(feature = "logging")]
    /// Logging trait variant that extents `Logging` only when `logging` feature is enabled.
    pub trait Logging: crate::logging::Logging {}

    #[cfg(feature = "logging")]
    impl<T> Logging for T where T: crate::logging::Logging {}

    #[cfg(not(feature = "logging"))]
    /// Logging trait variant that extents `Logging` only when `logging` feature is enabled.
    pub trait Logging {}

    #[cfg(not(feature = "logging"))]
    impl<T> Logging for T {}
}

/// Trait for transports implementing `dnet` interface.
pub trait Transport<Incoming, Outgoing, Error>:
    Sink<Outgoing, Error = crate::Error<Error>>
    + Stream<Item = Result<Incoming, Error>>
    + conditional::Logging
{
}

impl<T, Incoming, Outgoing, Error> Transport<Incoming, Outgoing, Error> for T where
    T: Sink<Outgoing, Error = crate::Error<Error>>
        + Stream<Item = Result<Incoming, Error>>
        + conditional::Logging
{
}

/// Helper trait for transports where incoming and outgoing messages are of the same type.
pub trait SymmetricTransport<Message, Error>: Transport<Message, Message, Error> {}

impl<T, Message, Error> SymmetricTransport<Message, Error> for T where
    T: Transport<Message, Message, Error>
{
}
