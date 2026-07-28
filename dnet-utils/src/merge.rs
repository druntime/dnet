//! Merge [Sink] and [Stream] into a `dnet` transport.

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    stream::{Fuse, FusedStream},
    Sink, Stream, StreamExt,
};
use pin_project::pin_project;

/// Transport created from merging provided sender and receiver.
#[pin_project]
pub struct MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    #[pin]
    receiver: Fuse<Receiver>,
    #[pin]
    sender: Sender,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<Receiver, Sender, Incoming, Outgoing, Error>
    MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    /// Create new transport wrapping provided sender and receiver.
    pub fn new(sender: Sender, receiver: Receiver) -> Self {
        MergedTransport {
            receiver: receiver.fuse(),
            sender,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

impl<Receiver, Sender, Incoming, Outgoing, Error> Sink<Outgoing>
    for MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    type Error = dnet_base::Error<Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.sender.poll_ready(cx);

        #[cfg(feature = "logging")]
        me.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        let result = me.sender.start_send(item);

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
            Err(error) => me.logger.log_sending_failure(error),
        }

        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.sender.poll_flush(cx);

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.sender.poll_close(cx);

        #[cfg(feature = "logging")]
        me.logger.log_close(&result);

        result
    }
}

impl<Receiver, Sender, Incoming, Outgoing, Error> Stream
    for MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    type Item = Result<Incoming, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let result = me.receiver.poll_next(cx);

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<Receiver, Sender, Incoming, Outgoing, Error> FusedStream
    for MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.receiver.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Receiver, Sender, Incoming, Outgoing, Error> dnet_base::Logging
    for MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    const KIND: &'static str = "Merged";

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

/// Merge provided sender and receiver into a single transport.
pub fn merge<Receiver, Sender, Incoming, Outgoing, Error>(
    sender: Sender,
    receiver: Receiver,
) -> MergedTransport<Receiver, Sender, Incoming, Outgoing, Error>
where
    Receiver: Stream<Item = Result<Incoming, Error>>,
    Sender: Sink<Outgoing, Error = dnet_base::Error<Error>>,
    Error: std::error::Error,
{
    MergedTransport::new(sender, receiver)
}
