//! Wrapper transport turning a [numbered] transport into a transport discarding
//! old messages.
//!
//! Polling a transport for the next message will return the latest
//! received message, ignoring messages received before.
//!
//! Potentially useful when user doesn't care about stale messages
//! (for example: multiplayer video games).
//!
//! [numbered]: super::number::Number

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, Stream, StreamExt};
use pin_project::pin_project;

use super::number::Number;

/// Trait for requesting only the most recent messages.
pub trait OnlyLatest<N, Incoming, Outgoing, Error>:
    dnet_base::Transport<Incoming, Outgoing, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Latest] transport turning it into transport returning latest
    /// message when polling it for the next received message.
    fn only_latest(self) -> Latest<Self, Error, N, Incoming, Outgoing>
    where
        Incoming: Number<Output = N>,
        for<'a> &'a N: PartialOrd,
    {
        Latest::new(self)
    }
}

impl<T, N, Incoming, Outgoing, Error> OnlyLatest<N, Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Wrapper transport turning a [numbered] transport into a transport discarding
/// old messages (polling a transport for the next message will return the latest
/// received message, ignoring messages received before).
///
/// [numbered]: super::number::Number
#[pin_project]
pub struct Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E>,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    #[pin]
    inner: T,
    last_number: Option<N>,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _error: PhantomData<E>,
    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<T, E, N, Incoming, Outgoing> Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E>,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    /// Wrap a provided [numbered] transport turning it into ordered transport returning latest
    /// message when polling it for the next received message.
    ///
    /// [numbered]: super::number::Number
    pub fn new(transport: T) -> Self {
        Latest {
            inner: transport,
            last_number: None,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _error: PhantomData,
            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

impl<T, E, N, Incoming, Outgoing> Sink<Outgoing> for Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E>,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    type Error = dnet_base::Error<E>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_ready(cx);

        #[cfg(feature = "logging")]
        me.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        let result = me.inner.start_send(item);

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

impl<T, E, N, Incoming, Outgoing> Stream for Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E>,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    type Item = Result<Incoming, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        let mut latest = None;
        let result = loop {
            match me.inner.poll_next_unpin(cx) {
                Poll::Ready(item) => {
                    if let Some(item) = item {
                        let item = item?;
                        let number = item.number();

                        if let Some(last_number) = me.last_number.as_ref() {
                            if &number > last_number {
                                latest = Some(item);
                                *me.last_number = Some(number);
                            }

                            #[cfg(feature = "logging")]
                            me.logger.log_incoming_filtered_out::<Incoming>();
                        } else {
                            latest = Some(item);
                            *me.last_number = Some(number);
                        }
                    } else {
                        break Poll::Ready(None);
                    }
                }
                Poll::Pending => {
                    break if let Some(latest) = latest {
                        Poll::Ready(Some(Ok(latest)))
                    } else {
                        Poll::Pending
                    }
                }
            }
        };

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<T, E, N, Incoming, Outgoing> FusedStream for Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E> + FusedStream,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, E, N, Incoming, Outgoing> dnet_base::Logging for Latest<T, E, N, Incoming, Outgoing>
where
    T: dnet_base::Transport<Incoming, Outgoing, E> + dnet_base::Logging,
    Incoming: Number<Output = N>,
    for<'a> &'a N: PartialOrd,
    E: std::error::Error,
{
    const KIND: &'static str = "Latest";

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

#[cfg(test)]
mod tests {
    use dnet_base::Receive;
    use dnet_tests::{dtest, dtest_configure};
    use futures::SinkExt;

    use crate::{
        channel::transports,
        latest::OnlyLatest,
        number::{NumberMessagesU128, NumberMessagesU32, Wrapper},
        unwrap::{Unwrap, Unwrapping},
    };

    dtest_configure!();

    #[dtest]
    async fn test_transport() {
        let (left, right) = transports();

        let mut left = left.number_messages_u32().only_latest().unwrapping();
        let mut right = right.number_messages_u32().only_latest().unwrapping();

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(1).await.unwrap();
        left.send(2).await.unwrap();
        left.send(3).await.unwrap();

        assert_eq!(right.receive().await.unwrap(), 3);

        right.send(1).await.unwrap();
        right.send(2).await.unwrap();
        right.send(3).await.unwrap();

        assert_eq!(left.receive().await.unwrap(), 3);
    }

    #[dtest]
    async fn test_order() {
        let (left, right) = transports();

        let mut left = left.only_latest();
        let mut right = right.number_messages_u128().only_latest();

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(Wrapper {
            number: 1,
            wrapped: 2,
        })
        .await
        .unwrap();
        left.send(Wrapper {
            number: 2,
            wrapped: 3,
        })
        .await
        .unwrap();
        left.send(Wrapper {
            number: 0,
            wrapped: 1,
        })
        .await
        .unwrap();

        right.send(1).await.unwrap();

        assert_eq!(right.receive().await.unwrap().unwrap(), 3);
        assert_eq!(left.receive().await.unwrap().unwrap(), 1);
    }
}
