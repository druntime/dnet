//! Filtering incoming and/or outgoing messages.

#![allow(clippy::type_complexity)]

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;

/// Trait for filtering messages.
pub trait Filter<Item> {
    /// Filter item.
    ///
    /// Return [`true`] to keep the item, [`false`] to filter it out.
    fn filter(&mut self, item: &Item) -> bool;
}

impl<T, I> Filter<I> for T
where
    T: FnMut(&I) -> bool,
{
    fn filter(&mut self, item: &I) -> bool {
        (self)(item)
    }
}

/// Trait for filtering incoming and/or outgoing messages.
pub trait FilterExt<Incoming, Outgoing, Error>:
    dnet_base::Transport<Incoming, Outgoing, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Filter outgoing massages.
    fn filter_outgoing<Filter>(
        self,
        filter: Filter,
    ) -> Filtering<Self, Incoming, Outgoing, fn(&Incoming) -> bool, Filter, Error>
    where
        Filter: self::Filter<Outgoing>,
    {
        self.filter_incoming_and_outgoing(|_| true, filter)
    }

    /// Filter incoming messages.
    fn filter_incoming<Filter>(
        self,
        filter: Filter,
    ) -> Filtering<Self, Incoming, Outgoing, Filter, fn(&Outgoing) -> bool, Error>
    where
        Filter: self::Filter<Incoming>,
    {
        self.filter_incoming_and_outgoing(filter, |_| true)
    }

    /// Filter incoming and outgoing massages.
    fn filter_incoming_and_outgoing<IncomingFilter, OutgoingFilter>(
        self,
        incoming_filter: IncomingFilter,
        outgoing_filter: OutgoingFilter,
    ) -> Filtering<Self, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
    where
        IncomingFilter: self::Filter<Incoming>,
        OutgoingFilter: self::Filter<Outgoing>,
    {
        Filtering {
            inner: self,
            incoming_filter,
            outgoing_filter,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<
                Filtering<Self, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>,
            >(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
            _error: PhantomData,
        }
    }
}

impl<T, Incoming, Outgoing, Error> FilterExt<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Wrapper transport mapping outgoing and/or incoming messages into other message types.
#[pin_project]
pub struct Filtering<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
where
    Transport: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    IncomingFilter: self::Filter<Incoming>,
    OutgoingFilter: self::Filter<Outgoing>,
    Error: std::error::Error,
{
    inner: Transport,
    incoming_filter: IncomingFilter,
    outgoing_filter: OutgoingFilter,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
    _error: PhantomData<Error>,
}

impl<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error> Sink<Outgoing>
    for Filtering<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
where
    Transport: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    IncomingFilter: self::Filter<Incoming>,
    OutgoingFilter: self::Filter<Outgoing>,
    Error: std::error::Error,
{
    type Error = dnet_base::Error<Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_ready_unpin(cx);

        #[cfg(feature = "logging")]
        me.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        if me.outgoing_filter.filter(&item) {
            let result = me.inner.start_send_unpin(item);

            #[cfg(feature = "logging")]
            match &result {
                Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
                Err(error) => me.logger.log_sending_failure(error),
            }

            result
        } else {
            #[cfg(feature = "logging")]
            me.logger.log_outgoing_filtered_out::<Outgoing>();

            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_flush_unpin(cx);

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_close_unpin(cx);

        #[cfg(feature = "logging")]
        me.logger.log_close(&result);

        result
    }
}

impl<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error> Stream
    for Filtering<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
where
    Transport: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    IncomingFilter: self::Filter<Incoming>,
    OutgoingFilter: self::Filter<Outgoing>,
    Error: std::error::Error,
{
    type Item = Result<Incoming, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let result = loop {
            let result = ready!(me.inner.poll_next_unpin(cx));
            match result {
                Some(Ok(item)) => {
                    if me.incoming_filter.filter(&item) {
                        break Poll::Ready(Some(Ok(item)));
                    } else {
                        #[cfg(feature = "logging")]
                        me.logger.log_incoming_filtered_out::<Incoming>();

                        continue;
                    }
                }
                Some(Err(error)) => break Poll::Ready(Some(Err(error))),
                None => break Poll::Ready(None),
            }
        };

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error> FusedStream
    for Filtering<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
where
    Transport: dnet_base::Transport<Incoming, Outgoing, Error> + FusedStream + Unpin,
    IncomingFilter: self::Filter<Incoming>,
    OutgoingFilter: self::Filter<Outgoing>,
    Error: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error> dnet_base::Logging
    for Filtering<Transport, Incoming, Outgoing, IncomingFilter, OutgoingFilter, Error>
where
    Transport: dnet_base::Transport<Incoming, Outgoing, Error> + dnet_base::Logging + Unpin,
    IncomingFilter: self::Filter<Incoming>,
    OutgoingFilter: self::Filter<Outgoing>,
    Error: std::error::Error,
{
    const KIND: &'static str = "Filtering";

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
    use dnet_base::Messages;
    use dnet_tests::{dtest, dtest_configure};
    use futures::{stream, SinkExt, StreamExt};

    use crate::channel::transports;

    use super::FilterExt;

    dtest_configure!();

    #[dtest]
    async fn test_filter() {
        let (mut left, right) = transports();
        let mut right = right.filter_incoming_and_outgoing(
            |integer: &u32| integer % 2 == 0,
            |string: &String| !string.starts_with("A"),
        );

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(1).await.unwrap();
        left.send(2).await.unwrap();
        left.send(3).await.unwrap();
        left.send(4).await.unwrap();
        left.send(5).await.unwrap();
        left.close().await.unwrap();

        right
            .send_all(&mut stream::iter(
                vec!["Anna", "Tom", "Albert", "Robert"]
                    .into_iter()
                    .map(String::from)
                    .map(Ok),
            ))
            .await
            .unwrap();
        right.close().await.unwrap();

        assert_eq!(
            vec!["Tom", "Robert"],
            left.messages().collect::<Vec<String>>().await
        );

        assert_eq!(vec![2, 4], right.messages().collect::<Vec<u32>>().await);
    }
}
