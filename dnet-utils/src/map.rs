//! Mapping transports into other message types.

#![allow(clippy::type_complexity)]

use std::{
    convert::identity,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;

/// Trait for mapping input into output.
pub trait Mapper<Item> {
    /// Output type.
    type Output;

    /// Map item into output.
    fn map(&mut self, item: Item) -> Self::Output;
}

impl<T, I, O> Mapper<I> for T
where
    T: FnMut(I) -> O,
{
    type Output = O;

    fn map(&mut self, item: I) -> Self::Output {
        (self)(item)
    }
}

/// Trait allowing transports to be mapped into other message types.
pub trait Map<Incoming, Outgoing, Error>:
    dnet_base::Transport<Incoming, Outgoing, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Map outgoing massages.
    fn map<O, Mapper>(
        self,
        mapper: Mapper,
    ) -> Mapping<Self, Incoming, O, fn(Incoming) -> Incoming, Mapper, Error>
    where
        Mapper: self::Mapper<O, Output = Outgoing>,
    {
        self.map_and_unmap(mapper, identity)
    }

    /// Map incoming messages.
    fn unmap<I, Unmapper>(
        self,
        unmapper: Unmapper,
    ) -> Mapping<Self, Incoming, Outgoing, Unmapper, fn(Outgoing) -> Outgoing, Error>
    where
        Unmapper: self::Mapper<Incoming, Output = I>,
    {
        self.map_and_unmap(identity, unmapper)
    }

    /// Map outgoing messages and unmap incoming messages.
    fn map_and_unmap<I, O, Unmapper, Mapper>(
        self,
        mapper: Mapper,
        unmapper: Unmapper,
    ) -> Mapping<Self, Incoming, O, Unmapper, Mapper, Error>
    where
        Mapper: self::Mapper<O, Output = Outgoing>,
        Unmapper: self::Mapper<Incoming, Output = I>,
    {
        Mapping {
            inner: self,
            mapper,
            unmapper,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
            _error: PhantomData,
        }
    }
}

impl<T, Incoming, Outgoing, Error> Map<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Wrapper transport mapping outgoing and/or incoming messages into other message types.
#[pin_project]
pub struct Mapping<Transport, Incoming, Outgoing, Unmapper, Mapper, Error>
where
    Transport: dnet_base::Transport<Incoming, Mapper::Output, Error> + Unpin,
    Mapper: self::Mapper<Outgoing>,
    Unmapper: self::Mapper<Incoming>,
{
    inner: Transport,
    mapper: Mapper,
    unmapper: Unmapper,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
    _error: PhantomData<Error>,
}

impl<Transport, Incoming, Outgoing, Unmapper, Mapper, Error> Sink<Outgoing>
    for Mapping<Transport, Incoming, Outgoing, Unmapper, Mapper, Error>
where
    Transport: dnet_base::Transport<Incoming, Mapper::Output, Error> + Unpin,
    Mapper: self::Mapper<Outgoing>,
    Unmapper: self::Mapper<Incoming>,
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
        let item = me.mapper.map(item);
        let result = me.inner.start_send_unpin(item);

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
            Err(error) => me.logger.log_sending_failure(error),
        }

        result
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

impl<Transport, Incoming, Outgoing, Unmapper, Mapper, Error> Stream
    for Mapping<Transport, Incoming, Outgoing, Unmapper, Mapper, Error>
where
    Transport: dnet_base::Transport<Incoming, Mapper::Output, Error> + Unpin,
    Mapper: self::Mapper<Outgoing>,
    Unmapper: self::Mapper<Incoming>,
    Error: std::error::Error,
{
    type Item = Result<Unmapper::Output, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let result = me
            .inner
            .poll_next_unpin(cx)
            .map_ok(|item| me.unmapper.map(item));

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<Transport, Incoming, Outgoing, Unmapper, Mapper, Error> FusedStream
    for Mapping<Transport, Incoming, Outgoing, Unmapper, Mapper, Error>
where
    Transport: dnet_base::Transport<Incoming, Mapper::Output, Error> + FusedStream + Unpin,
    Mapper: self::Mapper<Outgoing>,
    Unmapper: self::Mapper<Incoming>,
    Error: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Transport, Incoming, Outgoing, Unmapper, Mapper, Error> dnet_base::Logging
    for Mapping<Transport, Incoming, Outgoing, Unmapper, Mapper, Error>
where
    Transport: dnet_base::Transport<Incoming, Mapper::Output, Error> + dnet_base::Logging + Unpin,
    Mapper: self::Mapper<Outgoing>,
    Unmapper: self::Mapper<Incoming>,
    Error: std::error::Error,
{
    const KIND: &'static str = "Map";

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

    use crate::channel::transports;

    dtest_configure!();

    use super::Map;
    #[derive(Debug, PartialEq, Eq)]
    struct Wrapper<T>(pub T);

    impl<T> Wrapper<T> {
        fn unwrap(self) -> T {
            self.0
        }
    }

    #[dtest]
    async fn test_map() {
        let (left, right) = transports();
        let mut left = left.map(Wrapper);
        let mut right = right.map_and_unmap(Wrapper, Wrapper::unwrap);

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(30).await.unwrap();
        right.send("Hello".to_string()).await.unwrap();

        assert_eq!(left.receive().await.unwrap(), Wrapper("Hello".to_string()));
        assert_eq!(right.receive().await.unwrap(), 30);
    }

    #[dtest]
    async fn test_map_and_unmap() {
        let (left, right) = transports();
        let left = left.map_and_unmap(Wrapper, Wrapper::unwrap);
        let right = right.map_and_unmap(Wrapper, Wrapper::unwrap);
        dnet_tests::test_transport(left, right).await;
    }

    #[dtest]
    async fn test_map_and_unmap_unit_message() {
        let (left, right) = transports();
        let left = left.map_and_unmap(Wrapper, Wrapper::unwrap);
        let right = right.map_and_unmap(Wrapper, Wrapper::unwrap);
        dnet_tests::test_unit_message(left, right).await;
    }

    #[dtest]
    async fn test_map_and_unmap_stream() {
        let (left, right) = transports();
        let left = left.map_and_unmap(Wrapper, Wrapper::unwrap);
        let right = right.map_and_unmap(Wrapper, Wrapper::unwrap);
        dnet_tests::test_stream(left, right).await;
    }
}
