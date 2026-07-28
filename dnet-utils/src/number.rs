//! Wrapper transport attaching a number to messages.

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, Stream};
use num::{traits::bounds::UpperBounded, One, Zero};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

use super::unwrap::Unwrap;

/// Trait for adding message number to transport messages.
pub trait NumberMessages<N, Incoming, Outgoing, Error>:
    dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Numbered] transport adding message number of type `N`.
    fn number_messages(self) -> Numbered<N, Self, Error, Incoming, Outgoing>
    where
        N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    {
        Numbered::new(self)
    }
}

impl<T, N, Incoming, Outgoing, Error> NumberMessages<N, Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Trait for adding [usize] message number to transport messages.
///
/// **NOTE**: [usize] size may differ between platforms.
pub trait NumberMessagesUsize<Incoming, Outgoing, Error>:
    dnet_base::Transport<Wrapper<usize, Incoming>, Wrapper<usize, Outgoing>, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Numbered] transport adding message number of type [`usize`].
    fn number_messages_u64(self) -> Numbered<usize, Self, Error, Incoming, Outgoing> {
        Numbered::new(self)
    }
}

impl<T, Incoming, Outgoing, Error> NumberMessagesUsize<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Wrapper<usize, Incoming>, Wrapper<usize, Outgoing>, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Trait for adding [u32] message number to transport messages.
pub trait NumberMessagesU32<Incoming, Outgoing, Error>:
    dnet_base::Transport<Wrapper<u32, Incoming>, Wrapper<u32, Outgoing>, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Numbered] transport adding message number of type [`u32`].
    fn number_messages_u32(self) -> Numbered<u32, Self, Error, Incoming, Outgoing> {
        Numbered::new(self)
    }
}

impl<T, Incoming, Outgoing, Error> NumberMessagesU32<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Wrapper<u32, Incoming>, Wrapper<u32, Outgoing>, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Trait for adding [u64] message number to transport messages.
pub trait NumberMessagesU64<Incoming, Outgoing, Error>:
    dnet_base::Transport<Wrapper<u64, Incoming>, Wrapper<u64, Outgoing>, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Numbered] transport adding message number of type [`u64`].
    fn number_messages_u64(self) -> Numbered<u64, Self, Error, Incoming, Outgoing> {
        Numbered::new(self)
    }
}

impl<T, Incoming, Outgoing, Error> NumberMessagesU64<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Wrapper<u64, Incoming>, Wrapper<u64, Outgoing>, Error> + Unpin,
    Error: std::error::Error,
{
}

/// Trait for adding [u128] message number to transport messages.
pub trait NumberMessagesU128<Incoming, Outgoing, Error>:
    dnet_base::Transport<Wrapper<u128, Incoming>, Wrapper<u128, Outgoing>, Error> + Sized + Unpin
where
    Error: std::error::Error,
{
    /// Wrap transport with [Numbered] transport adding message number of type [`u128`].
    fn number_messages_u128(self) -> Numbered<u128, Self, Error, Incoming, Outgoing> {
        Numbered::new(self)
    }
}

impl<T, Incoming, Outgoing, Error> NumberMessagesU128<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Wrapper<u128, Incoming>, Wrapper<u128, Outgoing>, Error> + Unpin,
    Error: std::error::Error,
{
}

/// [Numbered] transport error.
#[derive(Debug)]
pub enum Error<T> {
    /// Maximum message number reached.
    MaximumNumberReached,

    /// Wrapped transport error.
    Transport(T),
}

impl<T> Display for Error<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MaximumNumberReached => write!(f, "maximum number reached"),
            Error::Transport(error) => write!(f, "transport error: {error}"),
        }
    }
}

impl<T> std::error::Error for Error<T> where T: Debug + Display {}

/// Trait implemented by messages with attached number.
pub trait Number {
    /// Message number type.
    type Output;

    /// Message number.
    fn number(&self) -> Self::Output;
}

/// Message wrapper used by [`Numbered`] transport.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wrapper<N, T> {
    /// Message number.
    pub number: N,

    /// Wrapped message.
    pub wrapped: T,
}

impl<N, T> Unwrap for Wrapper<N, T> {
    type Output = T;

    fn unwrap(self) -> Self::Output {
        self.wrapped
    }
}

impl<N, T> Number for Wrapper<N, T>
where
    N: Clone,
{
    type Output = N;

    fn number(&self) -> Self::Output {
        self.number.clone()
    }
}

/// Wrapper transport attaching number to sent messages.
///
/// Received messages are of [`Wrapper`] type.
///
/// First message number is [zero].<br>
/// Next message number = previous message number + [one].
///
/// Sending will result in an [Error::MaximumNumberReached] error when reaching maximum value.
///
/// [zero]: num::Zero
/// [one]: num::One
#[pin_project]
pub struct Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E>,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
{
    #[pin]
    inner: T,
    current_number: N,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _error: PhantomData<E>,
    _number: PhantomData<N>,
    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<N, T, E, Incoming, Outgoing> Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E>,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    E: std::error::Error,
{
    /// Create new [numbered] transport wrapping provided transport.
    ///
    /// [numbered]: self::Number
    pub fn new(transport: T) -> Self {
        Numbered {
            inner: transport,
            current_number: N::zero(),

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _error: PhantomData,
            _number: PhantomData,
            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }

    /// Number that will be attached to the next sent message.
    pub fn current_number(&self) -> N {
        self.current_number.clone()
    }
}

impl<T, E, Incoming, Outgoing> Numbered<usize, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<usize, Incoming>, Wrapper<usize, Outgoing>, E>,
    E: std::error::Error,
{
    /// Create new [`Numbered`] transport using [`usize`] type as  message number.
    pub fn new_usize(transport: T) -> Self {
        Numbered::new(transport)
    }
}

impl<T, E, Incoming, Outgoing> Numbered<u32, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<u32, Incoming>, Wrapper<u32, Outgoing>, E>,
    E: std::error::Error,
{
    /// Create new [`Numbered`] transport using [`u32`] type as  message number.
    pub fn new_u32(transport: T) -> Self {
        Numbered::new(transport)
    }
}

impl<T, E, Incoming, Outgoing> Numbered<u64, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<u64, Incoming>, Wrapper<u64, Outgoing>, E>,
    E: std::error::Error,
{
    /// Create new [`Numbered`] transport using [`u64`] type as  message number.
    pub fn new_u64(transport: T) -> Self {
        Numbered::new(transport)
    }
}

impl<T, E, Incoming, Outgoing> Numbered<u128, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<u128, Incoming>, Wrapper<u128, Outgoing>, E>,
    E: std::error::Error,
{
    /// Create new [`Numbered`] transport using [`u128`] type as  message number.
    pub fn new_u128(transport: T) -> Self {
        Numbered::new(transport)
    }
}

impl<N, T, E, Incoming, Outgoing> Sink<Outgoing> for Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E>,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    E: std::error::Error,
{
    type Error = dnet_base::Error<Error<E>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_ready(cx).map_err(map_error);

        #[cfg(feature = "logging")]
        me.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        let result = if *me.current_number == N::max_value() {
            Err(dnet_base::Error::Other(Error::MaximumNumberReached))
        } else {
            let item = Wrapper {
                number: me.current_number.clone(),
                wrapped: item,
            };
            let result = me.inner.start_send(item);
            if result.is_ok() {
                *me.current_number = me.current_number.clone().add(One::one());
            }
            result.map_err(map_error)
        };

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
            Err(error) => me.logger.log_sending_failure(error),
        }

        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_flush(cx).map_err(map_error);

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = me.inner.poll_close(cx).map_err(map_error);

        #[cfg(feature = "logging")]
        me.logger.log_close(&result);

        result
    }
}

impl<N, T, E, Incoming, Outgoing> Stream for Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E>,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    E: std::error::Error,
{
    type Item = Result<Wrapper<N, Incoming>, Error<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let result = me.inner.poll_next(cx).map_err(Error::Transport);

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<N, T, E, Incoming, Outgoing> FusedStream for Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E> + FusedStream,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    E: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<N, T, E, Incoming, Outgoing> dnet_base::Logging for Numbered<N, T, E, Incoming, Outgoing>
where
    T: dnet_base::Transport<Wrapper<N, Incoming>, Wrapper<N, Outgoing>, E> + dnet_base::Logging,
    N: Clone + Zero + One + UpperBounded + PartialEq + Eq,
    E: std::error::Error,
{
    const KIND: &'static str = "Numbered";

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

fn map_error<T>(error: dnet_base::Error<T>) -> dnet_base::Error<Error<T>> {
    match error {
        dnet_base::Error::Closed => dnet_base::Error::Closed,
        dnet_base::Error::Other(error) => dnet_base::Error::Other(Error::Transport(error)),
    }
}

#[cfg(test)]
mod tests {
    use dnet_base::Receive;
    use dnet_tests::{dtest, dtest_configure};
    use futures::SinkExt;

    use crate::{
        channel::transports,
        number::{Numbered, Wrapper},
    };

    dtest_configure!();

    #[dtest]
    async fn test_transport() {
        let (left, right) = transports();

        let mut left = Numbered::new_usize(left);
        let mut right = Numbered::new_usize(right);

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(1).await.unwrap();
        left.send(2).await.unwrap();
        left.send(3).await.unwrap();

        assert_eq!(
            right.receive().await.unwrap(),
            Wrapper {
                number: 0,
                wrapped: 1,
            }
        );
        assert_eq!(
            right.receive().await.unwrap(),
            Wrapper {
                number: 1,
                wrapped: 2,
            }
        );
        assert_eq!(
            right.receive().await.unwrap(),
            Wrapper {
                number: 2,
                wrapped: 3,
            }
        );

        right.send(1).await.unwrap();
        right.send(2).await.unwrap();
        right.send(3).await.unwrap();

        assert_eq!(
            left.receive().await.unwrap(),
            Wrapper {
                number: 0,
                wrapped: 1,
            }
        );
        assert_eq!(
            left.receive().await.unwrap(),
            Wrapper {
                number: 1,
                wrapped: 2,
            }
        );
        assert_eq!(
            left.receive().await.unwrap(),
            Wrapper {
                number: 2,
                wrapped: 3,
            }
        );
    }
}
