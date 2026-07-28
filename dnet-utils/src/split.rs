//! Splitting transport into multiple transports of different message types.

#![allow(clippy::type_complexity)]

use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::{stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};

use super::map::Mapper;

/// Trait allowing transports to be split into two of different message types.
pub trait Split2<I1, O1, I2, O2, E>:
    dnet_base::Transport<Message<I1, I2, (), (), ()>, Message<O1, O2, (), (), ()>, E> + Sized + Unpin
where
    E: std::error::Error,
{
    /// Split into two transports.
    fn split_into_2(
        self,
    ) -> (
        Part<
            I1,
            O1,
            fn(O1) -> Message<O1, O2, (), (), ()>,
            fn(Message<I1, I2, (), (), ()>) -> Result<I1, Message<I1, I2, (), (), ()>>,
            Self,
            E,
            I1,
            I2,
            (),
            (),
            (),
        >,
        Part<
            I2,
            O2,
            fn(O2) -> Message<O1, O2, (), (), ()>,
            fn(Message<I1, I2, (), (), ()>) -> Result<I2, Message<I1, I2, (), (), ()>>,
            Self,
            E,
            I1,
            I2,
            (),
            (),
            (),
        >,
    ) {
        let state = State::new(2, self);
        (
            Part::new(0, &state, Message::Variant1, Message::unwrap1),
            Part::new(1, &state, Message::Variant2, Message::unwrap2),
        )
    }
}

impl<T, I1, O1, I2, O2, E> Split2<I1, O1, I2, O2, E> for T
where
    T: dnet_base::Transport<Message<I1, I2, (), (), ()>, Message<O1, O2, (), (), ()>, E>
        + Sized
        + Unpin,
    E: std::error::Error,
{
}

/// Trait allowing transports to be split into three of different message types.
pub trait Split3<I1, O1, I2, O2, I3, O3, E>:
    dnet_base::Transport<Message<I1, I2, I3, (), ()>, Message<O1, O2, O3, (), ()>, E> + Sized + Unpin
where
    E: std::error::Error,
{
    /// Split into three transports.
    fn split_into_3(
        self,
    ) -> (
        Part<
            I1,
            O1,
            fn(O1) -> Message<O1, O2, O3, (), ()>,
            fn(Message<I1, I2, I3, (), ()>) -> Result<I1, Message<I1, I2, I3, (), ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            (),
            (),
        >,
        Part<
            I2,
            O2,
            fn(O2) -> Message<O1, O2, O3, (), ()>,
            fn(Message<I1, I2, I3, (), ()>) -> Result<I2, Message<I1, I2, I3, (), ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            (),
            (),
        >,
        Part<
            I3,
            O3,
            fn(O3) -> Message<O1, O2, O3, (), ()>,
            fn(Message<I1, I2, I3, (), ()>) -> Result<I3, Message<I1, I2, I3, (), ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            (),
            (),
        >,
    ) {
        let state = State::new(3, self);
        (
            Part::new(0, &state, Message::Variant1, Message::unwrap1),
            Part::new(1, &state, Message::Variant2, Message::unwrap2),
            Part::new(2, &state, Message::Variant3, Message::unwrap3),
        )
    }
}

impl<T, I1, O1, I2, O2, I3, O3, E> Split3<I1, O1, I2, O2, I3, O3, E> for T
where
    T: dnet_base::Transport<Message<I1, I2, I3, (), ()>, Message<O1, O2, O3, (), ()>, E> + Unpin,
    E: std::error::Error,
{
}

/// Trait allowing transports to be split into four of different message types.
pub trait Split4<I1, O1, I2, O2, I3, O3, I4, O4, E>:
    dnet_base::Transport<Message<I1, I2, I3, I4, ()>, Message<O1, O2, O3, O4, ()>, E> + Sized + Unpin
where
    E: std::error::Error,
{
    /// Split into four transports.
    fn split_into_4(
        self,
    ) -> (
        Part<
            I1,
            O1,
            fn(O1) -> Message<O1, O2, O3, O4, ()>,
            fn(Message<I1, I2, I3, I4, ()>) -> Result<I1, Message<I1, I2, I3, I4, ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            (),
        >,
        Part<
            I2,
            O2,
            fn(O2) -> Message<O1, O2, O3, O4, ()>,
            fn(Message<I1, I2, I3, I4, ()>) -> Result<I2, Message<I1, I2, I3, I4, ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            (),
        >,
        Part<
            I3,
            O3,
            fn(O3) -> Message<O1, O2, O3, O4, ()>,
            fn(Message<I1, I2, I3, I4, ()>) -> Result<I3, Message<I1, I2, I3, I4, ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            (),
        >,
        Part<
            I4,
            O4,
            fn(O4) -> Message<O1, O2, O3, O4, ()>,
            fn(Message<I1, I2, I3, I4, ()>) -> Result<I4, Message<I1, I2, I3, I4, ()>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            (),
        >,
    ) {
        let state = State::new(4, self);
        (
            Part::new(0, &state, Message::Variant1, Message::unwrap1),
            Part::new(1, &state, Message::Variant2, Message::unwrap2),
            Part::new(2, &state, Message::Variant3, Message::unwrap3),
            Part::new(3, &state, Message::Variant4, Message::unwrap4),
        )
    }
}

impl<T, I1, O1, I2, O2, I3, O3, I4, O4, E> Split4<I1, O1, I2, O2, I3, O3, I4, O4, E> for T
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, ()>, Message<O1, O2, O3, O4, ()>, E> + Unpin,
    E: std::error::Error,
{
}

/// Trait allowing transports to be split into five of different message types.
pub trait Split5<I1, O1, I2, O2, I3, O3, I4, O4, I5, O5, E>:
    dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Message<O1, O2, O3, O4, O5>, E> + Sized + Unpin
where
    E: std::error::Error,
{
    /// Split into five transports.
    fn split_into_5(
        self,
    ) -> (
        Part<
            I1,
            O1,
            fn(O1) -> Message<O1, O2, O3, O4, O5>,
            fn(Message<I1, I2, I3, I4, I5>) -> Result<I1, Message<I1, I2, I3, I4, I5>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            I5,
        >,
        Part<
            I2,
            O2,
            fn(O2) -> Message<O1, O2, O3, O4, O5>,
            fn(Message<I1, I2, I3, I4, I5>) -> Result<I2, Message<I1, I2, I3, I4, I5>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            I5,
        >,
        Part<
            I3,
            O3,
            fn(O3) -> Message<O1, O2, O3, O4, O5>,
            fn(Message<I1, I2, I3, I4, I5>) -> Result<I3, Message<I1, I2, I3, I4, I5>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            I5,
        >,
        Part<
            I4,
            O4,
            fn(O4) -> Message<O1, O2, O3, O4, O5>,
            fn(Message<I1, I2, I3, I4, I5>) -> Result<I4, Message<I1, I2, I3, I4, I5>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            I5,
        >,
        Part<
            I5,
            O5,
            fn(O5) -> Message<O1, O2, O3, O4, O5>,
            fn(Message<I1, I2, I3, I4, I5>) -> Result<I5, Message<I1, I2, I3, I4, I5>>,
            Self,
            E,
            I1,
            I2,
            I3,
            I4,
            I5,
        >,
    ) {
        let state = State::new(5, self);
        (
            Part::new(0, &state, Message::Variant1, Message::unwrap1),
            Part::new(1, &state, Message::Variant2, Message::unwrap2),
            Part::new(2, &state, Message::Variant3, Message::unwrap3),
            Part::new(3, &state, Message::Variant4, Message::unwrap4),
            Part::new(4, &state, Message::Variant5, Message::unwrap5),
        )
    }
}

impl<T, I1, O1, I2, O2, I3, O3, I4, O4, I5, O5, E> Split5<I1, O1, I2, O2, I3, O3, I4, O4, I5, O5, E>
    for T
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Message<O1, O2, O3, O4, O5>, E> + Unpin,
    E: std::error::Error,
{
}

/// [Part] transport error.
#[derive(Debug)]
pub enum Error<T> {
    /// Unexpected variant received.
    UnexpectedVariantReceived(usize),

    /// Wrapped transport error.
    Transport(T),
}

impl<T> Display for Error<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedVariantReceived(variant) => {
                write!(f, "unexpected message variant received: {variant}")
            }
            Self::Transport(error) => write!(f, "transport error: {}", error),
        }
    }
}

impl<T> std::error::Error for Error<T> where T: Debug + Display {}

/// Wrapping message of split transport.
#[derive(Debug, Serialize, Deserialize)]
pub enum Message<T1, T2, T3, T4, T5> {
    /// Message of the first transport.
    Variant1(T1),

    /// Message of the second transport.
    Variant2(T2),

    /// Message of the third transport.
    Variant3(T3),

    /// Message of the fourth transport.
    Variant4(T4),

    /// Message of the fifth transport.
    Variant5(T5),
}

impl<T1, T2, T3, T4, T5> Message<T1, T2, T3, T4, T5> {
    fn unwrap1(self) -> Result<T1, Self> {
        if let Message::Variant1(message) = self {
            Ok(message)
        } else {
            Err(self)
        }
    }

    fn unwrap2(self) -> Result<T2, Self> {
        if let Message::Variant2(message) = self {
            Ok(message)
        } else {
            Err(self)
        }
    }

    fn unwrap3(self) -> Result<T3, Self> {
        if let Message::Variant3(message) = self {
            Ok(message)
        } else {
            Err(self)
        }
    }

    fn unwrap4(self) -> Result<T4, Self> {
        if let Message::Variant4(message) = self {
            Ok(message)
        } else {
            Err(self)
        }
    }

    fn unwrap5(self) -> Result<T5, Self> {
        if let Message::Variant5(message) = self {
            Ok(message)
        } else {
            Err(self)
        }
    }
}

/// One of the transports after split.
#[pin_project]
pub struct Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5> {
    variant: usize,
    state: Arc<Mutex<State<T, I1, I2, I3, I4, I5>>>,
    wrapper: Wrapper,
    unwrapper: Unwrapper,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
    _error: PhantomData<E>,
}

impl<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
    Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Wrapper::Output, E> + Unpin,
    Wrapper: Mapper<Outgoing>,
    Unwrapper:
        Mapper<Message<I1, I2, I3, I4, I5>, Output = Result<Incoming, Message<I1, I2, I3, I4, I5>>>,
    E: std::error::Error,
{
    fn new(
        variant: usize,
        state: &Arc<Mutex<State<T, I1, I2, I3, I4, I5>>>,
        wrapper: Wrapper,
        unwrapper: Unwrapper,
    ) -> Self {
        Part {
            variant,
            state: state.clone(),
            wrapper,
            unwrapper,

            #[cfg(feature = "logging")]
            logger: {
                let mut logger = dnet_base::Logger::new::<Self>();
                logger.override_kind_part::<Self>(variant);
                logger
            },

            _incoming: PhantomData,
            _outgoing: PhantomData,
            _error: PhantomData,
        }
    }
}

impl<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5> Sink<Outgoing>
    for Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Wrapper::Output, E> + Unpin,
    Wrapper: Mapper<Outgoing>,
    Unwrapper:
        Mapper<Message<I1, I2, I3, I4, I5>, Output = Result<Incoming, Message<I1, I2, I3, I4, I5>>>,
    E: std::error::Error,
{
    type Error = dnet_base::Error<Error<E>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self
            .state
            .lock()
            .unwrap()
            .inner
            .poll_ready_unpin(cx)
            .map_err(map_error);

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        let item = me.wrapper.map(item);
        let result = me
            .state
            .lock()
            .unwrap()
            .inner
            .start_send_unpin(item)
            .map_err(map_error);

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => me.logger.log_message_preparation_success::<Outgoing>(None),
            Err(error) => me.logger.log_sending_failure(error),
        }

        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self
            .state
            .lock()
            .unwrap()
            .inner
            .poll_flush_unpin(cx)
            .map_err(map_error);

        #[cfg(feature = "logging")]
        self.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self
            .state
            .lock()
            .unwrap()
            .inner
            .poll_close_unpin(cx)
            .map_err(map_error);

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5> Stream
    for Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Wrapper::Output, E> + Unpin,
    Wrapper: Mapper<Outgoing>,
    Unwrapper:
        Mapper<Message<I1, I2, I3, I4, I5>, Output = Result<Incoming, Message<I1, I2, I3, I4, I5>>>,
    E: std::error::Error,
{
    type Item = Result<Incoming, Error<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let mut lock = me.state.lock().unwrap();
        let result = if let Some(message) = lock.buffers[*me.variant].pop() {
            Poll::Ready(Some(Ok(me.unwrapper.map(message).ok().unwrap())))
        } else {
            loop {
                let Poll::Ready(item) = lock.inner.poll_next_unpin(cx) else {
                    lock.buffers[*me.variant].update_waker_with(cx.waker());
                    break Poll::Pending;
                };
                match item {
                    Some(Ok(item)) => {
                        let variant = match item {
                            Message::Variant1(_) => 0,
                            Message::Variant2(_) => 1,
                            Message::Variant3(_) => 2,
                            Message::Variant4(_) => 3,
                            Message::Variant5(_) => 4,
                        };
                        match me.unwrapper.map(item) {
                            Ok(item) => break Poll::Ready(Some(Ok(item))),
                            Err(message) => {
                                if let Some(buffer) = lock.buffers.get_mut(variant) {
                                    buffer.push(message);
                                } else {
                                    break Poll::Ready(Some(Err(
                                        Error::UnexpectedVariantReceived(variant),
                                    )));
                                }
                            }
                        }
                    }
                    Some(Err(error)) => break Poll::Ready(Some(Err(Error::Transport(error)))),
                    None => break Poll::Ready(None),
                }
            }
        };

        #[cfg(feature = "logging")]
        me.logger.log_receiving(&result, None);

        result
    }
}

impl<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5> FusedStream
    for Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Wrapper::Output, E> + FusedStream + Unpin,
    Wrapper: Mapper<Outgoing>,
    Unwrapper:
        Mapper<Message<I1, I2, I3, I4, I5>, Output = Result<Incoming, Message<I1, I2, I3, I4, I5>>>,
    E: std::error::Error,
{
    fn is_terminated(&self) -> bool {
        self.state.lock().unwrap().inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5> dnet_base::Logging
    for Part<Incoming, Outgoing, Wrapper, Unwrapper, T, E, I1, I2, I3, I4, I5>
where
    T: dnet_base::Transport<Message<I1, I2, I3, I4, I5>, Wrapper::Output, E> + Unpin,
    Wrapper: Mapper<Outgoing>,
    Unwrapper:
        Mapper<Message<I1, I2, I3, I4, I5>, Output = Result<Incoming, Message<I1, I2, I3, I4, I5>>>,
    E: std::error::Error,
{
    const KIND: &'static str = "Part";

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

struct State<T, I1, I2, I3, I4, I5> {
    inner: T,
    buffers: Vec<Buffer<I1, I2, I3, I4, I5>>,
}

impl<T, I1, I2, I3, I4, I5> State<T, I1, I2, I3, I4, I5> {
    fn new(size: usize, inner: T) -> Arc<Mutex<Self>> {
        let buffers = (0..size).map(|_| Buffer::new()).collect();
        let state = State { inner, buffers };
        Arc::new(Mutex::new(state))
    }
}

struct Buffer<I1, I2, I3, I4, I5> {
    inner: VecDeque<Message<I1, I2, I3, I4, I5>>,
    waker: Option<Waker>,
}

impl<I1, I2, I3, I4, I5> Buffer<I1, I2, I3, I4, I5> {
    fn new() -> Self {
        Buffer {
            inner: VecDeque::new(),
            waker: None,
        }
    }

    fn pop(&mut self) -> Option<Message<I1, I2, I3, I4, I5>> {
        self.inner.pop_front()
    }

    fn push(&mut self, message: Message<I1, I2, I3, I4, I5>) {
        self.inner.push_back(message);
        self.wake();
    }

    fn update_waker_with(&mut self, other: &Waker) {
        if let Some(waker) = &self.waker {
            if !waker.will_wake(other) {
                self.waker = Some(other.clone());
            }
        } else {
            self.waker = Some(other.clone());
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
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

    use crate::{channel::transports, split::Split2};

    dtest_configure!();

    #[dtest]
    async fn test_split() {
        let (left, right) = transports();

        let (mut left_string_i32, mut left_u32_f64) = left.split_into_2();
        let (mut right_i32_string, mut right_f64_u32) = right.split_into_2();

        dnet_tests::init_logging(&mut left_string_i32, &mut right_i32_string);
        dnet_tests::init_logging(&mut left_u32_f64, &mut right_f64_u32);

        left_string_i32.send(-50).await.unwrap();
        left_u32_f64.send(770.0).await.unwrap();

        right_i32_string.send("Hello".to_string()).await.unwrap();
        right_f64_u32.send(66).await.unwrap();

        assert_eq!(left_u32_f64.receive().await.unwrap(), 66);
        assert_eq!(left_string_i32.receive().await.unwrap(), "Hello");
        assert_eq!(right_f64_u32.receive().await.unwrap(), 770.0);
        assert_eq!(right_i32_string.receive().await.unwrap(), -50);
    }
}
