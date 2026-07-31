//! Pipe messages from one transport into another and vice versa.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use dportable::{spawn, value::Notifier};
use futures::{channel::oneshot, future::FusedFuture, select, FutureExt, Sink, SinkExt, StreamExt};

use crate::ConditionalSend;

/// Helper trait for transports that can be piped.
pub trait Transport<Incoming, Outgoing, Error>:
    dnet_base::Transport<Incoming, Outgoing, Error> + ConditionalSend + Unpin + 'static
{
}
impl<T, Incoming, Outgoing, Error> Transport<Incoming, Outgoing, Error> for T where
    T: dnet_base::Transport<Incoming, Outgoing, Error> + ConditionalSend + Unpin + 'static
{
}

/// Helper trait for pipe-able transport message.
pub trait Message: Clone + ConditionalSend + 'static {}
impl<T> Message for T where T: Clone + ConditionalSend + 'static {}

/// Helper trait for pipe-able transport error.
pub trait Error: ConditionalSend + 'static {}
impl<T> Error for T where T: ConditionalSend + 'static {}

/// Strategy of handling message passing errors.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ErrorHandlingStrategy {
    /// Ignore the error.
    #[default]
    Ignore,

    /// Try again to send/receive message.
    Retry,

    /// Close pipe.
    Close,
}

/// Callback called when an error is encountered while trying to send a message
/// through the transport.
///
/// **NOTE**: it also receives [dnet_base::Error::Closed] errors and
/// they need to be handled properly (most likely by returning
/// [ErrorHandlingStrategy::Close]).
pub trait SendErrorCallback<Message, Error>: ConditionalSend + 'static {
    /// Handle sending error.
    ///
    /// **NOTE**: it also receives [dnet_base::Error::Closed] errors and
    /// they need to be handled properly (most likely by returning
    /// [ErrorHandlingStrategy::Close]).
    fn on_send_error(
        &mut self,
        message: &Message,
        error: dnet_base::Error<Error>,
    ) -> ErrorHandlingStrategy;
}

impl<T, Message, Error> SendErrorCallback<Message, Error> for T
where
    T: FnMut(&Message, dnet_base::Error<Error>) -> ErrorHandlingStrategy
        + ConditionalSend
        + 'static,
{
    fn on_send_error(
        &mut self,
        message: &Message,
        error: dnet_base::Error<Error>,
    ) -> ErrorHandlingStrategy {
        (self)(message, error)
    }
}

/// Callback called when an error is encountered while
/// trying to receive a message from the transport.
pub trait ReceiveErrorCallback<Error>: ConditionalSend + 'static {
    /// Handle receiving error.
    fn on_receive_error(&mut self, error: Error) -> ErrorHandlingStrategy;
}

impl<T, Error> ReceiveErrorCallback<Error> for T
where
    T: FnMut(Error) -> ErrorHandlingStrategy + ConditionalSend + 'static,
{
    fn on_receive_error(&mut self, error: Error) -> ErrorHandlingStrategy {
        (self)(error)
    }
}

/// Default [SendErrorCallback].
///
/// It ignores errors (except [dnet_base::Error::Closed] error - which results in
/// closing pipe).
#[derive(Debug)]
pub struct DefaultSendErrorCallback;

impl<Message, Error> SendErrorCallback<Message, Error> for DefaultSendErrorCallback {
    fn on_send_error(
        &mut self,
        _message: &Message,
        error: dnet_base::Error<Error>,
    ) -> ErrorHandlingStrategy {
        match error {
            dnet_base::Error::Closed => ErrorHandlingStrategy::Close,
            dnet_base::Error::Other(_) => ErrorHandlingStrategy::Ignore,
        }
    }
}

/// Default [ReceiveErrorCallback].
///
/// It ignores errors.
#[derive(Debug)]
pub struct DefaultReceiveErrorCallback;

impl<Error> ReceiveErrorCallback<Error> for DefaultReceiveErrorCallback {
    fn on_receive_error(&mut self, _error: Error) -> ErrorHandlingStrategy {
        ErrorHandlingStrategy::Ignore
    }
}

/// Pipe message passing error handler.
pub struct ErrorHandler<Message, Error> {
    /// Callback called when an error is encountered while trying to send a message
    /// through the transport.
    pub send_error_callback: Box<dyn SendErrorCallback<Message, Error>>,

    /// Callback called when an error is encountered while trying to receive a message
    /// from the transport.
    pub receive_error_callback: Box<dyn ReceiveErrorCallback<Error>>,
}

impl<Message, Error> ErrorHandler<Message, Error> {
    /// Create new error handler.
    pub fn new<S, R>(send_error_callback: S, receive_error_callback: R) -> Self
    where
        S: SendErrorCallback<Message, Error>,
        R: ReceiveErrorCallback<Error>,
    {
        ErrorHandler {
            send_error_callback: Box::new(send_error_callback),
            receive_error_callback: Box::new(receive_error_callback),
        }
    }
}

impl<Message, Error> Default for ErrorHandler<Message, Error> {
    fn default() -> Self {
        ErrorHandler::new(DefaultSendErrorCallback, DefaultReceiveErrorCallback)
    }
}

/// Pipe sending messages from one transport into another and vice versa.
#[derive(Debug)]
pub struct Pipe {
    stop_sender: Option<oneshot::Sender<()>>,
    keep_open: bool,
    closed: Notifier,
}

impl Pipe {
    /// Create new pipe sending messages from one transport into another and vice versa.
    pub fn new<A, B, M1, M2, E1, E2>(
        a: A,
        b: B,
        mut a_error_handler: ErrorHandler<M2, E1>,
        mut b_error_handler: ErrorHandler<M1, E2>,
    ) -> Self
    where
        A: Transport<M1, M2, E1> + Unpin,
        B: Transport<M2, M1, E2> + Unpin,
        M1: Message,
        M2: Message,
        E1: Error,
        E2: Error,
    {
        let (stop_sender, mut stop_receiver) = oneshot::channel();
        let stop_sender = Some(stop_sender);
        let closed = Notifier::new();
        let closed_clone = closed.clone();
        spawn(async move {
            let (mut sender_a, receiver_a) = a.split();
            let mut receiver_a = receiver_a.fuse();
            let (mut sender_b, receiver_b) = b.split();
            let mut receiver_b = receiver_b.fuse();
            let mut should_close = false;
            loop {
                select! {
                    a = receiver_a.next() => {
                        handle_receive_result(
                            &mut sender_b,
                            a,
                            &mut a_error_handler.receive_error_callback,
                            &mut b_error_handler.send_error_callback,
                            &mut should_close
                        ).await;
                    }
                    b = receiver_b.next() => {
                        handle_receive_result(
                            &mut sender_a,
                            b,
                            &mut b_error_handler.receive_error_callback,
                            &mut a_error_handler.send_error_callback,
                            &mut should_close
                        ).await;
                    }
                    result = stop_receiver => {
                        if result.is_ok() {
                            should_close = true
                        }
                    }
                }
                if should_close {
                    break;
                }
            }
            closed_clone.notify();
        });
        Pipe {
            stop_sender,
            keep_open: false,
            closed,
        }
    }

    /// Is pipe still open.
    pub fn open(&self) -> bool {
        !self.closed.already_notified()
    }

    /// Stop message interchange.
    pub fn break_pipe(mut self) {
        self.keep_open = false;
        // drop(self)
    }

    /// Keep pipe open after drop.
    pub fn keep_open(&mut self) {
        self.keep_open = true;
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        if !self.keep_open {
            if let Some(sender) = self.stop_sender.take() {
                let _ = sender.send(());
            }
        }
    }
}

impl Future for Pipe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.closed.poll_unpin(cx)
    }
}

impl FusedFuture for Pipe {
    fn is_terminated(&self) -> bool {
        self.closed.is_terminated()
    }
}

/// Pipe messages from one transport into another and vice versa.
///
/// Pipe created with this function ignores errors.
pub fn pipe<A, B, M1, M2, E1, E2>(a: A, b: B) -> Pipe
where
    A: Transport<M1, M2, E1> + Unpin,
    B: Transport<M2, M1, E2> + Unpin,
    M1: Message,
    M2: Message,
    E1: Error,
    E2: Error,
{
    Pipe::new(a, b, Default::default(), Default::default())
}

async fn handle_receive_result<S, M, ER, ES>(
    sender: &mut S,
    result: Option<Result<M, ER>>,
    receive_error_callback: &mut Box<dyn ReceiveErrorCallback<ER>>,
    send_error_callback: &mut Box<dyn SendErrorCallback<M, ES>>,
    should_close: &mut bool,
) where
    S: Sink<M, Error = dnet_base::Error<ES>> + Unpin,
    M: Message,
    ES: Error,
    ER: Error,
{
    if let Some(result) = result {
        match result {
            Ok(message) => {
                send(sender, message, send_error_callback, should_close).await;
            }
            Err(error) => {
                let strategy = receive_error_callback.on_receive_error(error);
                if matches!(strategy, ErrorHandlingStrategy::Close) {
                    *should_close = true;
                }
            }
        }
    } else {
        *should_close = true;
    }
}

async fn send<S, M, E>(
    sender: &mut S,
    message: M,
    send_error_callback: &mut Box<dyn SendErrorCallback<M, E>>,
    should_close: &mut bool,
) where
    S: Sink<M, Error = dnet_base::Error<E>> + Unpin,
    M: Message,
    E: Error,
{
    while let Err(error) = sender.send(message.clone()).await {
        match send_error_callback.on_send_error(&message, error) {
            ErrorHandlingStrategy::Ignore => {
                break;
            }
            ErrorHandlingStrategy::Retry => {
                continue;
            }
            ErrorHandlingStrategy::Close => {
                *should_close = true;
                return;
            }
        }
    }
    *should_close = false;
}

#[cfg(test)]
mod tests {
    use dnet_base::Receive;
    use dnet_tests::{dtest, dtest_configure};
    use futures::SinkExt;

    use crate::channel::{transports, ChannelTransport};

    use super::{pipe, Message, Pipe};

    dtest_configure!();

    fn create_transports<A, B>() -> (ChannelTransport<A, B>, ChannelTransport<B, A>, Pipe)
    where
        A: Message,
        B: Message,
    {
        let (out_a, to_pipe_a) = transports();
        let (out_b, to_pipe_b) = transports();
        let pipe = pipe(to_pipe_a, to_pipe_b);
        (out_a, out_b, pipe)
    }

    #[dtest]
    async fn test_transport() {
        let (left, right, _pipe) = create_transports();
        dnet_tests::test_transport(left, right).await;
    }

    #[dtest]
    async fn test_unit_message() {
        let (left, right, _pipe) = create_transports();
        dnet_tests::test_unit_message(left, right).await;
    }

    #[dtest]
    async fn test_stream() {
        let (left, right, _pipe) = create_transports();
        dnet_tests::test_stream(left, right).await;
    }

    #[dtest]
    async fn test_pipe_drop() {
        let (mut left, mut right, pipe) = create_transports();

        dnet_tests::init_logging(&mut left, &mut right);

        left.send(1).await.unwrap();
        right.send(1).await.unwrap();
        assert_eq!(left.receive().await.unwrap(), 1);
        assert_eq!(right.receive().await.unwrap(), 1);
        drop(pipe);
        assert!(matches!(
            left.receive().await,
            Err(dnet_base::Error::Closed)
        ));
        assert!(matches!(
            right.receive().await,
            Err(dnet_base::Error::Closed)
        ));
    }
}
