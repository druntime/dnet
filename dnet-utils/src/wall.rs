//! Transport that rejects every operation.

use std::{
    fmt::Display,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, Stream};

/// Transport rejecting every operation.
///
/// Attempting to send, receive from, flush or close this transport will result in an error.
pub struct Wall<Incoming, Outgoing> {
    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<Incoming, Outgoing> Default for Wall<Incoming, Outgoing> {
    fn default() -> Self {
        Self {
            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

/// Transport-specific errors for [`Wall`].
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Attempted to send message into the wall.
    Send,

    /// Attempted to receive message from the wall.
    Receive,

    /// Attempted to flush the wall.
    Flush,

    /// Attempted to close the wall.
    Close,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Send => write!(f, "attempted to send message into the wall"),
            Self::Receive => write!(f, "attempted to receive message from the wall"),
            Self::Flush => write!(f, "attempted to flush the wall"),
            Self::Close => write!(f, "attempted to close the wall"),
        }
    }
}

impl std::error::Error for Error {}

impl<Incoming, Outgoing> Sink<Outgoing> for Wall<Incoming, Outgoing> {
    type Error = dnet_base::Error<Error>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, _item: Outgoing) -> Result<(), Self::Error> {
        let result = Err(dnet_base::Error::Other(Error::Send));

        #[cfg(feature = "logging")]
        match &result {
            Ok(_) => unreachable!(),
            Err(error) => self.logger.log_sending_failure(error),
        }

        result
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Err(dnet_base::Error::Other(Error::Flush)));

        #[cfg(feature = "logging")]
        self.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Err(dnet_base::Error::Other(Error::Close)));

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<Incoming, Outgoing> Stream for Wall<Incoming, Outgoing> {
    type Item = Result<Incoming, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Poll::Ready(Some(Err(Error::Receive)));

        #[cfg(feature = "logging")]
        self.logger.log_receiving(&result, None);

        result
    }
}

impl<Incoming, Outgoing> FusedStream for Wall<Incoming, Outgoing> {
    fn is_terminated(&self) -> bool {
        false
    }
}

#[cfg(feature = "logging")]
impl<Incoming, Outgoing> dnet_base::Logging for Wall<Incoming, Outgoing> {
    const KIND: &'static str = "Wall";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        f(&self.logger)
    }

    fn with_logger_mut<'a, F, R>(&mut self, f: F) -> R
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

    use crate::wall::{Error, Wall};

    dtest_configure!();

    fn enable_logging<Incoming, Outgoing>(wall: &mut Wall<Incoming, Outgoing>) {
        #[cfg(not(feature = "logging"))]
        {
            let _ = wall;
        }

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            dnet_tests::init_subscriber();
            wall.enable_logging();
        }
    }

    #[dtest]
    async fn test_wall_send_error() {
        let mut wall: Wall<i32, &str> = Wall::default();

        enable_logging(&mut wall);

        let result = wall.send("Hello").await;
        assert_eq!(result, Err(dnet_base::Error::Other(Error::Send)));
    }

    #[dtest]
    async fn test_wall_flush_error() {
        let mut wall: Wall<i32, &str> = Wall::default();

        enable_logging(&mut wall);

        let result = wall.flush().await;
        assert_eq!(result, Err(dnet_base::Error::Other(Error::Flush)));
    }

    #[dtest]
    async fn test_wall_close_error() {
        let mut wall: Wall<i32, &str> = Wall::default();

        enable_logging(&mut wall);

        let result = wall.close().await;
        assert_eq!(result, Err(dnet_base::Error::Other(Error::Close)));
    }

    #[dtest]
    async fn test_wall_receive_error() {
        let mut wall: Wall<i32, &str> = Wall::default();

        enable_logging(&mut wall);

        let result = wall.receive().await;
        assert_eq!(result, Err(dnet_base::Error::Other(Error::Receive)));
    }
}
