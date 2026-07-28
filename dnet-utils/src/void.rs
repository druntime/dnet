//! Transport sending messages into the void, never receiving any messages.

use std::{
    convert::Infallible,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, Stream};

/// Transport sending messages into the void, never receiving any messages.
///
/// Sending messages into this transport will always succeed.<br>
/// Attempt to receive message will never complete.
pub struct Void<Incoming, Outgoing> {
    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<Incoming, Outgoing> Default for Void<Incoming, Outgoing> {
    fn default() -> Self {
        Self {
            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

impl<Incoming, Outgoing> Sink<Outgoing> for Void<Incoming, Outgoing> {
    type Error = dnet_base::Error<Infallible>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, _item: Outgoing) -> Result<(), Self::Error> {
        let result = Ok(());

        #[cfg(feature = "logging")]
        self.logger.log_sending::<Outgoing, _>(&result, None);

        result
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<Incoming, Outgoing> Stream for Void<Incoming, Outgoing> {
    type Item = Result<Incoming, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #[cfg(feature = "logging")]
        self.logger.log_receive_from_void();

        Poll::Pending
    }
}

impl<Incoming, Outgoing> FusedStream for Void<Incoming, Outgoing> {
    fn is_terminated(&self) -> bool {
        false
    }
}

#[cfg(feature = "logging")]
impl<Incoming, Outgoing> dnet_base::Logging for Void<Incoming, Outgoing> {
    const KIND: &'static str = "Void";

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
    use core::panic;
    use std::pin::pin;
    use std::time::Duration;

    use dnet_base::Receive;
    use dnet_tests::{dtest, dtest_configure};
    use dportable::time::sleep;
    use futures::{select, FutureExt, SinkExt};

    use crate::void::Void;

    dtest_configure!();

    #[dtest]
    async fn test_void() {
        let mut void: Void<i32, &str> = Void::default();

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            dnet_tests::init_subscriber();
            void.enable_logging();
        }

        void.send("Hello").await.unwrap();

        let mut delay = pin!(sleep(Duration::from_millis(10)).fuse());
        let delay_finished_first;
        select! {
            _ = delay => {
                delay_finished_first = true;
            }
            _result = void.receive() => {
                panic!("received message");
            }
        }
        assert!(delay_finished_first);
    }
}
