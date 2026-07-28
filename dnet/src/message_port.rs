//! Transport for communication over
//! [MessagePort](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort)

use std::{
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

#[cfg(feature = "logging")]
use dnet_base::Logging;
use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;
use serde::Serialize;
use web_sys::MessagePort;

use crate::js::{self, Transport};

/// [MessagePortTransport] error.
pub type Error<Codec> = js::Error<Codec>;

/// Transport for communication over
/// [MessagePort](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort)
#[pin_project]
pub struct MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    #[pin]
    inner: Transport<MessagePort, Codec, Incoming, Outgoing>,
}

impl<Codec, Incoming, Outgoing> MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    /// Create a new transport over the given `MessagePort`.
    pub async fn new(port: MessagePort, codec: Codec) -> Result<Self, Error<Codec>> {
        let port = Rc::new(port);
        let mut inner = Transport::new(&port, None, codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| logger.override_kind::<Self>());

        port.start();
        inner.wait_for_open().await;

        Ok(MessagePortTransport { inner })
    }

    /// Create a new named transport over the given `MessagePort`.
    pub async fn new_with_name(
        port: MessagePort,
        codec: Codec,
        name: &str,
    ) -> Result<Self, Error<Codec>> {
        let port = Rc::new(port);
        let mut inner = Transport::new(&port, Some(name.to_string()), codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| logger.override_kind::<Self>());

        port.start();
        inner.wait_for_open().await;

        Ok(MessagePortTransport { inner })
    }

    /// Get the name of the transport.
    pub fn name(&self) -> Option<&str> {
        self.inner.name()
    }
}

impl<Codec, Incoming, Outgoing> Sink<Outgoing> for MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    type Error = crate::Error<Error<Codec>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        self.project().inner.start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close_unpin(cx)
    }
}

impl<Codec, Incoming, Outgoing> Stream for MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    type Item = Result<Incoming, Error<Codec>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next_unpin(cx)
    }
}

impl<Codec, Incoming, Outgoing> FusedStream for MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Codec, Incoming, Outgoing> dnet_base::Logging
    for MessagePortTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    const KIND: &'static str = "MessagePort";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        self.inner.with_logger(f)
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        self.inner.with_logger_mut(f)
    }
}

#[cfg(test)]
mod tests {
    use dnet_tests::dtest_configure;
    use futures::join;
    use serde::{Deserialize, Serialize};
    use wasm_bindgen_test::wasm_bindgen_test;
    use web_sys::MessageChannel;

    use crate::codecs::JsonCodec;

    use super::MessagePortTransport;

    dtest_configure!();

    async fn create_transports<I, O>() -> (
        MessagePortTransport<JsonCodec, I, O>,
        MessagePortTransport<JsonCodec, O, I>,
    )
    where
        I: Serialize + 'static,
        for<'de> I: Deserialize<'de>,
        O: Serialize + 'static,
        for<'de> O: Deserialize<'de>,
    {
        let channel = MessageChannel::new().unwrap();

        let left = channel.port1();
        let right = channel.port2();

        let left = MessagePortTransport::new(left, JsonCodec::default());
        let right = MessagePortTransport::new(right, JsonCodec::default());

        let (left, right) = join!(left, right);
        let left = left.unwrap();
        let right = right.unwrap();

        (left, right)
    }

    #[wasm_bindgen_test]
    async fn test_transport() {
        let (left, right) = create_transports().await;
        dnet_tests::test_transport(left, right).await;
    }

    #[wasm_bindgen_test]
    async fn test_unit_message() {
        let (left, right) = create_transports().await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[wasm_bindgen_test]
    async fn test_stream() {
        let (left, right) = create_transports().await;
        dnet_tests::test_stream(left, right).await;
    }
}
