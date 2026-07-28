//! Transport for communication over
//! [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket).

use std::{
    cell::RefCell,
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use futures::{channel::oneshot, stream::FusedStream, Sink, Stream};
use js_sys::{ArrayBuffer, Uint8Array};
use js_utils::{
    event::{EventListener, When},
    JsError,
};
use serde::Serialize;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use crate::{js::state::State, Decode, Encode};

/// WebSocket transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Error occurred during sending a message.
    SendingError(JsError),

    /// Error occurred during closing the transport.
    ClosingError(JsError),

    /// Malformed message received.
    MalformedMessage,

    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),

    /// [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)-specific error.
    WebSocketError(JsError),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SendingError(error) => write!(f, "failed to send message: {error}"),
            Error::ClosingError(error) => write!(f, "failed to close transport: {error}"),
            Error::MalformedMessage => write!(f, "malformed message received"),
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
            Error::WebSocketError(_error) => write!(f, "WebSocket error occurred"),
        }
    }
}

impl<SerializationError, DeserializationError> std::error::Error
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Debug + Display,
    DeserializationError: Debug + Display,
{
}

/// Transport for communication over
/// [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
pub struct WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    web_socket: Rc<WebSocket>,
    codec: Rc<RefCell<Codec>>,
    #[allow(clippy::type_complexity)]
    state: Rc<RefCell<State<JsValue, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>>>,
    buffer: RefCell<Vec<u8>>,

    #[cfg(feature = "logging")]
    logger: Rc<RefCell<dnet_base::Logger>>,

    _message_listener: EventListener<WebSocket, MessageEvent>,
    _error_listener: EventListener<WebSocket, Event>,
    _close_listener: EventListener<WebSocket, CloseEvent>,
    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<Codec, Incoming, Outgoing> WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    /// Create new transport for WebSocket without waiting for the `open` event.
    pub fn new_assuming_open(
        web_socket: WebSocket,
        codec: Codec,
    ) -> Result<Self, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>> {
        web_socket.set_binary_type(BinaryType::Arraybuffer);
        let web_socket = Rc::new(web_socket);
        let codec = Rc::new(RefCell::new(codec));
        let state = Rc::new(RefCell::new(State::new()));

        #[cfg(feature = "logging")]
        let logger = Rc::new(RefCell::new(dnet_base::Logger::new::<Self>()));

        #[cfg(feature = "logging")]
        let logger_clone = logger.clone();

        let state_clone = state.clone();
        let message_listener = web_socket
            .when("message", move |event: MessageEvent| {
                #[cfg(feature = "logging")]
                logger_clone.borrow().log_message_arrived_unknown(None);

                state_clone.borrow_mut().message(event.data());
            })
            .map_err(Error::WebSocketError)?;

        let state_clone = state.clone();
        let error_listener = web_socket
            .when("error", move |event: Event| {
                state_clone
                    .borrow_mut()
                    .error(Error::WebSocketError(JsValue::from(event).into()));
                state_clone.borrow_mut().close();
            })
            .map_err(Error::WebSocketError)?;

        let state_clone = state.clone();
        let close_listener = web_socket
            .when("close", move |_event: CloseEvent| {
                state_clone.borrow_mut().close();
            })
            .map_err(Error::WebSocketError)?;

        let buffer = RefCell::new(vec![]);

        let transport = WebSocketTransport {
            web_socket,
            codec,
            state,
            buffer,

            #[cfg(feature = "logging")]
            logger,

            _message_listener: message_listener,
            _error_listener: error_listener,
            _close_listener: close_listener,
            _incoming: PhantomData,
            _outgoing: PhantomData,
        };
        Ok(transport)
    }

    /// Create new transport for WebSocket.
    ///
    /// It waits for WebSocket's `open` event before returning transport.
    pub async fn new(
        web_socket: WebSocket,
        codec: Codec,
    ) -> Result<Self, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>> {
        let transport = WebSocketTransport::new_assuming_open(web_socket, codec)?;

        let (open_sender, open_receiver) = oneshot::channel();
        let open_sender = Rc::new(RefCell::new(Some(open_sender)));

        let open_sender_clone = open_sender.clone();
        let _open_listener = transport
            .web_socket
            .when("open", move |_event: Event| {
                if let Some(notifier) = open_sender_clone.borrow_mut().take() {
                    let _ = notifier.send(Ok(()));
                } else {
                    unreachable!("open message received twice!")
                }
            })
            .map_err(Error::WebSocketError)?;

        let _error_listener = transport
            .web_socket
            .when("error", move |event: Event| {
                if let Some(notifier) = open_sender.borrow_mut().take() {
                    let _ = notifier.send(Err(event));
                } else {
                    unreachable!("open message received twice!")
                }
            })
            .map_err(Error::WebSocketError)?;

        if let Ok(result) = open_receiver.await {
            result
                .map_err(|event| JsError::from(JsValue::from(event)))
                .map_err(Error::WebSocketError)?;
        }

        #[cfg(feature = "logging")]
        transport.logger.borrow().log_open_success();

        Ok(transport)
    }

    /// Create new transport connecting to given URL.
    pub async fn new_with_address(
        url: &str,
        codec: Codec,
    ) -> Result<Self, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>> {
        let web_socket = WebSocket::new(url)
            .map_err(JsError::from)
            .map_err(Error::WebSocketError)?;
        WebSocketTransport::new(web_socket, codec).await
    }

    fn send_inner(
        &self,
        message: Outgoing,
        message_length: &mut usize,
    ) -> Result<(), Error<<Codec as Encode>::Error, <Codec as Decode>::Error>> {
        let mut buffer = self.buffer.borrow_mut();
        buffer.clear();
        self.codec
            .borrow_mut()
            .encode(&mut *buffer, &message)
            .map_err(Error::SerializationError)?;
        *message_length = buffer.len();
        self.web_socket
            .send_with_u8_array(&buffer[..])
            .map_err(|error| Error::SendingError(error.into()))?;
        Ok(())
    }
}

impl<Codec, Incoming, Outgoing> Drop for WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    fn drop(&mut self) {
        let _ = self.web_socket.close();
    }
}

impl<Codec, Incoming, Outgoing> Sink<Outgoing> for WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.borrow().log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let mut message_length = 0;

        let result = if self.state.borrow().closed {
            Err(crate::Error::Closed)
        } else {
            self.send_inner(item, &mut message_length)
                .map_err(crate::Error::Other)
        };

        #[cfg(not(feature = "logging"))]
        let _ = message_length;

        #[cfg(feature = "logging")]
        self.logger
            .borrow()
            .log_sending::<Outgoing, _>(&result, Some(message_length));

        result
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = if self.state.borrow().closed {
            Poll::Ready(Err(crate::Error::Closed))
        } else {
            Poll::Ready(Ok(()))
        };

        #[cfg(feature = "logging")]
        self.logger.borrow().log_flush(&result);

        result
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = if self.state.borrow().closed {
            Poll::Ready(Err(crate::Error::Closed))
        } else {
            let result = self
                .web_socket
                .close()
                .map_err(|error| {
                    Error::<<Codec as Encode>::Error, <Codec as Decode>::Error>::ClosingError(
                        error.into(),
                    )
                })
                .map_err(crate::Error::Other);
            self.state.borrow_mut().close();
            Poll::Ready(result)
        };

        #[cfg(feature = "logging")]
        self.logger.borrow().log_close(&result);

        result
    }
}

impl<Codec, Incoming, Outgoing> Stream for WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut message_length = 0;

        let mut state = self.state.borrow_mut();
        let result = if state.is_terminated() {
            Poll::Ready(None)
        } else if let Some(item) = state.incoming.pop_front() {
            Poll::Ready(Some(item.and_then(|item| {
                let data = item
                    .dyn_into::<ArrayBuffer>()
                    .map_err(|_| Error::MalformedMessage)?;
                let data = Uint8Array::new(&data).to_vec();
                message_length = data.len();
                self.codec
                    .borrow_mut()
                    .decode(&data[..])
                    .map_err(Error::DeserializationError)
            })))
        } else {
            state.update_waker_with(cx.waker());
            Poll::Pending
        };

        #[cfg(not(feature = "logging"))]
        let _ = message_length;

        #[cfg(feature = "logging")]
        self.logger
            .borrow()
            .log_receiving(&result, Some(message_length));

        result
    }
}

impl<Codec, Incoming, Outgoing> FusedStream for WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    fn is_terminated(&self) -> bool {
        self.state.borrow().is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<Codec, Incoming, Outgoing> dnet_base::Logging for WebSocketTransport<Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
{
    const KIND: &'static str = "WebSocket";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        f(&self.logger.borrow())
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        f(&mut self.logger.borrow_mut())
    }
}

#[cfg(test)]
mod tests {
    use dnet_tests::dtest_configure;
    use serde::{Deserialize, Serialize};
    use wasm_bindgen_test::wasm_bindgen_test;
    use web_sys::WebSocket;

    use crate::codecs::BincodeCodec;

    use super::WebSocketTransport;

    dtest_configure!();

    async fn create_transports<I, O>(
        port: u16,
    ) -> (
        WebSocketTransport<BincodeCodec, I, O>,
        WebSocketTransport<BincodeCodec, O, I>,
    )
    where
        I: Serialize,
        for<'de> I: Deserialize<'de>,
        O: Serialize,
        for<'de> O: Deserialize<'de>,
    {
        let left = WebSocket::new(&format!("ws://localhost:{port}/left")).unwrap();
        let left = WebSocketTransport::new(left, BincodeCodec::default())
            .await
            .unwrap();

        let right = WebSocket::new(&format!("ws://localhost:{port}/right")).unwrap();
        let right = WebSocketTransport::new(right, BincodeCodec::default())
            .await
            .unwrap();

        (left, right)
    }

    #[wasm_bindgen_test]
    async fn test_transport() {
        let (left, right) = create_transports(3000).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[wasm_bindgen_test]
    async fn test_unit_message() {
        let (left, right) = create_transports(3000).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[wasm_bindgen_test]
    async fn test_stream() {
        let (left, right) = create_transports(3000).await;
        dnet_tests::test_stream(left, right).await;
    }
}
