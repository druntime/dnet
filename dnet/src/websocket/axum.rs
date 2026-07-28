//! Transport for communication over
//! [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
//! while using [axum](https://github.com/tokio-rs/axum).

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use axum::extract::ws::Message;
use futures::{stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;
use serde::Serialize;

use crate::{Decode, Encode};

/// WebSocket transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Error that occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error that occurred during deserialization of a message.
    DeserializationError(DeserializationError),

    /// [Axum](https://github.com/tokio-rs/axum)-specific error.
    AxumError(axum::Error),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
            Error::AxumError(error) => write!(f, "axum error occurred: {error}"),
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

/// Web Socket transport for [axum](https://github.com/tokio-rs/axum).
///
/// Wraps around [axum::extract::ws::WebSocket].
///
/// **NOTE**: This transport's receiving stream ignores all non-binary (text, ping, pong, close) messages.
#[pin_project]
pub struct WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: T,
    codec: Codec,
    terminated: bool,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided [axum::extract::ws::WebSocket].
    pub fn new(web_socket: T, codec: Codec) -> Self {
        WebSocketTransport {
            inner: web_socket,
            codec,
            terminated: false,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }

    #[allow(clippy::type_complexity)]
    fn send_inner(
        &mut self,
        message: Outgoing,
        message_length: &mut usize,
    ) -> Result<(), crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>> {
        let mut buffer = vec![];
        self.codec
            .encode(&mut buffer, &message)
            .map_err(Error::SerializationError)
            .map_err(crate::Error::Other)?;
        *message_length = buffer.len();
        let message = Message::binary(buffer);
        self.inner.start_send_unpin(message).map_err(map_axum_error)
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_ready_unpin(cx).map_err(map_axum_error);

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(mut self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let mut message_length = 0;

        let result = self.send_inner(item, &mut message_length);

        #[cfg(feature = "logging")]
        self.logger
            .log_sending::<Outgoing, _>(&result, Some(message_length));

        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_flush_unpin(cx).map_err(map_axum_error);

        #[cfg(feature = "logging")]
        self.logger.log_flush(&result);

        result
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_close_unpin(cx).map_err(map_axum_error);

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut message_length = 0;

        let result = match self.inner.poll_next_unpin(cx) {
            Poll::Ready(item) => {
                if let Some(item) = item {
                    match item {
                        Ok(message) => match message {
                            Message::Binary(bytes) => {
                                message_length = bytes.len();
                                let result: Result<Incoming, _> = self.codec.decode(bytes.as_ref());
                                match result {
                                    Ok(message) => Poll::Ready(Some(Ok(message))),
                                    Err(error) => {
                                        Poll::Ready(Some(Err(Error::DeserializationError(error))))
                                    }
                                }
                            }
                            Message::Close(_close_frame) => {
                                self.terminated = true;
                                Poll::Ready(None)
                            }
                            _ => Poll::Pending,
                        },
                        Err(axum_error) => {
                            use std::error::Error;
                            if let Some(error) = axum_error.source() {
                                if let Some(tungstenite_error) =
                                    error.downcast_ref::<tungstenite::Error>()
                                {
                                    match tungstenite_error {
                                        tungstenite::Error::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake) => {
                                            self.terminated = true;
                                            Poll::Ready(None)
                                        },
                                        tungstenite::Error::Io(error) => {
                                            if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted) {
                                                self.terminated = true;
                                                Poll::Ready(None)
                                            } else {
                                                Poll::Ready(Some(Err(self::Error::AxumError(axum_error))))
                                            }
                                        }
                                        _ => Poll::Ready(Some(Err(self::Error::AxumError(axum_error)))),
                                    }
                                } else {
                                    Poll::Ready(Some(Err(self::Error::AxumError(axum_error))))
                                }
                            } else {
                                Poll::Ready(Some(Err(self::Error::AxumError(axum_error))))
                            }
                        }
                    }
                } else {
                    self.terminated = true;
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        };

        #[cfg(not(feature = "logging"))]
        let _ = message_length;

        #[cfg(feature = "logging")]
        self.logger.log_receiving(&result, Some(message_length));

        result
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = axum::Error> + Stream<Item = Result<Message, axum::Error>> + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "WebSocket(axum)";

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

fn map_axum_error<SerializationError, DeserializationError>(
    axum_error: axum::Error,
) -> crate::Error<Error<SerializationError, DeserializationError>> {
    use std::error::Error;
    if let Some(error) = axum_error.source() {
        if let Some(error) = error.downcast_ref::<tungstenite::Error>() {
            if matches!(
                error,
                tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed
            ) {
                crate::Error::Closed
            } else {
                crate::Error::Other(self::Error::AxumError(axum_error))
            }
        } else {
            crate::Error::Other(self::Error::AxumError(axum_error))
        }
    } else {
        crate::Error::Other(self::Error::AxumError(axum_error))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{
        extract::{ws::WebSocket, WebSocketUpgrade},
        routing::any,
        Router,
    };
    use futures::channel::oneshot;
    use serde::{Deserialize, Serialize};
    use tokio::{
        net::{TcpListener, TcpStream},
        spawn,
    };
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use super::WebSocketTransport;

    use crate::codecs::BincodeCodec;

    async fn create_transports<I, O>(
        port: u16,
    ) -> (
        WebSocketTransport<WebSocket, BincodeCodec, I, O>,
        crate::websocket::WebSocketTransport<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            BincodeCodec,
            O,
            I,
        >,
    )
    where
        I: Serialize,
        for<'de> I: Deserialize<'de>,
        O: Serialize,
        for<'de> O: Deserialize<'de>,
    {
        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let ws_handler = |ws: WebSocketUpgrade| async move {
            ws.on_upgrade(|socket| async move {
                if let Some(tx) = tx.lock().unwrap().take() {
                    tx.send(socket).unwrap();
                }
            })
        };

        let router = Router::new().route("/ws", any(ws_handler));

        let address = format!("127.0.0.1:{port}");
        let listener = TcpListener::bind(&address).await.unwrap();
        spawn(async move {
            axum::serve(listener, router.into_make_service())
                .await
                .unwrap();
        });

        let (right, _) = tokio_tungstenite::connect_async(format!("ws://localhost:{port}/ws"))
            .await
            .unwrap();

        let left = rx.await.unwrap();
        let left = WebSocketTransport::new(left, BincodeCodec::default());
        let right = crate::websocket::WebSocketTransport::new(right, BincodeCodec::default());

        (left, right)
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_transports(8400).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_transports(8401).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_stream() {
        let (left, right) = create_transports(8402).await;
        dnet_tests::test_stream(left, right).await;
    }
}
