//! Transport for communication over
//! [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
//! while using [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite).

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Sink, SinkExt, Stream, StreamExt};
use native_tls::TlsConnector;
use pin_project::pin_project;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};
use tungstenite::Message;

use crate::{Decode, Encode};

/// WebSocket transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),

    /// Failed to create a TLS connector.
    TlsConnectorError(native_tls::Error),

    /// [Tungstenite](https://github.com/snapview/tokio-tungstenite)-specific error.
    TungsteniteError(tungstenite::Error),
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
            Error::TlsConnectorError(error) => {
                write!(f, "failed to create TLS connector: {error}")
            }
            Error::TungsteniteError(error) => write!(f, "tungstenite error occurred: {error}"),
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

/// Web Socket transport for [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite).
///
/// Wraps around [tokio_tungstenite::WebSocketStream].
///
/// **NOTE**: This transport's receiving stream ignores all non-binary (text, ping, pong, close) messages.
#[pin_project]
pub struct WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
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
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided [tokio_tungstenite::WebSocketStream].
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
        self.inner.start_send_unpin(message).map_err(map_error)
    }
}

impl<Codec, Incoming, Outgoing>
    WebSocketTransport<WebSocketStream<MaybeTlsStream<TcpStream>>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport connecting to given URL.
    pub async fn new_with_address(
        url: &str,
        codec: Codec,
    ) -> Result<Self, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>> {
        let connector = TlsConnector::new().map_err(Error::TlsConnectorError)?;
        let connector = Connector::NativeTls(connector);
        let (web_socket, _) = connect_async_tls_with_config(url, None, false, Some(connector))
            .await
            .map_err(Error::TungsteniteError)?;
        Ok(WebSocketTransport::new(web_socket, codec))
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_ready_unpin(cx).map_err(map_error);

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
        let result = self.inner.poll_flush_unpin(cx).map_err(map_error);

        #[cfg(feature = "logging")]
        self.logger.log_flush(&result);

        result
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.inner.poll_close_unpin(cx).map_err(map_error);

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for WebSocketTransport<T, Codec, Incoming, Outgoing>
where
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
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
                            Message::Binary(message) => {
                                message_length = message.len();
                                let result: Result<Incoming, _> = self.codec.decode(&message[..]);
                                match result {
                                    Ok(message) => Poll::Ready(Some(Ok(message))),
                                    Err(error) => {
                                        Poll::Ready(Some(Err(Error::DeserializationError(error))))
                                    }
                                }
                            }
                            Message::Close(_) => {
                                self.terminated = true;
                                Poll::Ready(None)
                            }
                            _ => Poll::Pending,
                        },
                        Err(error) => match &error {
                            tungstenite::Error::Protocol(
                                tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                            ) => {
                                self.terminated = true;
                                Poll::Ready(None)
                            }
                            _ => Poll::Ready(Some(Err(Error::TungsteniteError(error)))),
                        },
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
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
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
    T: Sink<Message, Error = tungstenite::Error>
        + Stream<Item = Result<Message, tungstenite::Error>>
        + Unpin,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "WebSocket";

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

fn map_error<SerializationError, DeserializationError>(
    tungstenite_error: tungstenite::Error,
) -> crate::Error<Error<SerializationError, DeserializationError>> {
    if matches!(
        tungstenite_error,
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed
    ) {
        crate::Error::Closed
    } else {
        crate::Error::Other(self::Error::TungsteniteError(tungstenite_error))
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;
    use serde::{Deserialize, Serialize};
    use tokio::{
        net::{TcpListener, TcpStream},
        spawn,
    };
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use crate::codecs::JsonCodec;

    use super::WebSocketTransport;

    async fn create_transports<I, O>(
        port: u16,
    ) -> (
        WebSocketTransport<WebSocketStream<TcpStream>, JsonCodec, I, O>,
        WebSocketTransport<WebSocketStream<MaybeTlsStream<TcpStream>>, JsonCodec, O, I>,
    )
    where
        I: Serialize,
        for<'de> I: Deserialize<'de>,
        O: Serialize,
        for<'de> O: Deserialize<'de>,
    {
        let (tx, rx) = oneshot::channel();
        spawn(async move {
            let address = format!("127.0.0.1:{port}");
            let left = TcpListener::bind(&address).await.unwrap();
            let (left, _) = left.accept().await.unwrap();
            let left = tokio_tungstenite::accept_async(left).await.unwrap();
            tx.send(left).unwrap();
        });

        let (right, _) = tokio_tungstenite::connect_async(format!("ws://localhost:{port}/"))
            .await
            .unwrap();

        let left = rx.await.unwrap();
        let left = WebSocketTransport::new(left, JsonCodec::default());
        let right = WebSocketTransport::new(right, JsonCodec::default());

        (left, right)
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_transports(8100).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_transports(8101).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_stream() {
        let (left, right) = create_transports(8102).await;
        dnet_tests::test_stream(left, right).await;
    }
}
