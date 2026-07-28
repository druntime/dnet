//! Transport for communication over [Tokio](https://tokio.rs/) UDP implementation.
//!
//! **NOTE**: This transport inherits UDP properties:
//! - it is **unreliable** - messages are not guaranteed to reach destination,
//! - it is **unordered** - messages may arrive at destination out of order, also they
//!   may be duplicated (the same message may arrive at destination twice or more times).
//! - message size is limited to datagram size - sending may result in error if encoded
//!   message is too large.
//!
//! ## Example
//!
//! ```ignore
//! let udp_socket = UdpSocket::bind("127.0.0.1:8080").await?;
//! udp_socket.connect(remote_address).await?;
//!
//! let mut transport: UdpTransport<_, _, i32, String> =
//!     UdpTransport::new(udp_socket, BincodeCodec::default());
//!
//! let integer = transport.receive().await?;
//! transport.send("Hello World!".to_string()).await?;
//! ```

use std::{
    borrow::Borrow,
    collections::VecDeque,
    fmt::{Debug, Display},
    io::ErrorKind,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::poll_fn, stream::FusedStream, Sink, SinkExt, Stream};
use pin_project::pin_project;
use serde::Serialize;
use tokio::{
    io::ReadBuf,
    net::{ToSocketAddrs, UdpSocket},
};

use dnet_base::{Decode, Encode};

/// UDP transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Failed to send message - not all bytes were sent.
    SendingError,

    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),

    /// IO error occurred.
    IoError(tokio::io::Error),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SendingError => write!(f, "not all bytes were sent"),
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
            Error::IoError(error) => write!(f, "IO error occurred: {error}"),
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

/// Transport over [Tokio](https://tokio.rs/)'s UDP implementation.
///
/// Wraps over [tokio::net::UdpSocket].
///
/// **NOTE**: This transport inherits UDP properties:
/// - it is **unreliable** - messages are NOT guaranteed to reach destination,
/// - it is **unordered** - messages may arrive at destination out of order, also they
/// may be duplicated (the same message may arrive at destination twice or more times).
/// - message size is limited to datagram size - sending may result in error if encoded
/// message is too large.
#[pin_project]
pub struct UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    udp_socket: Option<U>,
    codec: Codec,
    send_queue: VecDeque<Outgoing>,
    send_buffer: Vec<u8>,
    message_pending: bool,
    receive_buffer: Vec<u8>,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
}

impl<U, Codec, Incoming, Outgoing> UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided `[tokio::net::UdpSocket]`.
    pub fn new(udp_socket: U, codec: Codec) -> Self {
        UdpTransport {
            udp_socket: Some(udp_socket),
            codec,
            send_queue: VecDeque::new(),
            send_buffer: vec![],
            message_pending: false,
            receive_buffer: vec![0; 65536],

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
        }
    }

    /// Send message to address.
    pub async fn send_to<A: ToSocketAddrs>(
        &mut self,
        message: Outgoing,
        target: A,
    ) -> Result<(), crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>> {
        self.flush().await?;

        let result = if let Some(udp_socket) = &self.udp_socket {
            self.send_buffer.clear();
            self.codec
                .encode(&mut self.send_buffer, &message)
                .map_err(
                    Error::<<Codec as Encode>::Error, <Codec as Decode>::Error>::SerializationError,
                )
                .map_err(crate::Error::Other)?;

            udp_socket
                .borrow()
                .send_to(&self.send_buffer, target)
                .await
                .map_err(Error::<<Codec as Encode>::Error, <Codec as Decode>::Error>::IoError)
                .map_err(crate::Error::Other)?;
            Ok(())
        } else {
            Err(crate::Error::Closed)
        };

        #[cfg(feature = "logging")]
        self.logger
            .log_sending::<Outgoing, _>(&result, Some(self.send_buffer.len()));

        result
    }

    /// Receive single message.
    ///
    /// Returns a pair of incoming message and its origin address.
    pub async fn receive_from(
        &mut self,
    ) -> Result<
        (Incoming, SocketAddr),
        crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>,
    > {
        let mut message_length = 0;
        let result = if self.udp_socket.is_some() {
            let result = poll_fn(|cx| self.poll_recv_from(cx, &mut message_length)).await;
            if let Some(result) = result {
                result.map_err(crate::Error::Other)
            } else {
                Err(crate::Error::Closed)
            }
        } else {
            Err(crate::Error::Closed)
        };

        #[cfg(feature = "logging")]
        match &result {
            Ok((item, _address)) => self
                .logger
                .log_receiving_success(item, Some(message_length)),
            Err(error) => self.logger.log_receiving_failure(error),
        }

        result
    }

    #[allow(clippy::type_complexity)]
    fn poll_recv_from(
        &mut self,
        cx: &mut Context<'_>,
        message_length: &mut usize,
    ) -> Poll<
        Option<
            Result<
                (Incoming, SocketAddr),
                Error<<Codec as Encode>::Error, <Codec as Decode>::Error>,
            >,
        >,
    > {
        if let Some(udp_socket) = &self.udp_socket {
            let mut buf = ReadBuf::new(&mut self.receive_buffer);
            match udp_socket.borrow().poll_recv_from(cx, &mut buf) {
                Poll::Ready(result) => match result {
                    Ok(address) => {
                        let filled = buf.filled();
                        *message_length = filled.len();
                        let result: Result<Incoming, _> = self.codec.decode(filled);
                        match result {
                            Ok(message) => Poll::Ready(Some(Ok((message, address)))),
                            Err(error) => {
                                Poll::Ready(Some(Err(Error::DeserializationError(error))))
                            }
                        }
                    }
                    Err(error) => match error.kind() {
                        ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted => {
                            self.udp_socket = None;
                            Poll::Ready(None)
                        }
                        _ => Poll::Ready(Some(Err(Error::IoError(error)))),
                    },
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl<U, Codec, Incoming, Outgoing> Sink<Outgoing> for UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.borrow().log_ready(&result);

        result
    }

    fn start_send(mut self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let result = if self.udp_socket.is_some() {
            self.send_queue.push_back(item);
            Ok(())
        } else {
            Err(crate::Error::Closed)
        };

        #[cfg(feature = "logging")]
        self.logger
            .borrow()
            .log_message_staging::<Outgoing, _, _>(&result);

        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.project();
        let result = if me.send_queue.is_empty() && !*me.message_pending {
            Poll::Ready(Ok(()))
        } else if let Some(udp_socket) = &me.udp_socket {
            loop {
                if *me.message_pending {
                    let bytes_to_send = me.send_buffer.len();
                    let mut closed = false;
                    let result = udp_socket.borrow().poll_send(cx, me.send_buffer);
                    let result = match result {
                        Poll::Ready(result) => {
                            *me.message_pending = false;
                            me.send_buffer.clear();
                            match result {
                                Ok(bytes_written) => {
                                    if bytes_written != bytes_to_send {
                                        Some(Poll::Ready(Err(crate::Error::Other(
                                            Error::SendingError,
                                        ))))
                                    } else {
                                        None
                                    }
                                }
                                Err(error) => match error.kind() {
                                    ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted => {
                                        closed = true;
                                        Some(Poll::Ready(Err(crate::Error::Closed)))
                                    }
                                    _ => Some(Poll::Ready(Err(crate::Error::Other(
                                        Error::IoError(error),
                                    )))),
                                },
                            }
                        }
                        Poll::Pending => Some(Poll::Pending),
                    };
                    if let Some(result) = result {
                        if closed {
                            *me.udp_socket = None;
                        }

                        #[cfg(feature = "logging")]
                        if let Poll::Ready(result) = &result {
                            me.logger
                                .log_sending::<Outgoing, _>(result, Some(bytes_to_send));
                        }

                        break result;
                    }
                } else if let Some(message) = me.send_queue.pop_front() {
                    let result = me.codec.encode(&mut *me.send_buffer, &message);
                    if let Err(error) = result {
                        me.send_buffer.clear();
                        let error = crate::Error::Other(Error::SerializationError(error));

                        #[cfg(feature = "logging")]
                        me.logger.log_message_preparation_failure(&error);

                        break Poll::Ready(Err(error));
                    } else {
                        #[cfg(feature = "logging")]
                        me.logger.log_message_preparation_success::<Outgoing>(Some(
                            me.send_buffer.len(),
                        ));

                        *me.message_pending = true;
                    }
                } else {
                    break Poll::Ready(Ok(()));
                }
            }
        } else {
            Poll::Ready(Err(crate::Error::Closed))
        };

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = match self.poll_flush_unpin(cx) {
            Poll::Ready(_) => {
                self.udp_socket = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        };

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<U, Codec, Incoming, Outgoing> Stream for UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut message_length = 0;
        let result = match self.poll_recv_from(cx, &mut message_length) {
            Poll::Ready(result) => {
                let result = result.map(|result| result.map(|(incoming, _)| incoming));
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        };

        #[cfg(feature = "logging")]
        self.logger.log_receiving(&result, Some(message_length));

        result
    }
}

impl<U, Codec, Incoming, Outgoing> FusedStream for UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.udp_socket.is_none()
    }
}

#[cfg(feature = "logging")]
impl<U, Codec, Incoming, Outgoing> dnet_base::Logging for UdpTransport<U, Codec, Incoming, Outgoing>
where
    U: Borrow<UdpSocket>,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "UDP";

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
    // Note that were testing unreliable transport here - so technically speaking it's possible
    // for all tests to fail and for the implementation to be valid at the same time - we're
    // assuming zero packet loss here.

    use dnet_codecs::BincodeCodec;
    use serde::{Deserialize, Serialize};
    use tokio::net::UdpSocket;

    use crate::udp::UdpTransport;

    async fn create_transports<I, O>(
        port_1: u16,
        port_2: u16,
    ) -> (
        UdpTransport<UdpSocket, BincodeCodec, I, O>,
        UdpTransport<UdpSocket, BincodeCodec, O, I>,
    )
    where
        I: Serialize,
        for<'de> I: Deserialize<'de>,
        O: Serialize,
        for<'de> O: Deserialize<'de>,
    {
        let address_1 = format!("127.0.0.1:{port_1}");
        let address_2 = format!("127.0.0.1:{port_2}");

        let left = UdpSocket::bind(address_1).await.unwrap();
        let right = UdpSocket::bind(address_2).await.unwrap();

        left.connect(right.local_addr().unwrap()).await.unwrap();
        right.connect(left.local_addr().unwrap()).await.unwrap();

        let left = UdpTransport::new(left, BincodeCodec::default());
        let right = UdpTransport::new(right, BincodeCodec::default());

        (left, right)
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_transports(8085, 8086).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_transports(8087, 8088).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    #[ignore = "not working - you can't reliably detect UDP socket closing on other side"]
    async fn test_stream() {
        let (left, right) = create_transports(8089, 8090).await;
        dnet_tests::test_stream(left, right).await;
    }
}
