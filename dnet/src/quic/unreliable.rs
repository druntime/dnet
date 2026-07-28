//! Unreliable QUIC transport.

use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{BufMut, BytesMut};
use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, FutureExt, Sink, Stream};
use pin_project::{pin_project, pinned_drop};
use quinn::{Connection, ReadDatagram, SendDatagram, SendDatagramError, VarInt};
use serde::Serialize;

/// QUIC unreliable transport error.
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// Failed to send message - not all bytes were sent.
    SendingError(SendDatagramError),

    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SendingError(error) => write!(f, "failed to send datagram: {error}"),
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
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

/// Configuration of unreliable QUIC transport.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Configuration {
    /// Use [Connection::send_datagram_wait] to send messages instead of [Connection::send_datagram].
    ///
    /// Default is `false`.
    pub wait_after_send: bool,

    /// Close connection on drop or closing of the transport.
    ///
    /// Default is `false`.
    pub close_connection: bool,
}

/// Unreliable QUIC transport.
///
/// Wraps over reference to [quinn::Connection].
/// Sends/receives messages using [Connection::send_datagram] (or [Connection::send_datagram_wait])
/// and [Connection::read_datagram] methods.
///
/// **NOTE**: This transport inherits QUIC's datagram properties:
/// - it is **unreliable** - messages are NOT guaranteed to reach destination,
/// - it is **unordered** - messages may arrive at destination out of order, also they
/// may be duplicated (the same message may arrive at destination twice or more times).
/// - message size is limited to datagram size - sending may result in error if encoded
/// message is too large.
#[pin_project(PinnedDrop)]
pub struct QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    connection: Option<&'a Connection>,
    send_queue: VecDeque<Outgoing>,
    send_buffer: BytesMut,
    send_datagram_future: Option<Pin<Box<SendDatagram<'a>>>>,
    read_datagram_future: Option<Pin<Box<ReadDatagram<'a>>>>,
    codec: Codec,
    configuration: Configuration,

    #[cfg(feature = "logging")]
    logger: dnet_base::Logger,

    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<'a, Codec, Incoming, Outgoing> QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new unreliable QUIC transport wrapping provided [Connection] reference.
    pub fn new(connection: &'a Connection, codec: Codec, configuration: Configuration) -> Self {
        QuicUnreliableTransport {
            connection: Some(connection),
            send_queue: VecDeque::new(),
            send_buffer: BytesMut::new(),
            send_datagram_future: None,
            read_datagram_future: None,
            codec,
            configuration,

            #[cfg(feature = "logging")]
            logger: dnet_base::Logger::new::<Self>(),

            _incoming: PhantomData,
            _outgoing: PhantomData,
        }
    }
}

impl<'a, Codec, Incoming, Outgoing> Sink<Outgoing>
    for QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let me = self.project();
        if let Some(connection) = me.connection {
            if me.configuration.wait_after_send {
                me.send_queue.push_back(item);

                #[cfg(feature = "logging")]
                me.logger.log_message_staging_success::<Outgoing>();

                Ok(())
            } else {
                me.send_buffer.clear();
                let result = me
                    .codec
                    .encode(me.send_buffer.writer(), &item)
                    .map_err(|error| crate::Error::Other(Error::SerializationError(error)))
                    .and_then(|_| {
                        connection
                            .send_datagram(me.send_buffer.split().freeze())
                            .map_err(|error| crate::Error::Other(Error::SendingError(error)))
                    });

                #[cfg(feature = "logging")]
                me.logger
                    .log_sending::<Outgoing, _>(&result, Some(me.send_buffer.len()));

                result
            }
        } else {
            let error = crate::Error::Closed;

            #[cfg(feature = "logging")]
            if me.configuration.wait_after_send {
                me.logger.log_message_staging_failure(&error);
            } else {
                me.logger.log_sending_failure(&error);
            }

            Err(error)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut me = self.project();
        let result = if me.configuration.wait_after_send {
            loop {
                let Some(connection) = me.connection else {
                    break Poll::Ready(Err(crate::Error::Closed));
                };
                if let Some(send_datagram_future) = &mut me.send_datagram_future {
                    match send_datagram_future.poll_unpin(cx) {
                        Poll::Ready(result) => {
                            *me.send_datagram_future = None;
                            if let Err(error) = result {
                                let error = crate::Error::Other(Error::SendingError(error));

                                #[cfg(feature = "logging")]
                                me.logger.log_sending_failure(&error);

                                break Poll::Ready(Err(error));
                            }
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                } else if let Some(message) = me.send_queue.pop_front() {
                    me.send_buffer.clear();
                    let result = me.codec.encode(me.send_buffer.writer(), &message);
                    if let Err(error) = result {
                        let error = crate::Error::Other(Error::SerializationError(error));

                        #[cfg(feature = "logging")]
                        me.logger.log_message_preparation_failure(&error);

                        break Poll::Ready(Err(error));
                    }

                    #[cfg(feature = "logging")]
                    me.logger
                        .log_message_preparation_success::<Outgoing>(Some(me.send_buffer.len()));

                    *me.send_datagram_future = Some(Box::pin(
                        connection.send_datagram_wait(me.send_buffer.split().freeze()),
                    ));
                } else {
                    break Poll::Ready(Ok(()));
                }
            }
        } else {
            Poll::Ready(Ok(()))
        };

        #[cfg(feature = "logging")]
        me.logger.log_flush(&result);

        result
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.configuration.close_connection {
            if let Some(connection) = self.connection {
                connection.close(VarInt::from_u32(0), &[]);
            }
        }
        self.connection = None;
        self.send_datagram_future = None;
        self.read_datagram_future = None;
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.log_close(&result);

        result
    }
}

impl<'a, Codec, Incoming, Outgoing> Stream
    for QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<Incoming, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #[cfg(feature = "logging")]
        let mut message_length = 0;

        let result = if let Some(connection) = self.connection {
            let read_datagram_future = self
                .read_datagram_future
                .get_or_insert_with(|| Box::pin(connection.read_datagram()));
            match read_datagram_future.poll_unpin(cx) {
                Poll::Ready(result) => {
                    self.read_datagram_future = None;
                    match result {
                        Ok(bytes) => {
                            #[cfg(feature = "logging")]
                            {
                                message_length = bytes.len();
                            }

                            let result: Result<Incoming, _> = self.codec.decode(&*bytes);
                            match result {
                                Ok(message) => Poll::Ready(Some(Ok(message))),
                                Err(error) => {
                                    Poll::Ready(Some(Err(Error::DeserializationError(error))))
                                }
                            }
                        }
                        Err(_) => Poll::Ready(None),
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        };

        #[cfg(feature = "logging")]
        self.logger.log_receiving(&result, Some(message_length));

        result
    }
}

impl<'a, Codec, Incoming, Outgoing> FusedStream
    for QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.connection.is_none()
    }
}

#[pinned_drop]
impl<'a, Codec, Incoming, Outgoing> PinnedDrop
    for QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn drop(self: Pin<&mut Self>) {
        if self.configuration.close_connection {
            if let Some(connection) = self.connection {
                connection.close(VarInt::from_u32(0), &[]);
            }
        }
    }
}

#[cfg(feature = "logging")]
impl<'a, Codec, Incoming, Outgoing> dnet_base::Logging
    for QuicUnreliableTransport<'a, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "QUIC(unreliable)";

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
    // Note that we're testing unreliable transport here - so technically speaking it's possible
    // for all tests to fail and for the implementation to be valid at the same time - we're
    // assuming zero packet loss here.

    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    use dnet_codecs::BincodeCodec;
    use futures::join;
    use quinn::{
        rustls::pki_types::CertificateDer, ClientConfig, Connection, Endpoint, ServerConfig,
    };
    use rcgen::generate_simple_self_signed;
    use rustls::{pki_types::PrivatePkcs8KeyDer, RootCertStore};
    use serde::{Deserialize, Serialize};
    use tokio::spawn;

    use crate::quic::{unreliable::Configuration, QuicUnreliableTransport};

    async fn create_connections(port: u16) -> (Connection, Connection) {
        let server_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let certificate = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate_der = CertificateDer::from(certificate.cert.clone());

        let certificate_der_clone = CertificateDer::from(certificate.cert.clone());
        let server = spawn(async move {
            let private_key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());

            let mut server_config =
                ServerConfig::with_single_cert(vec![certificate_der_clone], private_key.into())
                    .unwrap();
            let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
            transport_config.max_concurrent_uni_streams(0_u8.into());

            let endpoint = Endpoint::server(server_config, server_address).unwrap();

            let incoming = endpoint.accept().await.unwrap();

            incoming.await.unwrap()
        });

        let client = spawn(async move {
            let mut certificates = RootCertStore::empty();
            certificates.add(certificate_der).unwrap();
            let config = ClientConfig::with_root_certificates(Arc::new(certificates)).unwrap();

            let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(config);

            let connecting = endpoint.connect(server_address, "localhost").unwrap();

            connecting.await.unwrap()
        });

        let (server, client) = join!(server, client);
        let server = server.unwrap();
        let client = client.unwrap();

        (server, client)
    }

    fn create_transports<'a, I, O>(
        server_connection: &'a Connection,
        client_connection: &'a Connection,
        configuration: Configuration,
    ) -> (
        QuicUnreliableTransport<'a, BincodeCodec, I, O>,
        QuicUnreliableTransport<'a, BincodeCodec, O, I>,
    )
    where
        I: Serialize + Send + 'static,
        for<'de> I: Deserialize<'de>,
        O: Serialize + Send + 'static,
        for<'de> O: Deserialize<'de>,
    {
        (
            QuicUnreliableTransport::new(server_connection, Default::default(), configuration),
            QuicUnreliableTransport::new(client_connection, Default::default(), configuration),
        )
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_connections(8280).await;
        let (left, right) = create_transports(&left, &right, Default::default());
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_transport_waiting() {
        let (left, right) = create_connections(8281).await;
        let (left, right) = create_transports(
            &left,
            &right,
            Configuration {
                wait_after_send: true,
                close_connection: true,
            },
        );
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_connections(8282).await;
        let (left, right) = create_transports(&left, &right, Default::default());
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message_waiting() {
        let (left, right) = create_connections(8283).await;
        let (left, right) = create_transports(
            &left,
            &right,
            Configuration {
                wait_after_send: true,
                close_connection: true,
            },
        );
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_stream() {
        let (left, right) = create_connections(8284).await;
        let (left, right) = create_transports(
            &left,
            &right,
            Configuration {
                wait_after_send: true,
                close_connection: true,
            },
        );
        // we're waiting for a bit before dropping the sending transport (and therefore
        // closing the connection) to let messages pass to the other side
        dnet_tests::test_stream_with_sleep_before_drop(left, right, Duration::from_millis(10))
            .await;
    }
}
