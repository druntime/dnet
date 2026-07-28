//! Transport for communication over [Quinn](https://github.com/quinn-rs/quinn)
//! QUIC implementation.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, Join};

use crate::io::{
    length_delimited::{self, DEFAULT_MAX_MESSAGE_LENGTH},
    Buffered, Pending, Void,
};

pub mod framed;
pub use framed::QuicFramedTransport;

pub mod unreliable;
pub use unreliable::QuicUnreliableTransport;

/// Length-delimited QUIC transport error.
pub type Error<Codec> = length_delimited::Error<<Codec as Encode>::Error, <Codec as Decode>::Error>;

/// Unidirectional receiving transport.
///
/// This transport can only receive messages.
/// Sending messages will succeed but will not have any effect - they will be sent into the void.
pub type UnidirectionalReceive = Join<RecvStream, Void>;

/// Unidirectional sending transport.
///
/// This transport can only send messages.
/// Note: attempt to receive a message will never complete.
pub type UnidirectionalSend = Join<Pending, SendStream>;

/// Bidirectional transport.
pub type Bidirectional = Join<RecvStream, SendStream>;

/// Wrapper for messages sent over QUIC transport.
#[derive(Debug, Serialize, Deserialize)]
pub enum Wrapper<T> {
    /// Open message - sent when a new transport is created to signal
    /// the other side that the transport is ready to be used.
    Open,

    /// Regular message.
    Message(T),
}

/// Length-delimited QUIC transport.
///
/// See [length_delimited] module for more details on the used framing method.
#[pin_project]
pub struct QuicTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: QuicFramedTransport<T, length_delimited::Codec<Codec>, Incoming, Outgoing>,
}

impl<Codec, Incoming, Outgoing> QuicTransport<Bidirectional, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new length-delimited QUIC transport wrapping a provided [RecvStream] and [SendStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn new(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::new_with_max_message_length(
            send_stream,
            recv_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new length-delimited QUIC transport wrapping a provided [RecvStream] and [SendStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn new_with_max_message_length(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::new(
            send_stream,
            recv_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing> QuicTransport<UnidirectionalSend, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new [unidirectional](UnidirectionalSend) length-delimited QUIC transport wrapping a provided [SendStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending messages of larger size will result in [Error::MessageTooLong].
    pub async fn unidirectional_send(
        send_stream: SendStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::unidirectional_send_with_max_message_length(
            send_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new [unidirectional](UnidirectionalSend) length-delimited QUIC transport wrapping a provided [SendStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending messages of larger size will result in [Error::MessageTooLong].
    pub async fn unidirectional_send_with_max_message_length(
        send_stream: SendStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::unidirectional_send(
            send_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing> QuicTransport<UnidirectionalReceive, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new [unidirectional](UnidirectionalReceive) length-delimited QUIC transport wrapping a provided [RecvStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].
    /// Receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn unidirectional_receive(
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::unidirectional_receive_with_max_message_length(
            recv_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new [unidirectional](UnidirectionalReceive) length-delimited QUIC transport wrapping a provided [RecvStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn unidirectional_receive_with_max_message_length(
        recv_stream: RecvStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::unidirectional_receive(
            recv_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing> QuicTransport<Buffered<Bidirectional>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered length-delimited QUIC transport wrapping a provided [RecvStream] and [SendStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::buffered_with_max_message_length(
            send_stream,
            recv_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new buffered length-delimited QUIC transport wrapping a provided [RecvStream] and [SendStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered_with_max_message_length(
        send_stream: SendStream,
        recv_stream: RecvStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::buffered(
            send_stream,
            recv_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicTransport<Buffered<UnidirectionalSend>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered [unidirectional](UnidirectionalSend) length-delimited QUIC transport wrapping a provided [SendStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered_unidirectional_send(
        send_stream: SendStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::buffered_unidirectional_send_with_max_message_length(
            send_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new buffered [unidirectional](UnidirectionalSend) length-delimited QUIC transport wrapping a provided [SendStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered_unidirectional_send_with_max_message_length(
        send_stream: SendStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::buffered_unidirectional_send(
            send_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing>
    QuicTransport<Buffered<UnidirectionalReceive>, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new buffered [unidirectional](UnidirectionalReceive) length-delimited QUIC transport wrapping a provided [RecvStream].
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered_unidirectional_receive(
        recv_stream: RecvStream,
        codec: Codec,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        QuicTransport::buffered_unidirectional_receive_with_max_message_length(
            recv_stream,
            codec,
            DEFAULT_MAX_MESSAGE_LENGTH,
        )
        .await
    }

    /// Create new buffered [unidirectional](UnidirectionalReceive) length-delimited QUIC transport wrapping a provided [RecvStream].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Receiving messages of larger size will result in [Error::MessageTooLong].
    pub async fn buffered_unidirectional_receive_with_max_message_length(
        recv_stream: RecvStream,
        codec: Codec,
        max_message_length: u32,
    ) -> Result<Self, crate::Error<Error<Codec>>> {
        let inner = QuicFramedTransport::buffered_unidirectional_receive(
            recv_stream,
            length_delimited::Codec::new(codec, max_message_length),
        )
        .await
        .map_err(length_delimited::map_error)?;
        Ok(QuicTransport { inner })
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing> for QuicTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Error = crate::Error<Error<Codec>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(length_delimited::map_error)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(item)
            .map_err(length_delimited::map_error)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(length_delimited::map_error)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_close(cx)
            .map_err(length_delimited::map_error)
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for QuicTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    type Item = Result<
        Incoming,
        length_delimited::Error<<Codec as Encode>::Error, <Codec as Decode>::Error>,
    >;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .inner
            .poll_next(cx)
            .map_err(length_delimited::map_error_inner)
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for QuicTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for QuicTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "QUIC";

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
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    use dnet_codecs::BincodeCodec;
    use futures::join;
    use quinn::{rustls::pki_types::CertificateDer, ClientConfig, Endpoint, ServerConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::{pki_types::PrivatePkcs8KeyDer, RootCertStore};
    use serde::{Deserialize, Serialize};
    use tokio::spawn;

    use crate::quic::{Bidirectional, QuicTransport};

    async fn create_transports<I, O>(
        port: u16,
    ) -> (
        QuicTransport<Bidirectional, BincodeCodec, I, O>,
        QuicTransport<Bidirectional, BincodeCodec, O, I>,
    )
    where
        I: Serialize + Send + 'static,
        for<'de> I: Deserialize<'de>,
        O: Serialize + Send + 'static,
        for<'de> O: Deserialize<'de>,
    {
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

            let connection = incoming.await.unwrap();

            let (send_stream, recv_stream) = connection.accept_bi().await.unwrap();

            QuicTransport::new(send_stream, recv_stream, BincodeCodec::default())
                .await
                .unwrap()
        });

        let client = spawn(async move {
            let mut certificates = RootCertStore::empty();
            certificates.add(certificate_der).unwrap();
            let config = ClientConfig::with_root_certificates(Arc::new(certificates)).unwrap();

            let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(config);

            let connecting = endpoint.connect(server_address, "localhost").unwrap();

            let connection = connecting.await.unwrap();

            let (send_stream, recv_stream) = connection.open_bi().await.unwrap();

            QuicTransport::new(send_stream, recv_stream, BincodeCodec::default())
                .await
                .unwrap()
        });

        let (left, right) = join!(server, client);
        let left = left.unwrap();
        let right = right.unwrap();

        (left, right)
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_transports(8180).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_transports(8181).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_stream() {
        let (left, right) = create_transports(8182).await;
        dnet_tests::test_stream(left, right).await;
    }
}
