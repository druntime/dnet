//! Transport for communication over [Tokio](https://tokio.rs/) TCP implementation.
//!
//! ## Example
//!
//! ```ignore
//! let tcp_stream = TcpStream::connect("127.0.0.1:8080").await?;
//!
//! let mut transport: TcpTransport<_, _, i32, String> =
//!     TcpTransport::new(tcp_stream, BincodeCodec::default());
//!
//! let integer = transport.receive().await?;
//! transport.send("Hello World!".to_string()).await?;
//! ```

pub mod framed;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use dnet_base::{Decode, Encode};
pub use framed::TcpFramedTransport;

use futures::{stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

use crate::io::{
    length_delimited::{self, codec, DEFAULT_MAX_MESSAGE_LENGTH},
    Buffered,
};

/// Length-delimited TCP transport error.
pub type Error<Codec> = length_delimited::Error<<Codec as Encode>::Error, <Codec as Decode>::Error>;

/// Length-delimited transport for communication over
/// [Tokio](https://tokio.rs/)'s TCP implementation.
///
/// See also: [LengthDelimitedTransport].
#[pin_project]
pub struct TcpTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    #[pin]
    inner: TcpFramedTransport<T, length_delimited::Codec<Codec>, Incoming, Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> TcpTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided TCP stream.
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn new(tcp_stream: T, codec: Codec) -> Self {
        TcpTransport::new_with_max_message_length(tcp_stream, codec, DEFAULT_MAX_MESSAGE_LENGTH)
    }

    /// Create new transport wrapping a provided TCP stream.
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn new_with_max_message_length(
        tcp_stream: T,
        codec: Codec,
        max_message_length: u32,
    ) -> Self {
        let codec = codec::Codec::new(codec, max_message_length);

        #[allow(unused_mut)]
        let mut inner = TcpFramedTransport::new(tcp_stream, codec);

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            inner.with_logger_mut(|logger| logger.override_kind::<Self>());
        }

        TcpTransport { inner }
    }
}

impl<T, Codec, Incoming, Outgoing> TcpTransport<Buffered<T>, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    /// Create new transport wrapping a provided TCP stream.
    ///
    /// **NOTE**: By default serialized message size is limited to [DEFAULT_MAX_MESSAGE_LENGTH].<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn buffered(tcp_stream: T, codec: Codec) -> Self {
        Self::buffered_with_max_message_length(tcp_stream, codec, DEFAULT_MAX_MESSAGE_LENGTH)
    }

    /// Create new buffered transport wrapping a provided struct implementing
    /// [AsyncRead] and [AsyncWrite].
    ///
    /// Serialized message size will be limited to `max_message_length`.<br>
    /// Sending or receiving messages of larger size will result in [Error::MessageTooLong].
    pub fn buffered_with_max_message_length(
        tcp_stream: T,
        codec: Codec,
        max_message_length: u32,
    ) -> Self {
        Self::new_with_max_message_length(
            BufReader::new(BufWriter::new(tcp_stream)),
            codec,
            max_message_length,
        )
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing> for TcpTransport<T, Codec, Incoming, Outgoing>
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

impl<T, Codec, Incoming, Outgoing> Stream for TcpTransport<T, Codec, Incoming, Outgoing>
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

impl<T, Codec, Incoming, Outgoing> FusedStream for TcpTransport<T, Codec, Incoming, Outgoing>
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
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging for TcpTransport<T, Codec, Incoming, Outgoing>
where
    T: AsyncRead + AsyncWrite,
    Codec: crate::Codec,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    Outgoing: Serialize,
{
    const KIND: &'static str = "TCP";

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
    use std::time::Duration;

    use futures::{select, FutureExt, SinkExt};
    use serde::{Deserialize, Serialize};
    use tokio::{
        net::{TcpListener, TcpStream},
        time::sleep,
    };

    use crate::{codecs::BincodeCodec, io::Buffered, tcp::TcpTransport, Receive};

    async fn create_transports<I, O>(
        port: u16,
    ) -> (
        TcpTransport<Buffered<TcpStream>, BincodeCodec, I, O>,
        TcpTransport<Buffered<TcpStream>, BincodeCodec, O, I>,
    )
    where
        I: Serialize,
        for<'de> I: Deserialize<'de>,
        O: Serialize,
        for<'de> O: Deserialize<'de>,
    {
        let address = format!("127.0.0.1:{port}");

        let left = TcpListener::bind(&address).await.unwrap();
        let right = TcpStream::connect(&address).await.unwrap();

        let (left, _) = left.accept().await.unwrap();

        let left = TcpTransport::buffered(left, BincodeCodec::default());
        let right = TcpTransport::buffered(right, BincodeCodec::default());

        (left, right)
    }

    #[tokio::test]
    async fn test_transport() {
        let (left, right) = create_transports(8080).await;
        dnet_tests::test_transport(left, right).await;
    }

    #[tokio::test]
    async fn test_unit_message() {
        let (left, right) = create_transports(8081).await;
        dnet_tests::test_unit_message(left, right).await;
    }

    #[tokio::test]
    async fn test_stream() {
        let (left, right) = create_transports(8082).await;
        dnet_tests::test_stream(left, right).await;
    }

    #[tokio::test]
    async fn test_feed() {
        let (mut left, mut right) = create_transports(8083).await;

        dnet_tests::init_logging(&mut left, &mut right);

        left.feed(1).await.unwrap();
        left.feed(2).await.unwrap();
        left.feed(3).await.unwrap();

        right.feed('a').await.unwrap();
        right.feed('b').await.unwrap();
        right.feed('c').await.unwrap();

        left.flush().await.unwrap();
        right.flush().await.unwrap();

        assert_eq!(right.receive().await.unwrap(), 1);
        assert_eq!(right.receive().await.unwrap(), 2);
        assert_eq!(right.receive().await.unwrap(), 3);

        assert_eq!(left.receive().await.unwrap(), 'a');
        assert_eq!(left.receive().await.unwrap(), 'b');
        assert_eq!(left.receive().await.unwrap(), 'c');

        left.feed(4).await.unwrap();
        select! {
            _rec = right.receive() => { panic!("got without flushing!") }
            _sleep = sleep(Duration::from_millis(10)).fuse() => {}
        }

        left.flush().await.unwrap();
        assert_eq!(right.receive().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_size_limit() {
        let left = TcpListener::bind("127.0.0.1:1234").await.unwrap();
        let right = TcpStream::connect("127.0.0.1:1234").await.unwrap();

        let (left, _) = left.accept().await.unwrap();

        let mut left: TcpTransport<_, _, String, String> =
            TcpTransport::buffered_with_max_message_length(left, BincodeCodec::default(), 15);
        let mut right: TcpTransport<_, _, String, String> =
            TcpTransport::buffered(right, BincodeCodec::default());

        dnet_tests::init_logging(&mut left, &mut right);

        left.send("Hey".to_string()).await.unwrap();
        assert!(matches!(
            left.send("Hello, hello, hello".to_string()).await,
            Err(crate::Error::Other(
                crate::io::length_delimited::Error::MessageTooLong
            ))
        ));
        left.send("Hi".to_string()).await.unwrap();

        assert_eq!(right.receive().await.unwrap(), "Hey");
        assert_eq!(right.receive().await.unwrap(), "Hi");

        right.send("Hey".to_string()).await.unwrap();
        for _i in 0..139 {
            right.send("Hello, hello, hello".to_string()).await.unwrap();
        }
        right.send("Hi".to_string()).await.unwrap();

        assert_eq!(left.receive().await.unwrap(), "Hey");
        for _i in 0..139 {
            assert!(matches!(
                left.receive().await,
                Err(crate::Error::Other(
                    crate::io::length_delimited::Error::MessageTooLong
                ))
            ));
        }
        assert_eq!(left.receive().await.unwrap(), "Hi");

        right.send("Hey".to_string()).await.unwrap();
        for _i in 0..17 {
            right.send("Hello, hello, hello".to_string()).await.unwrap();
            right
                .send("Hello, hello, hello, hi".to_string())
                .await
                .unwrap();
        }
        right.send("Hi".to_string()).await.unwrap();

        assert_eq!(left.receive().await.unwrap(), "Hey");
        for _i in 0..17 {
            assert!(matches!(
                left.receive().await,
                Err(crate::Error::Other(
                    crate::io::length_delimited::Error::MessageTooLong
                ))
            ));
            assert!(matches!(
                left.receive().await,
                Err(crate::Error::Other(
                    crate::io::length_delimited::Error::MessageTooLong
                ))
            ));
        }
        assert_eq!(left.receive().await.unwrap(), "Hi");
    }
}
