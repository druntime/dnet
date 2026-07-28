//! Communication over transports implementing [Tokio](https://tokio.rs/)'s
//! [AsyncRead] and [AsyncWrite] traits.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[allow(unused_imports)]
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadBuf, Sink};

pub mod framed;
pub use framed::FramedTransport;

pub mod length_delimited;
pub use length_delimited::LengthDelimitedTransport;

/// Buffered transport.
pub type Buffered<T> = BufReader<BufWriter<T>>;

/// [AsyncWrite] implementation that sends data to the void.
pub type Void = Sink;

/// [AsyncRead] implementation that never produces any data.
pub struct Pending;

impl AsyncRead for Pending {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}
