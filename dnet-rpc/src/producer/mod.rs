//! Producer related functionality.

pub mod abortable;

use std::time::Duration;

use dportable::JoinHandle;
use futures::future::pending;
use serde::{Deserialize, Serialize};

use crate::consumer;

use super::ShutdownType;

/// Error handler for errors that may occur while sending or receiving
/// messages through transport.
pub type ErrorHandler<Response, Error> = dnet_utils::pipe::ErrorHandler<Message<Response>, Error>;

/// Helper trait for producer transports.
pub trait Transport<Request, Response, Error>:
    crate::Transport<consumer::Message<Request>, self::Message<Response>, Error> + Unpin
{
}
impl<T, Request, Response, Error> Transport<Request, Response, Error> for T where
    T: crate::Transport<consumer::Message<Request>, self::Message<Response>, Error> + Unpin
{
}

/// Configuration for [produce] method.
///
/// [produce]: self::Produce::produce
pub struct Configuration {
    /// When this future resolves producers stops producing and returns.
    ///
    /// **NOTE**: By default it is set with [futures::future::Pending], but it doesn't
    /// mean producer will never stop - it will still stop when transport is closed,
    /// or it can be stopped by provided `send_error_callback` or `receive_error_callback`.
    ///
    /// This future is intended for triggering shutdown manually.
    pub shutdown: Box<dyn crate::Shutdown>,

    /// Optional duration after which producer will shutdown if it will not receive
    /// any messages during that period (and there are no requests pending).
    ///
    /// **NOTE**: It resets on every message received - so a producer that has received messages
    /// in the past can still time out when a period of `duration` length with no further messages
    /// occurs.
    pub timeout: Option<Duration>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            shutdown: Box::new(pending()),
            timeout: Default::default(),
        }
    }
}

/// Producer message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message<Response> {
    /// Response to request.
    Response {
        /// Request id.
        id: u64,

        /// Response value.
        response: Response,
    },

    /// Producer was shutdown with [ShutdownType::Aborted].
    Aborted,

    /// Producer was shutdown with [ShutdownType::Shutdown].
    Shutdown,
}

/// Stream response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamResponse<T> {
    /// Stream open.
    Open,

    /// Next stream item.
    Item(T),

    /// Stream closed (has no more items).
    Closed,
}

#[cfg(target_arch = "wasm32")]
mod transferable {
    use std::marker::PhantomData;

    use dnet_base::Codec;
    use dnet_js::{wrapper::WrapperLikeTransferable, IntoTransferable};

    /// Producer message.
    #[derive(Debug, Clone, IntoTransferable)]
    pub enum Message<C, Response>
    where
        Response: WrapperLikeTransferable<C>,
        C: Codec,
    {
        /// Response to request.
        Response {
            id: u64,
            #[into_transferable]
            response: Response,
            _codec: PhantomData<C>,
        },

        /// Producer was shutdown with [ShutdownType::Aborted].
        Aborted,

        /// Producer was shutdown with [ShutdownType::Shutdown].
        Shutdown,
    }

    impl<C, Response> From<super::Message<Response>> for Message<C, Response>
    where
        Response: WrapperLikeTransferable<C>,
        C: Codec,
    {
        fn from(value: super::Message<Response>) -> Self {
            match value {
                super::Message::Response { id, response, .. } => Self::Response {
                    id,
                    response,
                    _codec: PhantomData,
                },
                super::Message::Aborted => Self::Aborted,
                super::Message::Shutdown => Self::Shutdown,
            }
        }
    }

    /// Stream response wrapper.
    #[derive(Debug, Clone, IntoTransferable)]
    pub enum StreamResponse<C, T>
    where
        T: WrapperLikeTransferable<C>,
        C: Codec,
    {
        /// Stream open.
        Open,

        /// Next stream item.
        Item {
            #[into_transferable]
            item: T,
            _codec: PhantomData<C>,
        },

        /// Stream closed (has no more items).
        Closed,
    }

    impl<C, T> From<super::StreamResponse<T>> for StreamResponse<C, T>
    where
        T: WrapperLikeTransferable<C>,
        C: Codec,
    {
        fn from(value: super::StreamResponse<T>) -> Self {
            match value {
                super::StreamResponse::Open => Self::Open,
                super::StreamResponse::Item(item) => Self::Item {
                    item,
                    _codec: PhantomData,
                },
                super::StreamResponse::Closed => Self::Closed,
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, Response> From<transferable::Message<C, Response>> for Message<Response>
where
    Response: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    fn from(value: transferable::Message<C, Response>) -> Self {
        match value {
            transferable::Message::Response { id, response, .. } => Self::Response { id, response },
            transferable::Message::Aborted => Self::Aborted,
            transferable::Message::Shutdown => Self::Shutdown,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, Request>
    dnet_js::IntoTransferable<
        dnet_js::wrapper::Context<C>,
        dnet_js::wrapper::Error<<C as dnet_base::Encode>::Error, <C as dnet_base::Decode>::Error>,
    > for Message<Request>
where
    Request: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    type Output = transferable::MessageWrapper<C, Request>;

    fn into_transferable(self) -> Self::Output {
        transferable::Message::from(self).into_transferable()
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, Request>
    dnet_js::FromTransferable<
        dnet_js::wrapper::Context<C>,
        dnet_js::wrapper::Error<<C as dnet_base::Encode>::Error, <C as dnet_base::Decode>::Error>,
    > for Message<Request>
where
    Request: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    type Input = transferable::MessageWrapper<C, Request>;

    fn from_transferable(input: Self::Input) -> Self {
        use dnet_utils::unwrap::Unwrap;

        input.unwrap().into()
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, T> From<transferable::StreamResponse<C, T>> for StreamResponse<T>
where
    T: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    fn from(value: transferable::StreamResponse<C, T>) -> Self {
        match value {
            transferable::StreamResponse::Open => Self::Open,
            transferable::StreamResponse::Item { item, .. } => Self::Item(item),
            transferable::StreamResponse::Closed => Self::Closed,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, T>
    dnet_js::IntoTransferable<
        dnet_js::wrapper::Context<C>,
        dnet_js::wrapper::Error<<C as dnet_base::Encode>::Error, <C as dnet_base::Decode>::Error>,
    > for StreamResponse<T>
where
    T: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    type Output = transferable::StreamResponseWrapper<C, T>;

    fn into_transferable(self) -> Self::Output {
        transferable::StreamResponse::from(self).into_transferable()
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, T>
    dnet_js::FromTransferable<
        dnet_js::wrapper::Context<C>,
        dnet_js::wrapper::Error<<C as dnet_base::Encode>::Error, <C as dnet_base::Decode>::Error>,
    > for StreamResponse<T>
where
    T: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    type Input = transferable::StreamResponseWrapper<C, T>;

    fn from_transferable(input: Self::Input) -> Self {
        use dnet_utils::unwrap::Unwrap;

        input.unwrap().into()
    }
}

/// Trait implemented by producers.
///
/// You should never have to implement it manually - use derive macro.
pub trait Produce: Sized {
    /// Request message type.
    type Request;
    /// Response message type.
    type Response;

    /// Produce using given transport and configuration.
    fn produce<Transport, Error>(
        self,
        transport: Transport,
        configuration: Configuration,
        error_handler: ErrorHandler<Self::Response, Error>,
    ) -> JoinHandle<ShutdownType>
    where
        Transport: self::Transport<Self::Request, Self::Response, Error>,
        Error: crate::TransportError;
}
