//! Consumer related functionality.

mod value;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{channel::oneshot, future::pending};
use serde::{Deserialize, Serialize};
pub use value::*;

mod stream;
pub use stream::*;

use crate::{
    parts::consumer::{RequestSender, ResultSender},
    producer,
};

#[allow(unused_imports)]
use super::ShutdownType;

/// Error handler for errors that may occur while sending or receiving
/// messages through transport.
pub type ErrorHandler<Request, Error> = dnet_utils::pipe::ErrorHandler<Message<Request>, Error>;

/// Helper trait for consumer transports.
pub trait Transport<Request, Response, Error>:
    crate::Transport<producer::Message<Response>, self::Message<Request>, Error> + Unpin
{
}
impl<T, Request, Response, Error> Transport<Request, Response, Error> for T where
    T: crate::Transport<producer::Message<Response>, self::Message<Request>, Error> + Unpin
{
}

/// Request id.
pub type RequestId = u64;

/// Consumer message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Request> {
    /// Request id.
    pub id: RequestId,

    /// Message payload.
    pub payload: Payload<Request>,
}

/// Consumer message payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload<Request> {
    /// Request arguments.
    Request(Request),

    /// Abort request.
    Abort,
}

#[cfg(target_arch = "wasm32")]
mod transferable {
    use std::marker::PhantomData;

    use dnet_base::Codec;
    use dnet_js::{wrapper::WrapperLikeTransferable, IntoTransferable};

    use crate::consumer::RequestId;

    /// Consumer message.
    #[derive(Debug, Clone, IntoTransferable)]
    pub struct Message<C, Request>
    where
        Request: WrapperLikeTransferable<C>,
        C: Codec,
    {
        /// Request id.
        pub id: RequestId,

        /// Message payload.
        #[into_transferable]
        pub payload: Payload<C, Request>,
    }

    /// Consumer message payload.
    #[derive(Debug, Clone, IntoTransferable)]
    pub enum Payload<C, Request>
    where
        Request: WrapperLikeTransferable<C>,
        C: Codec,
    {
        /// Request arguments.
        Request {
            #[into_transferable]
            request: Request,
            _codec: PhantomData<C>,
        },

        /// Abort request.
        Abort,
    }

    impl<C, Request> From<super::Message<Request>> for Message<C, Request>
    where
        Request: WrapperLikeTransferable<C>,
        C: Codec,
    {
        fn from(value: super::Message<Request>) -> Self {
            Self {
                id: value.id,
                payload: value.payload.into(),
            }
        }
    }

    impl<C, Request> From<super::Payload<Request>> for Payload<C, Request>
    where
        Request: WrapperLikeTransferable<C>,
        C: Codec,
    {
        fn from(value: super::Payload<Request>) -> Self {
            match value {
                super::Payload::Request(request) => Self::Request {
                    request,
                    _codec: PhantomData,
                },
                super::Payload::Abort => Self::Abort,
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, Request> From<transferable::Message<C, Request>> for Message<Request>
where
    Request: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    fn from(value: transferable::Message<C, Request>) -> Self {
        Self {
            id: value.id,
            payload: value.payload.into(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, Request> From<transferable::Payload<C, Request>> for Payload<Request>
where
    Request: dnet_js::wrapper::WrapperLikeTransferable<C>,
    C: dnet_base::Codec,
{
    fn from(value: transferable::Payload<C, Request>) -> Self {
        match value {
            transferable::Payload::Request { request, .. } => Self::Request(request),
            transferable::Payload::Abort => Self::Abort,
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

/// Result type for consumer methods.
pub type Result<T> = std::result::Result<T, super::Error>;

/// Configuration for [consume] method.
///
/// [consume]: self::Consume::consume
pub struct Configuration {
    /// When this future resolves all pending consumer requests error out with
    /// returned [ShutdownType]-related error.
    pub shutdown: Box<dyn crate::Shutdown>,

    /// Consumer timeout - closes used transport if consumer is idle
    /// (there are no pending requests) and consumer is not used for
    /// specified duration.
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

/// Responsible for aborting requests/streams.
#[derive(Debug)]
pub struct Aborter<Request, T> {
    id: RequestId,
    sender: RequestSender<Request, T>,
    abort_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl<Request, Output> Aborter<Request, Output> {
    /// Abort value request/stream request/stream.
    ///
    /// **NOTE** regarding streams: Values that were already received from the producer
    /// before abort has been completed will still be returned by the stream.
    pub fn abort(self) {
        let mut abort_sender = self.abort_sender.lock().unwrap();
        if let Some(abort_sender) = abort_sender.take() {
            self.sender.abort(self.id);
            let _ = abort_sender.send(());
        }
    }

    /// Id of the request this [Aborter] can abort.
    pub fn request_id(&self) -> u64 {
        self.id
    }
}

impl<Request, Output> Clone for Aborter<Request, Output> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            sender: self.sender.clone(),
            abort_sender: self.abort_sender.clone(),
        }
    }
}

/// Trait implemented by consumers.
///
/// It is used to create consumers.
///
/// You should never have to implement it manually - use macros.
pub trait Consume<Consumer> {
    /// Request message type.
    type Request;

    /// Response message type.
    type Response;

    /// Create consumer using given transport and configuration.
    fn consume<Transport, Error>(
        transport: Transport,
        configuration: Configuration,
        error_handler: ErrorHandler<Self::Request, Error>,
    ) -> Consumer
    where
        Transport: self::Transport<Self::Request, Self::Response, Error>,
        Error: crate::TransportError;
}
