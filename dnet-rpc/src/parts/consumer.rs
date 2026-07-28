//! Consumer parts.

use std::{collections::HashMap, sync::Arc};

use atomic_counter::{AtomicCounter, ConsistentCounter};
use dportable::time::Timeout;
use futures::{
    channel::{
        mpsc::{unbounded, TrySendError, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Sink, SinkExt,
};

use crate::{
    consumer::{self, Message, Payload, RequestId},
    producer::{self, StreamResponse},
    ShutdownType,
};

/// Consumer state trait.
pub trait State<Response> {
    /// Handle message from producer.
    fn handle_message(&mut self, message: producer::Message<Response>) -> Result<(), ShutdownType>;

    /// Shutdown consumer with specified shutdown type.
    fn shutdown(&mut self, shutdown_type: ShutdownType);

    /// Check if consumer is idle (has no pending requests).
    fn idle(&self) -> bool;
}

/// Generates request ids.
#[derive(Debug, Default)]
pub struct RequestIdGenerator {
    counter: ConsistentCounter,
}

impl RequestIdGenerator {
    /// Create new request id generator.
    pub fn new() -> Self {
        RequestIdGenerator {
            counter: ConsistentCounter::new(0),
        }
    }

    /// Generate new request id.
    pub fn id(&self) -> RequestId {
        self.counter.inc() as RequestId
    }
}

/// Sender for singular value results.
pub type ValueResultSender<T> = oneshot::Sender<consumer::Result<T>>;

/// Sender for stream result.
pub type StreamResultSender = ValueResultSender<()>;

/// Sender for stream values.
pub type StreamValuesSender<T> = UnboundedSender<T>;

/// Used to send results to consumer request futures.
#[derive(Debug)]
pub enum ResultSender<T> {
    /// Result sender for single values.
    Value(ValueResultSender<T>),

    /// Result sender for streams.
    Stream {
        /// Sender for stream open result.
        result_sender: StreamResultSender,

        /// Sender for stream values.
        values_sender: StreamValuesSender<T>,
    },

    /// Request was aborted.
    Abort,
}

/// Pair of consumer message and result sender.
#[derive(Debug)]
pub struct FullRequest<Request, T> {
    /// Consumer message.
    pub message: Message<Request>,

    /// Sender for request result.
    pub result_sender: ResultSender<T>,
}

/// Sends requests from value futures to consumer implementation.
#[derive(Debug)]
pub struct RequestSender<Request, T> {
    sender: Arc<UnboundedSender<FullRequest<Request, T>>>,
}

impl<Request, T> Clone for RequestSender<Request, T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<Request, T> RequestSender<Request, T> {
    /// Create sender-receiver pair.
    pub fn pair() -> (
        RequestSender<Request, T>,
        UnboundedReceiver<FullRequest<Request, T>>,
    ) {
        let (sender, receiver) = unbounded();
        let sender = Arc::new(sender);
        let sender = RequestSender { sender };
        (sender, receiver)
    }

    /// Abort request with specified id.
    pub fn abort(&self, id: RequestId) {
        let payload = Payload::Abort;
        let message = Message { id, payload };
        let full = FullRequest {
            message,
            result_sender: ResultSender::Abort,
        };
        let _ = self.sender.unbounded_send(full);
    }

    /// Send request.
    pub fn send(
        &self,
        id: RequestId,
        request: Request,
        result_sender: ResultSender<T>,
    ) -> Result<(), TrySendError<FullRequest<Request, T>>> {
        let message = Message {
            id,
            payload: Payload::Request(request),
        };
        let full = FullRequest {
            message,
            result_sender,
        };
        self.sender.unbounded_send(full)
    }
}

/// Handle response from producer.
pub fn handle_response<T>(
    result: T,
    request_id: RequestId,
    requests: &mut HashMap<RequestId, oneshot::Sender<consumer::Result<T>>>,
    pending: &mut usize,
) {
    if let Some(sender) = requests.remove(&request_id) {
        let _ = sender.send(Ok(result));
        *pending -= 1;
    }
}

/// Handle stream response from producer.
pub fn handle_stream_response<T>(
    result: StreamResponse<T>,
    request_id: RequestId,
    requests: &mut HashMap<RequestId, (Option<StreamResultSender>, StreamValuesSender<T>)>,
    pending: &mut usize,
) {
    match result {
        StreamResponse::Open => {
            if let Some((sender, _)) = requests.get_mut(&request_id) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok(()));
                    *pending -= 1;
                }
            }
        }
        StreamResponse::Item(item) => {
            if let Some((_, sender)) = requests.get_mut(&request_id) {
                let _ = sender.unbounded_send(item);
            }
        }
        StreamResponse::Closed => {
            requests.remove(&request_id);
        }
    }
}

/// Handle message from producer.
pub fn handle_producer_message<Response, Error, S>(
    message_option: Option<Result<producer::Message<Response>, Error>>,
    has_timeout: bool,
    timeout_future: &mut Timeout,
    state: &mut S,
    should_break: &mut bool,
) where
    S: State<Response>,
{
    if let Some(result) = message_option {
        if has_timeout {
            timeout_future.reset();
        }

        match result {
            Ok(message) => {
                if let Err(shutdown_type) = state.handle_message(message) {
                    state.shutdown(shutdown_type);
                    *should_break = true;
                }
            }
            Err(_) => {
                *should_break = true;
            }
        }
    } else {
        state.shutdown(ShutdownType::Closed);
        *should_break = true;
    }
}

/// Handle new request.
pub async fn handle_new_request<Request, T, S>(
    request_option: Option<FullRequest<Request, T>>,
    has_timeout: bool,
    timeout_future: &mut Timeout,
    requests: &mut HashMap<RequestId, oneshot::Sender<consumer::Result<T>>>,
    sender: &mut S,
    pending: &mut usize,
) where
    S: Sink<consumer::Message<Request>> + Unpin,
{
    if let Some(FullRequest {
        message,
        result_sender,
    }) = request_option
    {
        if has_timeout {
            timeout_future.reset();
        }

        let id = message.id;
        let result = sender.send(message).await;
        let result = result.map_err(|_| crate::Error::Closed);
        match result_sender {
            ResultSender::Value(sender) => {
                if let Err(error) = result {
                    let _ = sender.send(Err(error));
                } else {
                    requests.insert(id, sender);
                    *pending += 1;
                }
            }
            ResultSender::Abort => {
                if requests.remove(&id).is_some() {
                    *pending -= 1;
                }
            }
            _ => unreachable!("stream sender got when value sender expected"),
        }
    }
}

/// Handle new no-ack request.
pub async fn handle_new_no_ack_request<Request, S>(
    request_option: Option<FullRequest<Request, ()>>,
    has_timeout: bool,
    timeout_future: &mut Timeout,
    sender: &mut S,
) where
    S: Sink<consumer::Message<Request>> + Unpin,
{
    if let Some(FullRequest {
        message,
        result_sender,
    }) = request_option
    {
        if has_timeout {
            timeout_future.reset();
        }

        let result = sender.send(message).await;
        let result = result.map_err(|_| crate::Error::Closed);
        match result_sender {
            ResultSender::Value(sender) => {
                if let Err(error) = result {
                    let _ = sender.send(Err(error));
                } else {
                    let _ = sender.send(Ok(()));
                }
            }
            ResultSender::Abort => {}
            _ => unreachable!("stream sender got when value sender expected"),
        }
    }
}

/// Handle new stream request.
pub async fn handle_new_stream_request<Request, T, S>(
    request_option: Option<FullRequest<Request, T>>,
    has_timeout: bool,
    timeout_future: &mut Timeout,
    requests: &mut HashMap<RequestId, (Option<StreamResultSender>, StreamValuesSender<T>)>,
    sender: &mut S,
    pending: &mut usize,
) where
    S: Sink<consumer::Message<Request>> + Unpin,
{
    if let Some(FullRequest {
        message,
        result_sender,
    }) = request_option
    {
        if has_timeout {
            timeout_future.reset();
        }

        let id = message.id;
        let result = sender.send(message).await;
        let result = result.map_err(|_| crate::Error::Closed);
        match result_sender {
            ResultSender::Stream {
                result_sender,
                values_sender,
            } => {
                if let Err(error) = result {
                    let _ = result_sender.send(Err(error));
                } else {
                    requests.insert(id, (Some(result_sender), values_sender));
                    *pending += 1;
                }
            }
            ResultSender::Abort => {
                if let Some((sender, _)) = requests.remove(&id) {
                    if sender.is_some() {
                        *pending -= 1;
                    }
                }
            }
            _ => unreachable!("value sender got when stream sender expected"),
        }
    }
}
