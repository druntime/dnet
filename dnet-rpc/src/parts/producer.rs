//! Producer parts.

use std::future::Future;

use futures::{
    channel::{mpsc::UnboundedSender, oneshot},
    select, FutureExt, Sink, SinkExt, Stream, StreamExt,
};

use dportable::{create_non_sync_send_variant_for_wasm, spawn};
use futures::pin_mut;

use crate::{
    consumer::RequestId,
    producer::{self, abortable::AborterToken, StreamResponse},
    ShutdownType,
};

macro_rules! wrap_task {
    ($task:expr, $task_aborter:expr) => {{
        #[cfg(target_arch = "wasm32")]
        {
            if $task_aborter.is_some() {
                spawn(async move { $task.await })
                    .map(|join_handle| join_handle.unwrap())
                    .boxed_local()
            } else {
                $task.boxed_local()
            }
            .fuse()
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            if $task_aborter.is_some() {
                spawn(async move { $task.await })
                    .map(|join_handle| join_handle.unwrap())
                    .boxed()
            } else {
                $task.boxed()
            }
            .fuse()
        }
    }};
}

/// Handle response to customer.
pub async fn handle_response<Response, S>(
    message: producer::Message<Response>,
    sender: &mut S,
    stop_sender: &mut Option<oneshot::Sender<ShutdownType>>,
) where
    S: Sink<producer::Message<Response>> + Unpin,
{
    match message {
        producer::Message::Response { .. } => {
            if sender.send(message).await.is_err() {
                if let Some(stop_sender) = stop_sender.take() {
                    let _ = stop_sender.send(ShutdownType::Closed);
                }
            }
        }
        _ => {
            let _ = sender.send(message).await;
        }
    }
}

/// Handle request from consumer.
pub fn handle_request<F, O, E, R, Response>(
    id: RequestId,
    task: F,
    response_factory: R,
    reply_sender: UnboundedSender<producer::Message<Response>>,
    remove_aborter_sender: UnboundedSender<RequestId>,
    mut abort_receiver: oneshot::Receiver<()>,
    task_aborter: Option<AborterToken>,
) where
    F: Future<Output = Result<O, E>> + SendUnlessWasm + 'static,
    O: Send + 'static,
    E: Send + 'static,
    R: Fn(O) -> Response + Send + 'static,
    Response: Send + 'static,
{
    spawn(async move {
        let task = wrap_task!(task, task_aborter);
        pin_mut!(task);
        select! {
            result = task => {
                if let Ok(result) = result {
                    let response = response_factory(result);
                    let message = producer::Message::Response { id, response };
                    let _ = reply_sender.unbounded_send(message);
                }
                let _ = remove_aborter_sender.unbounded_send(id);
            },
            _ = abort_receiver => {
                if let Some(task_aborter) = task_aborter {
                    task_aborter.cancel();
                }
            },
        };
    });
}

/// Handle no-ack request from consumer.
pub fn handle_no_ack_request<F, E>(
    id: RequestId,
    task: F,
    remove_aborter_sender: UnboundedSender<RequestId>,
    mut abort_receiver: oneshot::Receiver<()>,
    task_aborter: Option<AborterToken>,
) where
    F: Future<Output = Result<(), E>> + SendUnlessWasm + 'static,
    E: Send + 'static,
{
    spawn(async move {
        let task = wrap_task!(task, task_aborter);
        pin_mut!(task);
        select! {
            _result = task => {
                let _ = remove_aborter_sender.unbounded_send(id);
            },
            _ = abort_receiver => {
                if let Some(task_aborter) = task_aborter {
                    task_aborter.cancel();
                }
            },
        };
    });
}

/// Handle stream request from consumer.
pub fn handle_stream_request<F, S, E, O, R, Response>(
    id: RequestId,
    task: F,
    response_factory: R,
    reply_sender: UnboundedSender<producer::Message<Response>>,
    remove_aborter_sender: UnboundedSender<RequestId>,
    mut abort_receiver: oneshot::Receiver<()>,
    task_aborter: Option<AborterToken>,
) where
    F: Future<Output = Result<S, E>> + SendUnlessWasm + 'static,
    S: Stream<Item = O> + Send + Unpin + 'static,
    E: Send + 'static,
    R: Fn(StreamResponse<O>) -> Response + Send + 'static,
    Response: Send + 'static,
{
    spawn(async move {
        let task = wrap_task!(task, task_aborter);

        let mut stream = match task.await {
            Ok(stream) => stream.fuse(),
            Err(_) => {
                let _ = remove_aborter_sender.unbounded_send(id);
                return;
            }
        };

        let response = StreamResponse::Open;
        let response = response_factory(response);
        let message = producer::Message::Response { id, response };
        let _ = reply_sender.unbounded_send(message);
        loop {
            select! {
                result = stream.next() => {
                    if let Some(result) = result {
                        let response = StreamResponse::Item(result);
                        let response = response_factory(response);
                        let message = producer::Message::Response { id, response };
                        let _ = reply_sender.unbounded_send(message);
                    } else {
                        let response = StreamResponse::Closed;
                        let response = response_factory(response);
                        let message = producer::Message::Response { id, response };
                        let _ = reply_sender.unbounded_send(message);

                        let _ = remove_aborter_sender.unbounded_send(id);
                        break;
                    }
                },
                _ = abort_receiver => {
                    if let Some(task_aborter) = task_aborter {
                        task_aborter.cancel();
                    }
                    break;
                },
            };
        }
    });
}

create_non_sync_send_variant_for_wasm! {
    /// Trait for types implementing [Send] unless running under
    /// WASM targets - then it does nothing.
    pub trait SendUnlessWasm: Send {}

    impl<T> SendUnlessWasm for T where T: Send {}
}
