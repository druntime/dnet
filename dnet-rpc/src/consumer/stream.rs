//! Return value for streaming requests.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver},
        oneshot,
    },
    future::FusedFuture,
    ready,
    stream::FusedStream,
    Future, FutureExt, StreamExt,
};
use pin_project::{pin_project, pinned_drop};

use crate::parts::consumer::{RequestSender, ResultSender};

use super::Aborter;

/// Future returned by consumer streaming requests.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct StreamRequest<Request, T> {
    sender: RequestSender<Request, T>,
    id: u64,
    request: Option<Request>,
    result_receiver: Option<oneshot::Receiver<super::Result<()>>>,
    values_receiver: Option<UnboundedReceiver<T>>,
    aborter: Option<Aborter<Request, T>>,
    abort_receiver: Option<oneshot::Receiver<()>>,
}

impl<Request, T> StreamRequest<Request, T> {
    /// Create new stream request.
    ///
    /// **NOTE**: This is used internally by the generated consumers.<br>
    /// You should never have to create it manually yourself.
    pub fn new(sender: RequestSender<Request, T>, id: u64, request: Request) -> Self {
        StreamRequest {
            sender,
            id,
            request: Some(request),
            result_receiver: None,
            values_receiver: None,
            aborter: None,
            abort_receiver: None,
        }
    }

    /// Request id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Aborter for this stream request.
    ///
    /// **NOTE**: This aborter has ability to abort resulting stream as well.
    pub fn aborter(&mut self) -> Aborter<Request, T> {
        let aborter = self.aborter.get_or_insert_with(|| {
            let (abort_sender, abort_receiver) = oneshot::channel();
            self.abort_receiver = Some(abort_receiver);
            Aborter {
                id: self.id,
                sender: self.sender.clone(),
                abort_sender: Arc::new(Mutex::new(Some(abort_sender))),
            }
        });
        aborter.clone()
    }
}

impl<Request, T> Future for StreamRequest<Request, T> {
    type Output = super::Result<Stream<Request, T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        if let Some(abort_receiver) = &mut me.abort_receiver {
            if let Poll::Ready(result) = abort_receiver.poll_unpin(cx) {
                if result.is_ok() {
                    me.request.take();
                    me.result_receiver.take();
                    me.values_receiver.take();
                    return Poll::Ready(Err(super::super::Error::Aborted));
                }
            }
        }

        if let Some(request) = me.request.take() {
            let (result_sender, mut result_receiver) = oneshot::channel();
            let (values_sender, values_receiver) = unbounded();
            let sender = ResultSender::Stream {
                result_sender,
                values_sender,
            };
            me.sender.send(*me.id, request, sender).map_err(|_| {
                me.result_receiver.take();
                super::super::Error::Shutdown
            })?;
            match result_receiver.poll_unpin(cx) {
                Poll::Ready(result) => {
                    me.result_receiver.take();
                    let result = result
                        .map(|_| Stream {
                            id: *me.id,
                            sender: me.sender.clone(),
                            receiver: Some(values_receiver),
                            aborter: me.aborter.take(),
                            abort_receiver: me.abort_receiver.take(),
                        })
                        .map_err(|_| {
                            me.values_receiver.take();
                            super::super::Error::Dropped
                        });
                    Poll::Ready(result)
                }
                Poll::Pending => {
                    *me.result_receiver = Some(result_receiver);
                    *me.values_receiver = Some(values_receiver);
                    Poll::Pending
                }
            }
        } else if let Some(receiver) = &mut me.result_receiver {
            let result = ready!(receiver.poll_unpin(cx));
            me.result_receiver.take();
            let result = result
                .map(|_| Stream {
                    id: *me.id,
                    sender: me.sender.clone(),
                    receiver: me.values_receiver.take(),
                    aborter: me.aborter.take(),
                    abort_receiver: me.abort_receiver.take(),
                })
                .map_err(|_| {
                    me.values_receiver.take();
                    super::super::Error::Dropped
                });
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl<Request, T> FusedFuture for StreamRequest<Request, T> {
    fn is_terminated(&self) -> bool {
        self.request.is_none() && self.result_receiver.is_none()
    }
}

#[pinned_drop]
impl<Request, T> PinnedDrop for StreamRequest<Request, T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.is_terminated() {
            self.sender.abort(self.id);
        }
    }
}

/// Stream returned by the [StreamRequest].
#[derive(Debug)]
pub struct Stream<Request, T> {
    id: u64,
    sender: RequestSender<Request, T>,
    receiver: Option<UnboundedReceiver<T>>,
    aborter: Option<Aborter<Request, T>>,
    abort_receiver: Option<oneshot::Receiver<()>>,
}

impl<Request, T> Stream<Request, T> {
    /// Aborter for this stream.
    pub fn aborter(&mut self) -> Aborter<Request, T> {
        let aborter = self.aborter.get_or_insert_with(|| {
            let (abort_sender, abort_receiver) = oneshot::channel();
            self.abort_receiver = Some(abort_receiver);
            Aborter {
                id: self.id,
                sender: self.sender.clone(),
                abort_sender: Arc::new(Mutex::new(Some(abort_sender))),
            }
        });
        aborter.clone()
    }
}

impl<Request, T> futures::Stream for Stream<Request, T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(receiver) = &mut self.receiver {
            match receiver.poll_next_unpin(cx) {
                Poll::Ready(result) => {
                    if let Some(result) = result {
                        Poll::Ready(Some(result))
                    } else {
                        self.receiver.take();
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => {
                    if let Some(abort_receiver) = &mut self.abort_receiver {
                        if let Poll::Ready(result) = abort_receiver.poll_unpin(cx) {
                            if result.is_ok() {
                                self.receiver.take();
                                return Poll::Ready(None);
                            }
                        }
                    }
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl<Request, T> FusedStream for Stream<Request, T> {
    fn is_terminated(&self) -> bool {
        self.receiver.is_none()
    }
}

impl<Request, T> Drop for Stream<Request, T> {
    fn drop(&mut self) {
        if !self.is_terminated() {
            self.sender.abort(self.id);
        }
    }
}
