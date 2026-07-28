//! Return value for value requests.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{channel::oneshot, future::FusedFuture, ready, Future, FutureExt};
use pin_project::{pin_project, pinned_drop};

use crate::parts::consumer::RequestSender;

use super::{Aborter, ResultSender};

/// Future returned by consumer value requests.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct ValueRequest<Request, T> {
    sender: RequestSender<Request, T>,
    id: u64,
    request: Option<Request>,
    receiver: Option<oneshot::Receiver<super::Result<T>>>,
    aborter: Option<Aborter<Request, T>>,
    abort_receiver: Option<oneshot::Receiver<()>>,
}

impl<Request, T> ValueRequest<Request, T> {
    /// Create new value request.
    ///
    /// **NOTE**: This is used internally by the generated consumers.<br>
    /// You should never have to create it manually yourself.
    pub fn new(sender: RequestSender<Request, T>, id: u64, request: Request) -> Self {
        ValueRequest {
            sender,
            id,
            request: Some(request),
            receiver: None,
            aborter: None,
            abort_receiver: None,
        }
    }

    /// Request id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Aborter for this value request.
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

impl<Request, T> Future for ValueRequest<Request, T> {
    type Output = super::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        if let Some(abort_receiver) = me.abort_receiver {
            if let Poll::Ready(result) = abort_receiver.poll_unpin(cx) {
                if result.is_ok() {
                    me.request.take();
                    me.receiver.take();
                    return Poll::Ready(Err(super::super::Error::Aborted));
                }
            }
        }

        if let Some(request) = me.request.take() {
            let (sender, mut receiver) = oneshot::channel();
            let sender = ResultSender::Value(sender);
            me.sender.send(*me.id, request, sender).map_err(|_| {
                me.receiver.take();
                super::super::Error::Shutdown
            })?;
            match receiver.poll_unpin(cx) {
                Poll::Ready(result) => {
                    me.receiver.take();
                    Poll::Ready(result.map_err(|_| super::super::Error::Dropped)?)
                }
                Poll::Pending => {
                    *me.receiver = Some(receiver);
                    Poll::Pending
                }
            }
        } else if let Some(receiver) = &mut me.receiver {
            let result = ready!(receiver.poll_unpin(cx));
            me.receiver.take();
            Poll::Ready(result.map_err(|_| super::super::Error::Dropped)?)
        } else {
            Poll::Pending
        }
    }
}

impl<Request, T> FusedFuture for ValueRequest<Request, T> {
    fn is_terminated(&self) -> bool {
        self.request.is_none() && self.receiver.is_none()
    }
}

#[pinned_drop]
impl<Request, T> PinnedDrop for ValueRequest<Request, T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.is_terminated() {
            self.sender.abort(self.id);
        }
    }
}
