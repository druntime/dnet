//! State used in WASM transports.

use std::{collections::VecDeque, task::Waker};
/// State used in WASM transports.
pub struct State<T, Error> {
    /// Incoming messages (or errors).
    pub incoming: VecDeque<Result<T, Error>>,

    /// Is transport closed.
    pub closed: bool,

    waker: Option<Waker>,
}

impl<T, Error> State<T, Error> {
    /// Create new state.
    pub fn new() -> Self {
        State {
            incoming: VecDeque::new(),
            waker: None,
            closed: false,
        }
    }

    /// Enqueue message.
    pub fn message(&mut self, message: T) {
        self.incoming.push_back(Ok(message));
        self.wake();
    }

    /// Enqueue error.
    pub fn error(&mut self, error: Error) {
        self.incoming.push_back(Err(error));
        self.wake();
    }

    /// Close transport.
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.wake();
        }
    }

    /// Update waker.
    pub fn update_waker_with(&mut self, other: &Waker) {
        if let Some(waker) = &self.waker {
            if !waker.will_wake(other) {
                self.waker = Some(other.clone());
            }
        } else {
            self.waker = Some(other.clone());
        }
    }

    /// Is stream terminated.
    pub fn is_terminated(&self) -> bool {
        self.closed && self.incoming.is_empty()
    }

    /// Wake waiting.
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl<T, Error> Default for State<T, Error> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Error> Drop for State<T, Error> {
    fn drop(&mut self) {
        if !self.closed {
            self.close();
        }
    }
}
