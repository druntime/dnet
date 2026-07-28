#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use chrono::NaiveTime;
use dnet::rpc::{api, no_ack};

#[api]
pub trait Api {
    /// Print "Hello World!" message on the server.
    #[no_ack]
    async fn hello_world(&self);

    /// Add two numbers together.
    async fn add_numbers(&self, a: i32, b: i32) -> i32;

    /// Concatenate two strings together.
    async fn concatenate_strings(&self, a: String, b: String) -> String;

    /// Repeatedly send server time at given interval.
    async fn stream_time(&self, interval: Duration) -> impl Stream<Item = NaiveTime>;
}
