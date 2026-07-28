#![cfg(target_arch = "wasm32")]

mod client;
mod server;

use client::run;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(start)]
pub async fn start() {
    run().await;
}
