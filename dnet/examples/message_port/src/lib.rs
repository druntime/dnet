#![cfg(target_arch = "wasm32")]

use std::{mem, rc::Rc, time::Duration};

use dnet::{codecs::BincodeCodec, message_port::MessagePortTransport, Messages};
use futures::{SinkExt, StreamExt};
use js_utils::{console_log, document, event::When, sleep, spawn, window};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use web_sys::{js_sys::Array, Event, HtmlIFrameElement, MessageEvent};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    content: String,
    some_int: i32,
}

#[wasm_bindgen(start)]
async fn start() {
    js_utils::set_panic_hook();

    let in_frame = in_frame();
    console_log!("WASM initialized, in frame: {in_frame}.");

    if in_frame {
        start_guest().await;
    } else {
        start_host().await;
    }
}

async fn start_host() {
    let channel = web_sys::MessageChannel::new().expect("failed to create MessageChannel");
    let port1 = channel.port1();
    let port2 = channel.port2();

    let iframe = document()
        .query_selector("iframe")
        .ok()
        .flatten()
        .expect("iframe element not found")
        .dyn_into::<HtmlIFrameElement>()
        .expect("failed to cast to HtmlIFrameElement");

    let iframe = Rc::new(iframe);
    let iframe_clone = iframe.clone();
    let _handler = iframe
        .when("load", move |_: Event| {
            let iframe_clone = iframe_clone.clone();
            let port2 = port2.clone();
            spawn(async move {
                sleep(Duration::from_millis(10)).await; // wait a bit for script to load
                console_log!("Iframe loaded, posting message to iframe...");
                iframe_clone
                    .content_window()
                    .expect("failed to get content window")
                    .post_message_with_transfer(
                        &JsValue::from_str("Hello from host, sent you a port!"),
                        "*",
                        &Array::of1(&port2),
                    )
                    .expect("failed to post message to iframe");
            });
        })
        .expect("failed to set up load event listener");
    iframe.set_src("frame.html");

    let mut transport =
        MessagePortTransport::<_, Message, Message>::new(port1, BincodeCodec::default())
            .await
            .expect("failed to create transport");

    transport
        .send(Message {
            content: "Hello from host!".to_string(),
            some_int: 42,
        })
        .await
        .expect("failed to send message");

    let mut messages = transport.messages();
    while let Some(message) = messages.next().await {
        set_output(&format!("Received message: {message:?}"));
    }
}

async fn start_guest() {
    let window = Rc::new(window());
    let handler = window
        .when("message", move |event: MessageEvent| {
            let data = event.data().as_string().expect("expected string data");
            console_log!("Received message in iframe: {:?}", data);
            let port = event
                .ports()
                .get(0)
                .dyn_into::<web_sys::MessagePort>()
                .expect("failed to get MessagePort");

            spawn(async move {
                let mut transport =
                    MessagePortTransport::<_, Message, Message>::new(port, BincodeCodec::default())
                        .await
                        .expect("failed to create transport");

                transport
                    .send(Message {
                        content: "Hello from guest!".to_string(),
                        some_int: 24,
                    })
                    .await
                    .expect("failed to send message");

                let mut messages = transport.messages();
                while let Some(message) = messages.next().await {
                    set_output(&format!("Received message: {message:?}"));
                }
            });
        })
        .expect("failed to set up message listener");
    mem::forget(handler); // prevent the handler from being dropped
}

fn set_output(text: &str) {
    let output = document()
        .query_selector(".output")
        .ok()
        .flatten()
        .expect("output element not found");
    output.set_text_content(Some(text));
}

fn in_frame() -> bool {
    let window = window();
    let top = window.top().ok().flatten();
    if let Some(top) = top {
        top != window
    } else {
        false
    }
}
