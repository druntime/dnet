#![cfg(target_arch = "wasm32")]

mod api;

use std::{cell::RefCell, collections::HashMap, mem::forget, rc::Rc};

use dnet::{
    codecs::BincodeCodec,
    rpc::{
        consumer,
        producer::{self, Produce},
        Consume, Error,
    },
    utils::pipe::ErrorHandler,
    webworker::WebWorkerTransport,
};
use js_utils::{console_log, document, event::When, spawn};
use wasm_bindgen::{prelude::wasm_bindgen, JsCast};
use web_sys::{
    js_sys::global, HtmlInputElement, HtmlTextAreaElement, MouseEvent, Worker, WorkerOptions,
    WorkerType,
};

use crate::api::{Api, Consumer, Producer};

#[wasm_bindgen(start)]
async fn start() {
    js_utils::set_panic_hook();

    let in_worker = in_worker();
    console_log!("WASM initialized, in worker: {in_worker}.");

    if in_worker {
        start_worker().await;
    } else {
        start_host().await;
    }
}

fn in_worker() -> bool {
    global().dyn_into::<web_sys::WorkerGlobalScope>().is_ok()
}

async fn start_host() {
    let options = WorkerOptions::new();
    options.set_type(WorkerType::Module);
    let worker = Worker::new_with_options("./starter.js", &options).unwrap();
    let transport = WebWorkerTransport::new(worker, BincodeCodec::default())
        .await
        .unwrap();
    let consumer = Rc::new(Consumer::consume(
        transport,
        consumer::Configuration::default(),
        ErrorHandler::default(),
    ));

    let document = document();

    let text_area = Rc::new(
        document
            .get_element_by_id("text")
            .unwrap()
            .dyn_into::<HtmlTextAreaElement>()
            .unwrap(),
    );

    let number = Rc::new(
        document
            .get_element_by_id("number")
            .unwrap()
            .dyn_into::<HtmlInputElement>()
            .unwrap(),
    );

    let fibonacci_button = Rc::new(document.get_element_by_id("fibonacci").unwrap());
    let factorial_button = Rc::new(document.get_element_by_id("factorial").unwrap());

    let abort_button = Rc::new(document.get_element_by_id("abort").unwrap());

    let write_line = move |line: &str| {
        let mut value = text_area.value();
        value += line;
        value += "\n";
        text_area.set_value(&value);
        text_area.set_scroll_top(text_area.scroll_height());
    };

    let write = write_line.clone();
    let get_number_input = move || {
        let value = number.value();
        let input = value.parse::<u64>();
        if input.is_err() {
            write(&format!("Error: {value} is not a non-negative integer."));
        }
        input.ok()
    };

    assert!(consumer.is_running().await.unwrap());
    write_line("Web worker is running and RPC connection has been established.");

    let aborters = Rc::new(RefCell::new(HashMap::new()));

    {
        let consumer = consumer.clone();
        let write = write_line.clone();
        let get_input = get_number_input.clone();
        let aborters = aborters.clone();
        let listener = fibonacci_button
            .when("click", move |_: MouseEvent| {
                if let Some(value) = get_input() {
                    let consumer = consumer.clone();
                    let write = write.clone();
                    let aborters = aborters.clone();
                    write(&format!("Calculating fibonacci({value})..."));
                    spawn(async move {
                        let mut request = consumer.fibonacci(value);
                        let aborter = request.aborter();
                        let id = aborter.request_id();
                        aborters.borrow_mut().insert(id, aborter);
                        match request.await {
                            Ok(result) => {
                                write(&format!("fibonacci({value}) = {result}"));
                                aborters.borrow_mut().remove(&id);
                            }
                            Err(Error::Aborted) => {
                                write(&format!("Task fibonacci({value}) was aborted."));
                            }
                            Err(error) => {
                                write(&format!(
                                    "Error occurred while calculating fibonacci({value}): {error}"
                                ));
                                aborters.borrow_mut().remove(&id);
                            }
                        }
                    });
                }
            })
            .unwrap();
        forget(listener); // otherwise listener will be dropped
    }

    {
        let consumer = consumer.clone();
        let write = write_line.clone();
        let get_input = get_number_input.clone();
        let aborters = aborters.clone();
        let listener = factorial_button
            .when("click", move |_: MouseEvent| {
                if let Some(value) = get_input() {
                    let consumer = consumer.clone();
                    let write = write.clone();
                    let aborters = aborters.clone();
                    write(&format!("Calculating {value}!..."));
                    spawn(async move {
                        let mut request = consumer.factorial(value);
                        let aborter = request.aborter();
                        let id = aborter.request_id();
                        aborters.borrow_mut().insert(id, aborter);
                        match request.await {
                            Ok(result) => {
                                write(&format!("{value}! = {result}"));
                                aborters.borrow_mut().remove(&id);
                            }
                            Err(Error::Aborted) => {
                                write(&format!("Task {value}! was aborted."));
                            }
                            Err(error) => {
                                write(&format!(
                                    "Error occurred while calculating {value}!: {error}"
                                ));
                                aborters.borrow_mut().remove(&id);
                            }
                        }
                    });
                }
            })
            .unwrap();
        forget(listener); // otherwise listener will be dropped
    }

    {
        let write = write_line.clone();
        let aborters = aborters.clone();
        let listener = abort_button
            .when("click", move |_: MouseEvent| {
                let mut aborters = aborters.borrow_mut();
                if aborters.is_empty() {
                    write("No tasks are running.");
                } else {
                    for (_, aborter) in aborters.drain() {
                        aborter.abort();
                    }
                }
            })
            .unwrap();
        forget(listener); // otherwise listener will be dropped
    }
}

async fn start_worker() {
    let transport = WebWorkerTransport::new_in_worker(BincodeCodec::default())
        .await
        .unwrap();
    let producer = Producer {};
    producer.produce(
        transport,
        producer::Configuration::default(),
        ErrorHandler::default(),
    );
}
