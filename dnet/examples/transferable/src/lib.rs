#![cfg(target_arch = "wasm32")]

mod demo;
mod math;
mod renderer;
mod shader;

use std::{cell::RefCell, collections::HashMap, mem::forget, rc::Rc};

use dnet::{
    codecs::BincodeCodec,
    js::{wrapper::Context, IntoTransferable, TransferableTransport},
    Receive,
};
use futures::{lock::Mutex, Sink, SinkExt};
use js_utils::{closure, console_log, document, event::When, spawn, window};
use wasm_bindgen::{prelude::wasm_bindgen, JsCast};
use web_sys::{
    js_sys::{global, Array},
    DedicatedWorkerGlobalScope, Event, HtmlCanvasElement, OffscreenCanvas, ResizeObserver, Worker,
    WorkerOptions, WorkerType,
};

use crate::demo::Demo;

#[derive(IntoTransferable)]
pub enum Message {
    Init {
        #[transferable]
        offscreen_canvas: OffscreenCanvas,
    },
    Resize {
        width: f32,
        height: f32,
    },
    StateUpdate {
        light_position: [f32; 3],
        shininess: f32,
        speed: f32,
    },
}

#[derive(IntoTransferable)]
pub struct Empty {}

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
    let worker = Rc::new(worker);
    let context = Context::new(BincodeCodec::default());
    let mut transport =
        TransferableTransport::<_, _, Empty, Message, _>::new(&worker, None, context, true)
            .await
            .unwrap();

    let document = document();

    let wrapper = document
        .get_element_by_id("wrapper")
        .expect("no #wrapper element");

    let canvas = document
        .get_element_by_id("canvas")
        .expect("no #canvas element")
        .dyn_into::<HtmlCanvasElement>()
        .unwrap();

    let offscreen_canvas = canvas.transfer_control_to_offscreen().unwrap();
    transport
        .send(Message::Init { offscreen_canvas })
        .await
        .unwrap();

    let transport = Rc::new(Mutex::new(transport));

    let state = InputsState::new();
    let state = Rc::new(RefCell::new(state));

    add_input_listener(&state, "light-x", &transport);
    add_input_listener(&state, "light-y", &transport);
    add_input_listener(&state, "light-z", &transport);
    add_input_listener(&state, "shininess", &transport);
    add_input_listener(&state, "speed", &transport);

    {
        let transport = transport.clone();

        let update_size = move || {
            let dpr = window().device_pixel_ratio();
            let width = (canvas.client_width() as f64 * dpr) as f32;
            let height = (canvas.client_height() as f64 * dpr) as f32;
            let transport = transport.clone();
            spawn(async move {
                transport
                    .lock()
                    .await
                    .send(Message::Resize { width, height })
                    .await
                    .unwrap();
            });
        };

        let update_size_clone = update_size.clone();
        let resize_closure = closure!(move |_entries: Array| {
            update_size_clone();
        });
        let resize_observer = ResizeObserver::new(resize_closure.as_ref().unchecked_ref()).unwrap();
        resize_observer.observe(&wrapper);
        forget(resize_closure);

        update_size();
    }
}

async fn start_worker() {
    let global = global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .expect("should be in a worker");
    let global = Rc::new(global);
    let context = Context::new(BincodeCodec::default());
    let mut transport =
        TransferableTransport::<_, _, Message, Empty, _>::new(&global, None, context, false)
            .await
            .unwrap();
    let init_message = transport.receive().await.unwrap();
    let Message::Init { offscreen_canvas } = init_message else {
        panic!("expected Init message");
    };
    let gl = offscreen_canvas
        .get_context("webgl2")
        .ok()
        .flatten()
        .expect("WebGL2 not supported")
        .dyn_into()
        .unwrap();

    let demo = Rc::new(Demo::new(gl));
    demo.update_size(
        offscreen_canvas.width() as f32,
        offscreen_canvas.height() as f32,
    );
    demo.clone().start();

    loop {
        let message = transport.receive().await.unwrap();
        match message {
            Message::Resize { width, height } => {
                offscreen_canvas.set_width(width as u32);
                offscreen_canvas.set_height(height as u32);
                demo.update_size(width, height);
            }
            Message::StateUpdate {
                light_position,
                shininess,
                speed,
            } => {
                let mut state = demo.state.borrow_mut();
                state.renderer.scene.light_position = light_position;
                state.renderer.scene.model.shininess = shininess;
                state.speed = speed;
            }
            _ => {
                panic!("unexpected message received in worker")
            }
        }
    }
}

struct InputsState(HashMap<&'static str, f32>);

impl InputsState {
    fn new() -> Self {
        let mut state = HashMap::new();
        state.insert("light-x", 2.0);
        state.insert("light-y", 2.0);
        state.insert("light-z", 2.0);
        state.insert("shininess", 32.0);
        state.insert("speed", 1.0);
        InputsState(state)
    }
}

impl From<&InputsState> for Message {
    fn from(state: &InputsState) -> Self {
        Message::StateUpdate {
            light_position: [state.0["light-x"], state.0["light-y"], state.0["light-z"]],
            shininess: state.0["shininess"],
            speed: state.0["speed"],
        }
    }
}

fn add_input_listener(
    state: &Rc<RefCell<InputsState>>,
    input_name: &'static str,
    transport: &Rc<Mutex<impl Sink<Message, Error = impl std::error::Error> + Unpin + 'static>>,
) {
    let input = document()
        .get_element_by_id(input_name)
        .unwrap()
        .dyn_into::<web_sys::HtmlInputElement>()
        .unwrap();
    let input = Rc::new(input);

    let input_clone = input.clone();
    let state_clone = state.clone();
    let transport_clone = transport.clone();
    let listener = Rc::new(input).when("input", move |_: Event| {
        let value = input_clone.value().parse::<f32>().unwrap_or(0.0);
        let state = state_clone.clone();
        state.borrow_mut().0.insert(input_name, value);
        let transport = transport_clone.clone();
        spawn(async move {
            let message = Message::from(&*state.borrow());
            transport.lock().await.send(message).await.unwrap();
        });
    });

    forget(listener);
}
