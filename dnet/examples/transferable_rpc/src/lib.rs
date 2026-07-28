#![cfg(target_arch = "wasm32")]

mod demo;
mod math;
mod renderer;
mod shader;

use std::{cell::RefCell, collections::HashMap, mem::forget, rc::Rc};

use dnet::{
    codecs::BincodeCodec,
    js::{wrapper::Context, TransferableTransport},
    rpc::{api, producer::Produce, Consume, Produce},
};
use js_utils::{closure, console_log, document, event::When, spawn, window};
use wasm_bindgen::{prelude::wasm_bindgen, JsCast};
use web_sys::{
    js_sys::{global, Array},
    DedicatedWorkerGlobalScope, Event, HtmlCanvasElement, OffscreenCanvas, ResizeObserver, Worker,
    WorkerOptions, WorkerType,
};

use crate::demo::Demo;

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
    let transport = TransferableTransport::new(&worker, None, context, true)
        .await
        .unwrap();

    let worker_api = Rc::new(Consumer::consume(
        transport,
        Default::default(),
        Default::default(),
    ));

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
    worker_api.init(offscreen_canvas).await.unwrap();

    let state = InputsState::new();
    let state = Rc::new(RefCell::new(state));

    add_input_listener(&state, "light-x", &worker_api);
    add_input_listener(&state, "light-y", &worker_api);
    add_input_listener(&state, "light-z", &worker_api);
    add_input_listener(&state, "shininess", &worker_api);
    add_input_listener(&state, "speed", &worker_api);

    {
        let update_size = move || {
            let dpr = window().device_pixel_ratio();
            let width = (canvas.client_width() as f64 * dpr) as f32;
            let height = (canvas.client_height() as f64 * dpr) as f32;
            let worker_api = worker_api.clone();
            spawn(async move {
                worker_api.resize(width, height).await.unwrap();
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
    let transport = TransferableTransport::new(&global, None, context, false)
        .await
        .unwrap();
    let demo_worker = DemoWorker::new();
    demo_worker
        .produce(transport, Default::default(), Default::default())
        .await
        .unwrap();
}

#[api]
trait WorkerApi {
    async fn init(&self, #[transferable] offscreen_canvas: OffscreenCanvas);

    async fn resize(&self, width: f32, height: f32);

    async fn update_state(&self, light_position: [f32; 3], shininess: f32, speed: f32);
}

#[derive(Produce)]
struct DemoWorker {
    state: RefCell<Option<WorkerState>>,
}

struct WorkerState {
    canvas: OffscreenCanvas,
    demo: Rc<Demo>,
}

impl DemoWorker {
    fn new() -> Self {
        DemoWorker {
            state: RefCell::new(None),
        }
    }

    async fn init(&self, offscreen_canvas: OffscreenCanvas) {
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

        let state = WorkerState {
            canvas: offscreen_canvas,
            demo,
        };
        *self.state.borrow_mut() = Some(state);
    }

    async fn resize(&self, width: f32, height: f32) {
        if let Some(state) = self.state.borrow().as_ref() {
            state.canvas.set_width(width as u32);
            state.canvas.set_height(height as u32);
            state.demo.update_size(width, height);
        } else {
            panic!("resize called before init");
        }
    }

    async fn update_state(&self, light_position: [f32; 3], shininess: f32, speed: f32) {
        if let Some(state) = self.state.borrow().as_ref() {
            let mut state = state.demo.state.borrow_mut();
            state.renderer.scene.light_position = light_position;
            state.renderer.scene.model.shininess = shininess;
            state.speed = speed;
        } else {
            panic!("update_state called before init");
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

impl InputsState {
    fn light_position(&self) -> [f32; 3] {
        [self.0["light-x"], self.0["light-y"], self.0["light-z"]]
    }

    fn shininess(&self) -> f32 {
        self.0["shininess"]
    }

    fn speed(&self) -> f32 {
        self.0["speed"]
    }
}

fn add_input_listener(
    state: &Rc<RefCell<InputsState>>,
    input_name: &'static str,
    worker_api: &Rc<Consumer>,
) {
    let input = document()
        .get_element_by_id(input_name)
        .unwrap()
        .dyn_into::<web_sys::HtmlInputElement>()
        .unwrap();
    let input = Rc::new(input);

    let input_clone = input.clone();
    let state_clone = state.clone();
    let worker_api = worker_api.clone();
    let listener = Rc::new(input).when("input", move |_: Event| {
        let value = input_clone.value().parse::<f32>().unwrap_or(0.0);
        let state = state_clone.clone();
        state.borrow_mut().0.insert(input_name, value);
        let worker_api = worker_api.clone();
        spawn(async move {
            let state = state.borrow();
            worker_api
                .update_state(state.light_position(), state.shininess(), state.speed())
                .await
                .unwrap();
        });
    });

    forget(listener);
}
