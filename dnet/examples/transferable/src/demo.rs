use std::{cell::RefCell, rc::Rc};

use js_utils::window;
use wasm_bindgen::{prelude::Closure, JsCast};
use web_sys::{js_sys::global, DedicatedWorkerGlobalScope, WebGl2RenderingContext as Gl};

use crate::{
    math::Cube,
    renderer::{Model, Renderer, Scene},
    shader::Program,
};

pub struct Demo {
    pub state: RefCell<State>,
}

impl Demo {
    pub fn new(gl: Gl) -> Self {
        let program = Program::new(&gl).unwrap();

        let cube = Cube::new();

        let mut model = Model::new(&gl, &program, cube);
        model.translation = [0.0, 0.0, -7.0];
        model.shininess = 32.0;

        let scene = Scene {
            background_color: [0.039, 0.039, 0.071, 1.0],
            light_position: [2.0, 2.0, 2.0],
            model,
        };

        let state = State {
            gl,
            renderer: Renderer {
                program,
                scene,
                width: 800.0,
                height: 600.0,
            },
            speed: 1.0,
            timestamp: 0.0,
        };

        Demo {
            state: RefCell::new(state),
        }
    }

    pub fn render(&self) {
        self.state.borrow().renderer.render(&self.state.borrow().gl);
    }

    pub fn update(&self, timestamp: f64) {
        let mut state = self.state.borrow_mut();
        let delta_time = ((timestamp - state.timestamp) / 1000.0) as f32;
        let speed = state.speed;
        state.renderer.scene.model.rotation[0] += speed * delta_time;
        state.renderer.scene.model.rotation[1] += speed * delta_time;
        state.renderer.scene.model.rotation[2] += speed * delta_time * 0.27;
        state.timestamp = timestamp;
    }

    pub fn update_size(&self, width: f32, height: f32) {
        let mut state = self.state.borrow_mut();
        state.renderer.width = width;
        state.renderer.height = height;
        drop(state);
        self.render();
    }

    pub fn start(self: Rc<Self>) {
        let f: Rc<RefCell<Option<Closure<_>>>> = Rc::new(RefCell::new(None));
        let g = f.clone();

        let closure: Closure<dyn FnMut(f64)> = Closure::new(move |timestamp| {
            self.update(timestamp);
            self.render();
            // Schedule ourself for another requestAnimationFrame callback.

            request_animation_frame(f.borrow().as_ref().unwrap())
        });
        *g.borrow_mut() = Some(closure);

        // Start the rendering loop.
        request_animation_frame(g.borrow().as_ref().unwrap());
    }
}

fn request_animation_frame(f: &Closure<dyn FnMut(f64)>) {
    let reference = f.as_ref().unchecked_ref();
    if let Ok(global) = global().dyn_into::<DedicatedWorkerGlobalScope>() {
        global.request_animation_frame(reference)
    } else {
        window().request_animation_frame(reference)
    }
    .expect("should register `requestAnimationFrame` OK");
}

pub struct State {
    pub gl: Gl,
    pub renderer: Renderer,
    pub speed: f32,
    pub timestamp: f64,
}
