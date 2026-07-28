use web_sys::{
    js_sys::{Float32Array, Uint16Array},
    WebGl2RenderingContext as Gl,
};

use crate::{
    math::{Matrix3, Matrix4},
    shader::{Program, Uniforms},
};

pub trait Mesh {
    fn vertices(&self) -> &[f32];
    fn normals(&self) -> &[f32];
    fn indices(&self) -> &[u16];
}

pub struct Model {
    pub vao: web_sys::WebGlVertexArrayObject,
    pub index_count: i32,
    pub translation: [f32; 3],
    pub rotation: [f32; 3],
    pub shininess: f32,
}

impl Model {
    pub fn new<M>(gl: &Gl, program: &Program, mesh: M) -> Model
    where
        M: Mesh,
    {
        let vao = gl.create_vertex_array().unwrap();
        gl.bind_vertex_array(Some(&vao));

        let position_buffer = gl.create_buffer().unwrap();
        gl.bind_buffer(Gl::ARRAY_BUFFER, Some(&position_buffer));
        // SAFETY: we're in single-threaded WASM, the slice is alive for this call.
        unsafe {
            let vert_array = Float32Array::view(mesh.vertices());
            gl.buffer_data_with_array_buffer_view(Gl::ARRAY_BUFFER, &vert_array, Gl::STATIC_DRAW);
        }
        gl.enable_vertex_attrib_array(program.locations.position as u32);
        gl.vertex_attrib_pointer_with_i32(
            program.locations.position as u32,
            3,
            Gl::FLOAT,
            false,
            0,
            0,
        );

        let normal_buffer = gl.create_buffer().unwrap();
        gl.bind_buffer(Gl::ARRAY_BUFFER, Some(&normal_buffer));
        // SAFETY: we're in single-threaded WASM, the slice is alive for this call.
        unsafe {
            let vert_array = Float32Array::view(mesh.normals());
            gl.buffer_data_with_array_buffer_view(Gl::ARRAY_BUFFER, &vert_array, Gl::STATIC_DRAW);
        }
        gl.enable_vertex_attrib_array(program.locations.normal as u32);
        gl.vertex_attrib_pointer_with_i32(
            program.locations.normal as u32,
            3,
            Gl::FLOAT,
            false,
            0,
            0,
        );

        let index_buffer = gl.create_buffer().unwrap();
        gl.bind_buffer(Gl::ELEMENT_ARRAY_BUFFER, Some(&index_buffer));
        // SAFETY: we're in single-threaded WASM, the slice is alive for this call.
        unsafe {
            let index_array = Uint16Array::view(mesh.indices());
            gl.buffer_data_with_array_buffer_view(
                Gl::ELEMENT_ARRAY_BUFFER,
                &index_array,
                Gl::STATIC_DRAW,
            );
        }

        let index_count = mesh.indices().len() as i32;

        Model {
            vao,
            index_count,
            translation: [0.0; 3],
            rotation: [0.0; 3],
            shininess: 32.0,
        }
    }

    pub fn use_model(&self, gl: &Gl) {
        gl.bind_vertex_array(Some(&self.vao));
    }

    pub fn draw(&self, gl: &Gl) {
        gl.draw_elements_with_i32(Gl::TRIANGLES, self.index_count, Gl::UNSIGNED_SHORT, 0);
    }
}

pub struct Scene {
    pub background_color: [f32; 4],
    pub light_position: [f32; 3],
    pub model: Model,
}

pub struct Renderer {
    pub program: Program,
    pub scene: Scene,
    pub width: f32,
    pub height: f32,
}

impl Renderer {
    pub fn render(&self, gl: &Gl) {
        gl.viewport(0, 0, self.width as i32, self.height as i32);

        gl.clear_color(
            self.scene.background_color[0],
            self.scene.background_color[1],
            self.scene.background_color[2],
            self.scene.background_color[3],
        );
        gl.clear(Gl::COLOR_BUFFER_BIT | Gl::DEPTH_BUFFER_BIT);
        gl.enable(Gl::DEPTH_TEST);

        self.program.use_program(gl);

        let translation = Matrix4::translation(
            self.scene.model.translation[0],
            self.scene.model.translation[1],
            self.scene.model.translation[2],
        );
        let rotation_z = Matrix4::rotation_z(self.scene.model.rotation[2]);
        let rotation_y = Matrix4::rotation_y(self.scene.model.rotation[1]);
        let rotation_x = Matrix4::rotation_x(self.scene.model.rotation[0]);
        let model_matrix = translation * rotation_z * rotation_y * rotation_x;

        let view_matrix = Matrix4::identity();

        let projection_matrix = Matrix4::perspective(
            std::f32::consts::FRAC_PI_4,
            self.width / self.height,
            0.1,
            100.0,
        );

        let mvp_matrix = projection_matrix * view_matrix * model_matrix;
        let normal_matrix = Matrix3::normal(model_matrix);

        let uniforms = Uniforms {
            mvp_matrix,
            model_matrix,
            normal_matrix,
            light_position: self.scene.light_position,
            camera_position: [0.0; 3],
            shininess: self.scene.model.shininess,
        };
        self.program.set_uniforms(gl, &uniforms);

        self.scene.model.use_model(gl);
        self.scene.model.draw(gl);
    }
}
