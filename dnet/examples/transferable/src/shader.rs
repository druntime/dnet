use web_sys::{WebGl2RenderingContext as Gl, WebGlProgram, WebGlShader, WebGlUniformLocation};

use crate::math::{Matrix3, Matrix4};

const VERTEX_SHADER: &str = r#"#version 300 es
precision highp float;

in vec3 position;
in vec3 normal;

uniform mat4 mvp_matrix;
uniform mat4 model_matrix;
uniform mat3 normal_matrix;

out vec3 fragment_position;
out vec3 transformed_normal;

void main() {
    vec4 world_position = model_matrix * vec4(position, 1.0);
    fragment_position = world_position.xyz;
    transformed_normal = normal_matrix * normal;
    gl_Position = mvp_matrix * vec4(position, 1.0);
}
"#;

const FRAGMENT_SHADER: &str = r#"#version 300 es
precision highp float;

in vec3 fragment_position;
in vec3 transformed_normal;

uniform vec3 light_position;
uniform vec3 camera_position;
uniform float shininess;

out vec4 fragment_color;

const vec3 LIGHT_COLOR = vec3(1.0, 0.95, 0.88);
const vec3 OBJECT_COLOR = vec3(0.22, 0.48, 0.92);
const vec3 AMBIENT = vec3(0.08, 0.08, 0.14);

void main() {
    vec3 N = normalize(transformed_normal);
    vec3 L = normalize(light_position - fragment_position);
    vec3 V = normalize(camera_position - fragment_position);
    vec3 H = normalize(L + V);

    float diffuse = max(dot(N, L), 0.0);
    float specular = pow(max(dot(N, H), 0.0), shininess);

    vec3 color = AMBIENT + LIGHT_COLOR * (diffuse * OBJECT_COLOR + specular * vec3(1.0));
    fragment_color = vec4(color, 1.0);
}
"#;

pub struct Program {
    pub program: WebGlProgram,
    pub locations: Locations,
}

impl Program {
    pub fn new(gl: &Gl) -> Result<Program, String> {
        let program = create_program(gl)?;
        let locations = Locations::new(gl, &program);
        Ok(Program { program, locations })
    }

    pub fn use_program(&self, gl: &Gl) {
        gl.use_program(Some(&self.program));
    }

    pub fn set_uniforms(&self, gl: &Gl, uniforms: &Uniforms) {
        gl.uniform_matrix4fv_with_f32_array(
            self.locations.mvp_matrix.as_ref(),
            false,
            &uniforms.mvp_matrix.0,
        );
        gl.uniform_matrix4fv_with_f32_array(
            self.locations.model_matrix.as_ref(),
            false,
            &uniforms.model_matrix.0,
        );
        gl.uniform_matrix3fv_with_f32_array(
            self.locations.normal_matrix.as_ref(),
            false,
            &uniforms.normal_matrix.0,
        );
        gl.uniform3f(
            self.locations.light_position.as_ref(),
            uniforms.light_position[0],
            uniforms.light_position[1],
            uniforms.light_position[2],
        );
        gl.uniform3f(
            self.locations.camera_position.as_ref(),
            uniforms.camera_position[0],
            uniforms.camera_position[1],
            uniforms.camera_position[2],
        );
        gl.uniform1f(self.locations.shininess.as_ref(), uniforms.shininess);
    }
}

pub struct Locations {
    pub position: i32,
    pub normal: i32,
    pub mvp_matrix: Option<WebGlUniformLocation>,
    pub model_matrix: Option<WebGlUniformLocation>,
    pub normal_matrix: Option<WebGlUniformLocation>,
    pub light_position: Option<WebGlUniformLocation>,
    pub camera_position: Option<WebGlUniformLocation>,
    pub shininess: Option<WebGlUniformLocation>,
}

#[derive(Debug, Clone, Copy)]
pub struct Uniforms {
    pub mvp_matrix: Matrix4,
    pub model_matrix: Matrix4,
    pub normal_matrix: Matrix3,
    pub light_position: [f32; 3],
    pub camera_position: [f32; 3],
    pub shininess: f32,
}

impl Locations {
    fn new(gl: &Gl, program: &WebGlProgram) -> Locations {
        let position = gl.get_attrib_location(program, "position");
        let normal = gl.get_attrib_location(program, "normal");

        let mvp_matrix = gl.get_uniform_location(program, "mvp_matrix");
        let model_matrix = gl.get_uniform_location(program, "model_matrix");
        let normal_matrix = gl.get_uniform_location(program, "normal_matrix");

        let light_position = gl.get_uniform_location(program, "light_position");
        let camera_position = gl.get_uniform_location(program, "camera_position");
        let shininess = gl.get_uniform_location(program, "shininess");

        Locations {
            position,
            normal,
            mvp_matrix,
            model_matrix,
            normal_matrix,
            light_position,
            camera_position,
            shininess,
        }
    }
}

fn create_program(gl: &Gl) -> Result<WebGlProgram, String> {
    let vert = compile_shader(gl, Gl::VERTEX_SHADER, VERTEX_SHADER)?;
    let frag = compile_shader(gl, Gl::FRAGMENT_SHADER, FRAGMENT_SHADER)?;
    link_program(gl, &vert, &frag)
}

fn compile_shader(gl: &Gl, kind: u32, src: &str) -> Result<WebGlShader, String> {
    let shader = gl.create_shader(kind).ok_or("create_shader failed")?;
    gl.shader_source(&shader, src);
    gl.compile_shader(&shader);
    if gl
        .get_shader_parameter(&shader, Gl::COMPILE_STATUS)
        .as_bool()
        .unwrap_or(false)
    {
        Ok(shader)
    } else {
        Err(gl.get_shader_info_log(&shader).unwrap_or_default())
    }
}

fn link_program(gl: &Gl, vert: &WebGlShader, frag: &WebGlShader) -> Result<WebGlProgram, String> {
    let prog = gl.create_program().ok_or("create_program failed")?;
    gl.attach_shader(&prog, vert);
    gl.attach_shader(&prog, frag);
    gl.link_program(&prog);
    if gl
        .get_program_parameter(&prog, Gl::LINK_STATUS)
        .as_bool()
        .unwrap_or(false)
    {
        Ok(prog)
    } else {
        Err(gl.get_program_info_log(&prog).unwrap_or_default())
    }
}
