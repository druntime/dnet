use std::ops::Mul;

use crate::renderer::Mesh;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Matrix4(pub [f32; 16]);

impl Matrix4 {
    pub fn identity() -> Self {
        let mut m = [0f32; 16];
        m[0] = 1.0;
        m[5] = 1.0;
        m[10] = 1.0;
        m[15] = 1.0;
        Matrix4(m)
    }

    pub fn perspective(fov_y: f32, aspect: f32, near: f32, far: f32) -> Self {
        let t = (fov_y / 2.0).tan();
        let mut m = [0f32; 16];
        m[0] = 1.0 / (aspect * t);
        m[5] = 1.0 / t;
        m[10] = -(far + near) / (far - near);
        m[11] = -1.0;
        m[14] = -2.0 * far * near / (far - near);
        Matrix4(m)
    }

    pub fn translation(x: f32, y: f32, z: f32) -> Self {
        let mut m = Matrix4::identity().0;
        m[12] = x;
        m[13] = y;
        m[14] = z;
        Matrix4(m)
    }

    pub fn rotation_z(a: f32) -> Self {
        let (s, c) = a.sin_cos();
        let mut m = Matrix4::identity().0;
        m[0] = c;
        m[1] = s;
        m[4] = -s;
        m[5] = c;
        Matrix4(m)
    }

    pub fn rotation_y(a: f32) -> Self {
        let (s, c) = a.sin_cos();
        let mut m = Matrix4::identity().0;
        m[0] = c;
        m[2] = s;
        m[8] = -s;
        m[10] = c;
        Matrix4(m)
    }

    pub fn rotation_x(a: f32) -> Self {
        let (s, c) = a.sin_cos();
        let mut m = Matrix4::identity().0;
        m[5] = c;
        m[6] = -s;
        m[9] = s;
        m[10] = c;
        Matrix4(m)
    }
}

impl Mul for Matrix4 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        let mut out = [0f32; 16];
        for i in 0..4 {
            for j in 0..4 {
                out[i + j * 4] = (0..4).map(|k| self.0[i + k * 4] * rhs.0[k + j * 4]).sum();
            }
        }
        Matrix4(out)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Matrix3(pub [f32; 9]);

impl Matrix3 {
    pub fn normal(m: Matrix4) -> Self {
        let m = m.0;
        // Upper-left 3×3 of a rotation/scale matrix — works correctly when there
        // is no non-uniform scale (i.e. pure rotation, which is our case).
        Matrix3([m[0], m[1], m[2], m[4], m[5], m[6], m[8], m[9], m[10]])
    }
}

pub struct Cube {
    positions: Vec<f32>,
    normals: Vec<f32>,
    indices: Vec<u16>,
}

impl Cube {
    pub fn new() -> Cube {
        // (normal, four CCW vertices)
        let faces: &[([f32; 3], [[f32; 3]; 4])] = &[
            (
                [1., 0., 0.],
                [[1., -1., -1.], [1., 1., -1.], [1., 1., 1.], [1., -1., 1.]],
            ),
            (
                [-1., 0., 0.],
                [
                    [-1., -1., 1.],
                    [-1., 1., 1.],
                    [-1., 1., -1.],
                    [-1., -1., -1.],
                ],
            ),
            (
                [0., 1., 0.],
                [[-1., 1., -1.], [1., 1., -1.], [1., 1., 1.], [-1., 1., 1.]],
            ),
            (
                [0., -1., 0.],
                [
                    [-1., -1., 1.],
                    [1., -1., 1.],
                    [1., -1., -1.],
                    [-1., -1., -1.],
                ],
            ),
            (
                [0., 0., 1.],
                [[-1., -1., 1.], [1., -1., 1.], [1., 1., 1.], [-1., 1., 1.]],
            ),
            (
                [0., 0., -1.],
                [
                    [1., -1., -1.],
                    [-1., -1., -1.],
                    [-1., 1., -1.],
                    [1., 1., -1.],
                ],
            ),
        ];

        let mut positions = Vec::new();
        let mut normals = Vec::new();
        let mut indices = Vec::new();
        let mut vi: u16 = 0;

        for (norm, verts) in faces {
            for v in verts {
                positions.extend_from_slice(v);
                normals.extend_from_slice(norm);
            }
            indices.extend_from_slice(&[vi, vi + 1, vi + 2, vi, vi + 2, vi + 3]);
            vi += 4;
        }

        Cube {
            positions,
            normals,
            indices,
        }
    }
}

impl Mesh for Cube {
    fn vertices(&self) -> &[f32] {
        &self.positions
    }

    fn normals(&self) -> &[f32] {
        &self.normals
    }

    fn indices(&self) -> &[u16] {
        &self.indices
    }
}
