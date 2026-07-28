#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(target_arch = "wasm32")]
#[allow(unused_imports)]
pub use browser::*;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Init { user_name: String },
    Message { content: String },
}
