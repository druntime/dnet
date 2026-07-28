#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum Message {
    Init { name_already_taken: bool },
    UserConnected { user_name: String },
    UserDisconnected { user_name: String },
    Message { user_name: String, content: String },
}
