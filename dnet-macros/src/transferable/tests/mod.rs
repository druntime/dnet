use crate::{transferable::Paths, utils::make_path};

mod enum_;
mod struct_;

fn paths() -> Paths {
    Paths {
        dnet_base: make_path(&["dnet"]),
        dnet_js: make_path(&["dnet", "js"]),
        dnet_utils: make_path(&["dnet", "utils"]),
        serde: make_path(&["serde"]),
        wasm_bindgen: make_path(&["wasm_bindgen"]),
        web_sys: make_path(&["web_sys"]),
    }
}
