use syn::Path;

use crate::utils::{crate_path, js_path, rpc_path};

pub mod api;
pub mod consumer;
pub mod producer;
pub mod request;
pub mod response;

pub(crate) struct Paths {
    pub dnet_rpc: Path,
    pub dnet_js: Path,
    pub serde: Path,
}

impl Paths {
    fn default() -> syn::Result<Self> {
        Ok(Paths {
            dnet_rpc: rpc_path()?,
            dnet_js: js_path()?,
            serde: crate_path("serde")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SerializationConfig {
    Serde(SerdeConfig),
    IntoTransferable,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        SerializationConfig::Serde(SerdeConfig::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SerdeConfig {
    pub serialize: bool,
    pub deserialize: bool,
}

impl SerdeConfig {
    pub const fn all() -> Self {
        Self {
            serialize: true,
            deserialize: true,
        }
    }

    pub const fn none() -> Self {
        Self {
            serialize: false,
            deserialize: false,
        }
    }
}

impl Default for SerdeConfig {
    fn default() -> Self {
        Self::all()
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;
    use syn::{parse2, ItemTrait};

    use crate::{rpc::Paths, utils::make_path};

    pub fn paths() -> Paths {
        Paths {
            dnet_rpc: make_path(&["dnet_rpc"]),
            dnet_js: make_path(&["dnet_js"]),
            serde: make_path(&["serde"]),
        }
    }

    pub fn parsed_api() -> ItemTrait {
        parse2::<ItemTrait>(api()).unwrap()
    }

    pub fn parsed_api_with_transferable() -> ItemTrait {
        parse2::<ItemTrait>(api_with_transferable()).unwrap()
    }

    fn api() -> TokenStream {
        quote! {
            /// Public api.
            pub trait Api {
                /// Print "Hello World!" message on the server.
                #[no_ack]
                async fn hello_world(&self);

                /// Wait for an acknowledgement from the producer.
                async fn wait_for_ack(&self);

                /// Add two integers.
                async fn add_numbers(&self, a: i32, b: i32) -> i32;

                /// Concatenate two strings.
                async fn concatenate_strings(&self, a: String, b: String) -> String;

                /// Create stream of consequent natural numbers.
                async fn stream_natural_numbers(&self) -> impl Stream<Item = usize>;

                /// Keep sending server time at given interval.
                async fn stream_time(&self, interval: Duration) -> impl Stream<Item = NaiveTime>;

                /// Abortable long-running task.
                #[abortable]
                async fn long_running_task(&self, input: u32) -> String;
            }
        }
    }

    fn api_with_transferable() -> TokenStream {
        quote! {
            /// Public api.
            pub trait Api {
                /// Send OffscreenCanvas to the worker.
                async fn send_transferable(&self, #[transferable] data: OffscreenCanvas);

                /// Get data from worker.
                #[into_transferable]
                async fn get_data(&self) -> Data;
            }
        }
    }
}
