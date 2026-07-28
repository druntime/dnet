//! Macros used by `dnet`.

mod rpc;
mod transferable;
mod utils;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_derive(IntoTransferable, attributes(transferable, into_transferable))]
pub fn derive_into_transferable(input: TokenStream) -> TokenStream {
    transferable::into_transferable(parse_macro_input!(input))
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn api(_attr: TokenStream, item: TokenStream) -> TokenStream {
    rpc::api::api(item.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn no_serde(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn no_ack(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn abortable(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_derive(Produce)]
pub fn produce(input: TokenStream) -> TokenStream {
    rpc::producer::produce(parse_macro_input!(input))
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

#[cfg(test)]
mod tests {
    use prettyplease::unparse;
    use proc_macro2::TokenStream;
    use syn::parse_file;

    pub fn pretty(item: TokenStream) -> String {
        let file = parse_file(&item.to_string()).unwrap();
        unparse(&file)
    }

    pub fn compare(expected: TokenStream, actual: TokenStream) {
        let expected = pretty(expected);
        let actual = pretty(actual);

        if expected != actual {
            panic!("\n\nEXPECTED:\n\n{expected}\n\n\nFOUND:\n\n{actual}\n");
        }
    }
}
