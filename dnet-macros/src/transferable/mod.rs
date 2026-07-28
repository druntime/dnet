mod enum_;
mod struct_;

#[cfg(test)]
mod tests;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Error, Field, GenericParam, Generics, Ident, Path, Token, Type, TypeGenerics, TypeParam, TypePath, TypeTuple, Visibility, WhereClause, WherePredicate, parse_quote, parse2, punctuated::Punctuated, token::Comma,
};

use crate::{
    transferable::{enum_::expand_enum, struct_::expand_struct},
    utils::{
        actual_new_param_name, base_path, crate_path, extract_codec_param_name,
        is_into_transferable_attribute, is_transferable_attribute, js_path, utils_path,
    },
};

struct CommonParams {
    paths: Paths,
    visibility: Visibility,
    idents: Idents,
    generics: Generics,
    phantom: Vec<Ident>,
    wrapper: Wrapper,
    codec: Ident,
}

struct Paths {
    dnet_base: Path,
    dnet_js: Path,
    dnet_utils: Path,
    serde: Path,
    wasm_bindgen: Path,
    web_sys: Path,
}

impl Paths {
    fn default() -> syn::Result<Self> {
        Ok(Paths {
            dnet_base: base_path()?,
            dnet_js: js_path()?,
            dnet_utils: utils_path()?,
            serde: crate_path("serde")?,
            wasm_bindgen: crate_path("wasm-bindgen")?,
            web_sys: crate_path("web-sys")?,
        })
    }
}

struct Idents {
    item: Ident,
    into_transferables: Ident,
    stripped: Ident,
    wrapper: Ident,
}

struct Fields {
    names: Vec<Ident>,
    stripped: Punctuated<Field, Token![,]>,
    stripped_names: Vec<Ident>,
    transferables_names: Vec<Ident>,
    into_transferables: Punctuated<Field, Token![,]>,
    into_transferables_names: Vec<Ident>,
    were_unnamed: bool,
}

struct Wrapper {
    ident: Ident,
    generics: Generics,
}

pub fn into_transferable(input: DeriveInput) -> syn::Result<TokenStream> {
    let paths = Paths::default()?;
    into_transferable_with_paths(input, paths)
}

fn into_transferable_with_paths(input: DeriveInput, paths: Paths) -> syn::Result<TokenStream> {
    let visibility = input.vis;

    let idents = Idents {
        item: input.ident.clone(),
        into_transferables: format_ident!("{}IntoTransferables", input.ident),
        stripped: format_ident!("{}Stripped", input.ident),
        wrapper: format_ident!("{}Wrapper", input.ident),
    };

    let generics = input.generics;

    let phantom: Vec<Ident> = generics
        .type_params()
        .map(|param| param.ident.clone())
        .collect();

    let mut append_codec_to_generics = false;
    let codec = extract_codec_param_name(&generics.where_clause.as_ref())?.unwrap_or_else(|| {
        append_codec_to_generics = true;
        actual_new_param_name(&generics, "C")
    });

    let mut wrapper_generics = generics.clone();
    if append_codec_to_generics {
        wrapper_generics.params.push(GenericParam::Type(TypeParam {
            attrs: vec![],
            ident: codec.clone(),
            colon_token: None,
            bounds: Punctuated::new(),
            default: None,
        }));
        let where_clause = wrapper_generics.where_clause.get_or_insert(WhereClause {
            where_token: Default::default(),
            predicates: Punctuated::new(),
        });
        let dnet_base = &paths.dnet_base;
        let codec_predicate = quote! {
            #codec: #dnet_base::Codec
        };
        let codec_predicate: WherePredicate =
            parse2(codec_predicate).expect("failed to parse codec predicate");
        where_clause.predicates.push(codec_predicate);
    }

    let wrapper = Wrapper {
        ident: idents.wrapper.clone(),
        generics: wrapper_generics,
    };

    let params = CommonParams {
        paths,
        visibility,
        idents,
        generics,
        phantom,
        wrapper,
        codec,
    };

    match input.data {
        Data::Enum(data_enum) => expand_enum(params, data_enum.variants),
        Data::Struct(data_struct) => expand_struct(params, data_struct.fields),
        Data::Union(_) => Err(Error::new_spanned(
            &params.idents.item,
            "unions are not supported in #[derive(IntoTransferable)]",
        )),
    }
}

fn impl_into_and_from_transferable(
    paths: &Paths,
    item_ident: &Ident,
    item_generics: &Generics,
    wrapper: &Wrapper,
    codec_param: &Ident,
) -> TokenStream {
    let Paths {
        dnet_base,
        dnet_js,
        dnet_utils,
        ..
    } = paths;

    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, item_generics, _) = item_generics.split_for_impl();

    let (wrapper_impl_generics, wrapper_generics, wrapper_where_clause) =
        wrapper_generics.split_for_impl();

    quote! {
        impl #wrapper_impl_generics #dnet_js::IntoTransferable<
            #dnet_js::wrapper::Context<#codec_param>,
            #dnet_js::wrapper::Error<
                <#codec_param as #dnet_base::Encode>::Error,
                <#codec_param as #dnet_base::Decode>::Error
            >
        > for #item_ident #item_generics
        #wrapper_where_clause
        {
            type Output = #wrapper_ident #wrapper_generics;

            fn into_transferable(self) -> Self::Output {
                #wrapper_ident::from(self)
            }
        }

        impl #wrapper_impl_generics #dnet_js::FromTransferable<
            #dnet_js::wrapper::Context<#codec_param>,
            #dnet_js::wrapper::Error<
                <#codec_param as #dnet_base::Encode>::Error,
                <#codec_param as #dnet_base::Decode>::Error
            >
        > for #item_ident #item_generics
        #wrapper_where_clause
        {
            type Input = #wrapper_ident #wrapper_generics;

            fn from_transferable(input: Self::Input) -> Self {
                use #dnet_utils::unwrap::Unwrap;
                input.unwrap()
            }
        }
    }
}

fn process_fields(
    fields: syn::Fields,
    phantom_params: &[Ident],
    codec_param: &Ident,
) -> syn::Result<Fields> {
    let mut names = vec![];
    let mut stripped = Punctuated::new();
    let mut transferables_names = vec![];
    let mut stripped_names = vec![];
    let mut into_transferables = Punctuated::new();
    let mut into_transferables_names = vec![];
    let mut were_unnamed = false;
    for (index, mut field) in fields.into_iter().enumerate() {
        if field.ident.is_none() {
            were_unnamed = true;
            field.ident = Some(format_ident!("unnamed_field_{index}"));
        }
        let ident = field.ident.clone().unwrap();
        names.push(ident.clone());
        let is_transferable = field.attrs.iter().any(is_transferable_attribute);
        let is_into_transferable = field.attrs.iter().any(is_into_transferable_attribute);
        let is_both = is_transferable && is_into_transferable;
        if is_both {
            return Err(Error::new_spanned(
                field,
                "a field cannot be both #[transferable] and #[into_transferable]",
            ));
        } else if is_transferable {
            transferables_names.push(ident.clone());
        } else if is_into_transferable {
            field
                .attrs
                .retain(|attribute| !is_into_transferable_attribute(attribute));
            into_transferables.push(field);
            into_transferables_names.push(ident.clone());
        } else {
            stripped.push(field);
            stripped_names.push(ident);
        }
    }
    stripped.push(phantom(phantom_params));

    let mut phantom_params_with_codec = phantom_params.to_vec();
    phantom_params_with_codec.push(codec_param.clone());
    into_transferables.push(phantom(&phantom_params_with_codec));

    Ok(Fields {
        names,
        stripped,
        stripped_names,
        transferables_names,
        into_transferables,
        into_transferables_names,
        were_unnamed,
    })
}

fn phantom(params: &[Ident]) -> Field {
    let ident = format_ident!("_phantom_data__");

    let elems: Punctuated<Type, Comma> = params
        .iter()
        .map(|param| {
            Type::Path(TypePath {
                attrs: Default::default(),
                qself: None,
                path: param.clone().into(),
            })
        })
        .collect();

    let tuple = Type::Tuple(TypeTuple {
        attrs: Default::default(),
        paren_token: Default::default(),
        elems,
    });

    let ty = Type::Path(TypePath {
        attrs: Default::default(),
        qself: None,
        path: parse_quote! {
            std::marker::PhantomData<#tuple>
        },
    });

    Field {
        attrs: vec![],
        vis: Visibility::Inherited,
        modifiers: Default::default(),
        ident: Some(ident),
        colon_token: Some(Default::default()),
        ty,
        default: None,
    }
}

fn pack_transferables(transferables_names: &[Ident]) -> TokenStream {
    let transferables = transferables_names.iter().map(|name| {
        quote! {
            #name.into()
        }
    });
    quote! {
        let transferables__ = vec![
            #(#transferables,)*
        ];
    }
}

fn unpack_transferables(transferables_names: &[Ident]) -> TokenStream {
    // note: we are popping from vec - so reconstruct in reverse order
    let transferables = transferables_names.iter().rev().map(|name| {
        quote! {
            let #name = transferables__
                .pop()
                .expect("malformed transferables")
                .dyn_into()
                .expect("malformed transferables");
        }
    });
    quote! {
        #(#transferables)*
    }
}

#[allow(clippy::too_many_arguments)]
fn wrapper(
    paths: &Paths,
    visibility: &Visibility,
    item_generics: &TypeGenerics,
    item_where_clause: &Option<&WhereClause>,
    stripped_ident: &Ident,
    into_transferables_ident: &Option<&Ident>,
    wrapper: &Wrapper,
    codec_param: &Ident,
) -> TokenStream {
    let Paths { wasm_bindgen, .. } = paths;
    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, wrapper_generics, _) = wrapper_generics.split_for_impl();

    let into_transferables = if let Some(ident) = into_transferables_ident {
        quote! {
            into_transferables: #ident #wrapper_generics,
        }
    } else {
        quote! {}
    };

    quote! {
        #visibility struct #wrapper_ident #wrapper_generics #item_where_clause {
            stripped: #stripped_ident #item_generics,
            transferables: Vec<#wasm_bindgen::JsValue>,
            #into_transferables
            _codec: std::marker::PhantomData<#codec_param>,
        }
    }
}

fn impl_transferable_for_wrapper(
    paths: &Paths,
    wrapper: &Wrapper,
    codec_param: &Ident,
    into_transferables_ident: &Option<&Ident>,
) -> TokenStream {
    let Paths {
        dnet_base,
        dnet_js,
        wasm_bindgen,
        web_sys,
        ..
    } = paths;
    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (wrapper_impl_generics, wrapper_generics, wrapper_where_clause) =
        wrapper_generics.split_for_impl();

    let (pack_into_transferables, unpack_into_transferables, into_transferables_field) =
        if let Some(into_transferables_ident) = into_transferables_ident {
            (
                quote! {
                    let into_transferables = self.into_transferables.pack(context)?;

                    Reflect::set(&data, &JsValue::from_str("into_transferables"), &into_transferables.data).unwrap();
                    let transfer = transfer.concat(&into_transferables.transfer);
                },
                quote! {
                    let into_transferables = Reflect::get(&object, &JsValue::from_str("into_transferables"))
                        .map_err(|_| Error::WrongType)?;
                    let into_transferables = #into_transferables_ident::unpack(into_transferables, context)?;
                },
                quote! {
                    into_transferables,
                },
            )
        } else {
            (quote! {}, quote! {}, quote! {})
        };

    quote! {
        impl #wrapper_impl_generics #dnet_js::Transferable for #wrapper_ident #wrapper_generics
        #wrapper_where_clause
        {
            type Context = #dnet_js::wrapper::Context<#codec_param>;

            type Error = #dnet_js::wrapper::Error<
                <#codec_param as #dnet_base::Encode>::Error,
                <#codec_param as #dnet_base::Decode>::Error
            >;

            fn prepare_for_transfer(
                mut self,
                context: &mut Self::Context,
            ) -> Result<#dnet_js::WithTransferable, Self::Error> {
                use #dnet_js::wrapper::Error;
                use #wasm_bindgen::JsValue;
                use #web_sys::js_sys::{Array, Object, Reflect, Uint8Array};

                context.buffer.clear();
                context
                    .codec
                    .encode(&mut context.buffer, &self.stripped)
                    .map_err(Error::SerializationError)?;
                let stripped = Uint8Array::from(&context.buffer[..]);

                let transferables = Array::of(&self.transferables);

                let data = Object::new();
                Reflect::set(&data, &JsValue::from_str("stripped"), &stripped).unwrap();
                Reflect::set(&data, &JsValue::from_str("transferables"), &transferables).unwrap();

                let mut transfer = self.transferables;
                transfer.push(stripped.buffer().into());

                let transfer = Array::of(&transfer);

                #pack_into_transferables

                Ok(#dnet_js::WithTransferable {
                    data: data.into(),
                    transfer,
                })
            }

            fn reconstruct(object: #wasm_bindgen::JsValue, context: &mut Self::Context) -> Result<Self, Self::Error>
            where
                Self: Sized,
            {
                use #dnet_js::wrapper::Error;
                use #wasm_bindgen::{JsCast, JsValue};
                use #web_sys::js_sys::{Array, Reflect, Uint8Array};

                let stripped = Reflect::get(&object, &JsValue::from_str("stripped"))
                    .map_err(|_| Error::WrongType)?
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| Error::WrongType)?;

                let stripped = context
                    .codec
                    .decode(&stripped.to_vec()[..])
                    .map_err(Error::DeserializationError)?;

                let transferables = Reflect::get(&object, &JsValue::from_str("transferables"))
                    .map_err(|_| Error::WrongType)?
                    .dyn_into::<Array>()
                    .map_err(|_| Error::WrongType)?;

                #unpack_into_transferables

                Ok(Self {
                    stripped,
                    transferables: transferables.to_vec(),
                    #into_transferables_field
                    _codec: std::marker::PhantomData,
                })
            }
        }
    }
}
