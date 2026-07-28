use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::{
    punctuated::Punctuated,
    spanned::Spanned,
    token::{Comma, PathSep},
    Attribute, Error, FnArg, GenericArgument, GenericParam, Generics, ItemTrait, Pat, PatIdent,
    Path, PathArguments, PathSegment, ReturnType, TraitItemFn, Type, TypeImplTrait, TypeParamBound,
    WhereClause, WherePredicate,
};

use crate::rpc::{self, SerializationConfig};

pub fn crate_path(crate_name: &str) -> syn::Result<Path> {
    if let Ok(found) = proc_macro_crate::crate_name(crate_name) {
        let ident = match found {
            FoundCrate::Itself => "crate".to_string(),
            FoundCrate::Name(name) => name,
        };

        return Ok(make_path(&[&ident]));
    }

    Err(Error::new(
        Span::call_site(),
        format!("crate `{crate_name}` not found in Cargo.toml"),
    ))
}

pub fn base_path() -> syn::Result<Path> {
    dnet_dependency_path("base")
}

pub fn js_path() -> syn::Result<Path> {
    dnet_dependency_path("js")
}

pub fn utils_path() -> syn::Result<Path> {
    dnet_dependency_path("utils")
}

pub fn rpc_path() -> syn::Result<Path> {
    dnet_dependency_path("rpc")
}

pub fn dnet_dependency_path(name: &str) -> syn::Result<Path> {
    if let Ok(found) = crate_name("dnet") {
        let ident = match found {
            FoundCrate::Itself => "dnet".to_string(),
            FoundCrate::Name(name) => name,
        };

        return Ok(if name == "base" {
            make_path(&[&ident])
        } else {
            make_path(&[&ident, name])
        });
    }

    let dependency_name = format!("dnet-{name}");
    if let Ok(found) = crate_name(&dependency_name) {
        let ident = match found {
            FoundCrate::Itself => "crate".to_string(),
            FoundCrate::Name(name) => name,
        };

        return Ok(make_path(&[&ident]));
    }

    Err(Error::new(
        Span::call_site(),
        format!("neither `dnet` nor `{dependency_name}` found in Cargo.toml"),
    ))
}

pub fn make_path(segments: &[&str]) -> Path {
    let leading_colon = if segments[0] == "crate" {
        None
    } else {
        Some(PathSep::default())
    };
    Path {
        leading_colon,
        segments: segments
            .iter()
            .map(|segment| PathSegment::from(Ident::new(segment, Span::call_site())))
            .collect::<Punctuated<_, _>>(),
    }
}

pub fn skip_self(arguments: &Punctuated<FnArg, Comma>) -> Punctuated<FnArg, Comma> {
    let mut output = Punctuated::new();
    for arg in arguments {
        if let FnArg::Typed(pat_type) = &arg {
            if let Pat::Ident(PatIdent { ident, .. }) = &*pat_type.pat {
                if ident != "self" {
                    output.push(arg.clone());
                }
            }
        }
    }
    output
}

pub fn extract_return_type(return_type: &ReturnType) -> syn::Result<TokenStream> {
    Ok(match return_type {
        ReturnType::Default => {
            quote! {
                ()
            }
        }
        ReturnType::Type(_, return_type) => match *return_type.to_owned() {
            Type::ImplTrait(impl_trait) => {
                let return_type = extract_stream_item_type(&impl_trait)?;
                quote! {
                    #return_type
                }
            }
            _ => {
                quote! {
                    #return_type
                }
            }
        },
    })
}

pub fn extract_stream_item_type(impl_trait: &TypeImplTrait) -> syn::Result<Type> {
    if impl_trait.bounds.len() == 1 {
        if let TypeParamBound::Trait(bound) = &impl_trait.bounds[0] {
            if bound.path.segments.len() == 1 {
                let stream = &bound.path.segments[0];
                if stream.ident == "Stream" {
                    if let PathArguments::AngleBracketed(arguments) = &stream.arguments {
                        if arguments.args.len() == 1 {
                            let argument = &arguments.args[0];
                            if let GenericArgument::AssocType(binding) = argument {
                                if binding.ident == "Item" {
                                    return Ok(binding.ty.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Err(Error::new_spanned(
        impl_trait,
        "invalid stream request method return type",
    ))
}

pub fn extract_codec_param_name(where_clause: &Option<&WhereClause>) -> syn::Result<Option<Ident>> {
    let Some(where_clause) = where_clause else {
        return Ok(None);
    };
    let mut found: Option<Ident> = None;

    let trait_name = "WrapperLikeTransferable";

    for predicate in &where_clause.predicates {
        let WherePredicate::Type(pred) = predicate else {
            continue;
        };

        for bound in &pred.bounds {
            let TypeParamBound::Trait(trait_bound) = bound else {
                continue;
            };

            let Some(segment) = trait_bound.path.segments.last() else {
                continue;
            };

            if segment.ident != trait_name {
                continue;
            }

            let PathArguments::AngleBracketed(args) = &segment.arguments else {
                return Err(Error::new(
                    segment.span(),
                    format!("{trait_name} must have exactly one type parameter"),
                ));
            };

            let ident = match args.args.iter().collect::<Vec<_>>().as_slice() {
                [GenericArgument::Type(Type::Path(type_path))]
                    if type_path.qself.is_none()
                        && type_path.path.segments.len() == 1
                        && matches!(type_path.path.segments[0].arguments, PathArguments::None) =>
                {
                    type_path.path.segments[0].ident.clone()
                }

                [GenericArgument::Type(ty)] => {
                    return Err(Error::new(
                        ty.span(),
                        format!("{trait_name} argument must be a simple type identifier"),
                    ));
                }

                _ => {
                    return Err(Error::new(
                        args.span(),
                        format!("{trait_name} must have exactly one type parameter"),
                    ));
                }
            };

            match &found {
                None => found = Some(ident),
                Some(existing) if existing == &ident => {}
                Some(existing) => {
                    return Err(Error::new(
                        ident.span(),
                        format!(
                            "conflicting {trait_name} type arguments: `{}` vs `{}`",
                            existing, ident,
                        ),
                    ));
                }
            }
        }
    }

    Ok(found)
}

pub fn actual_new_param_name(generics: &Generics, desired_name: &str) -> Ident {
    let mut index = 2;
    let mut tested_ident = format_ident!("{desired_name}");
    loop {
        if generics.params.iter().any(|param| {
            let ident = match param {
                GenericParam::Lifetime(lifetime_param) => &lifetime_param.lifetime.ident,
                GenericParam::Type(type_param) => &type_param.ident,
                GenericParam::Const(const_param) => &const_param.ident,
            };
            ident == &tested_ident
        }) {
            tested_ident = format_ident!("{desired_name}{index}");
            index += 1;
        } else {
            return tested_ident;
        }
    }
}

pub fn is_stream(return_type: &ReturnType) -> bool {
    match return_type {
        ReturnType::Default => false,
        ReturnType::Type(_, return_type) => {
            matches!(*return_type.to_owned(), Type::ImplTrait(_))
        }
    }
}

pub fn patternize_arguments(arguments: &Punctuated<FnArg, Comma>) -> Punctuated<Ident, Comma> {
    let mut output = Punctuated::new();
    for arg in arguments {
        if let FnArg::Typed(pat_type) = &arg {
            if let Pat::Ident(PatIdent { ident, .. }) = &*pat_type.pat {
                if ident != "self" {
                    output.push(ident.clone());
                }
            }
        }
    }

    output
}

pub fn request_response_derive(paths: &rpc::Paths, config: SerializationConfig) -> TokenStream {
    let rpc::Paths { dnet_js, serde, .. } = paths;

    match config {
        SerializationConfig::Serde(config) => match (config.serialize, config.deserialize) {
            (true, true) => {
                quote! { #[derive(Debug, Clone, #serde::Serialize, #serde::Deserialize)] }
            }
            (true, false) => quote! { #[derive(Debug, Clone, #serde::Serialize)] },
            (false, true) => quote! { #[derive(Debug, Clone, #serde::Deserialize)] },
            (false, false) => quote! { #[derive(Debug, Clone)] },
        },
        SerializationConfig::IntoTransferable => {
            quote! { #[derive(Debug, Clone, #dnet_js::IntoTransferable)] }
        }
    }
}

pub fn no_serde_attribute(attribute: &[Attribute]) -> syn::Result<Option<&Attribute>> {
    let no_serde_attributes: Vec<&Attribute> = attribute
        .iter()
        .filter(|attr| is_no_serde_attribute(attr))
        .collect();
    if no_serde_attributes.is_empty() {
        Ok(None)
    } else if no_serde_attributes.len() == 1 {
        Ok(Some(no_serde_attributes[0]))
    } else {
        let create_error = |span| {
            Error::new(
                span,
                "multiple #[no_serde] attributes found, only one (or none) is allowed",
            )
        };
        let mut error = create_error(no_serde_attributes[0].span());
        for attribute in no_serde_attributes.iter().skip(1) {
            error.combine(create_error(attribute.span()));
        }
        Err(error)
    }
}

pub fn has_methods_with_transferable(item: &ItemTrait) -> bool {
    for item in &item.items {
        if let syn::TraitItem::Fn(method) = item {
            if method.attrs.iter().any(is_transferable_attribute)
                || method.attrs.iter().any(is_into_transferable_attribute)
            {
                return true;
            }

            for arg in &method.sig.inputs {
                if let FnArg::Typed(pat_type) = arg {
                    if pat_type.attrs.iter().any(is_transferable_attribute)
                        || pat_type.attrs.iter().any(is_into_transferable_attribute)
                    {
                        return true;
                    }
                }
            }
        }
    }

    false
}

pub fn remove_transferable_attributes(attributes: &mut Vec<Attribute>) {
    attributes.retain(|attribute| {
        !(is_transferable_attribute(attribute) || is_into_transferable_attribute(attribute))
    });
}

pub fn is_no_ack(method: &TraitItemFn) -> syn::Result<bool> {
    let is_no_ack = method.attrs.iter().any(is_no_ack_attribute);
    if is_no_ack && !matches!(method.sig.output, ReturnType::Default) {
        Err(syn::Error::new_spanned(
            &method.sig,
            "#[no_ack] functions must not have a return type",
        ))?;
    }
    Ok(is_no_ack)
}

pub fn is_no_ack_attribute(attribute: &Attribute) -> bool {
    is_dnet_attribute(attribute, "no_ack")
}

pub fn is_abortable(method: &TraitItemFn) -> bool {
    method.attrs.iter().any(is_abortable_attribute)
}

pub fn is_abortable_attribute(attribute: &Attribute) -> bool {
    is_dnet_attribute(attribute, "abortable")
}

pub fn is_no_serde_attribute(attribute: &Attribute) -> bool {
    is_dnet_attribute(attribute, "no_serde")
}

pub fn is_transferable_attribute(attribute: &Attribute) -> bool {
    is_dnet_attribute(attribute, "transferable")
}

pub fn is_into_transferable_attribute(attribute: &Attribute) -> bool {
    is_dnet_attribute(attribute, "into_transferable")
}

pub fn is_dnet_attribute(attribute: &Attribute, ident: &str) -> bool {
    let path = attribute.path();
    let ident = format_ident!("{ident}");
    if path.is_ident(&ident) {
        true
    } else {
        let mut punctuated = Punctuated::new();
        punctuated.push(PathSegment {
            ident: format_ident!("dnet_rpc"),
            arguments: PathArguments::None,
        });
        punctuated.push(PathSegment {
            ident: ident.clone(),
            arguments: PathArguments::None,
        });
        if path.segments == punctuated {
            true
        } else {
            let mut punctuated = Punctuated::new();
            punctuated.push(PathSegment {
                ident: format_ident!("dnet"),
                arguments: PathArguments::None,
            });
            punctuated.push(PathSegment {
                ident: format_ident!("rpc"),
                arguments: PathArguments::None,
            });
            punctuated.push(PathSegment {
                ident,
                arguments: PathArguments::None,
            });
            path.segments == punctuated
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::{format_ident, quote};
    use syn::{parse2, ItemStruct, TraitItemFn};

    use crate::utils::{
        actual_new_param_name, is_abortable, is_no_ack, is_stream, patternize_arguments,
    };

    use super::{extract_return_type, skip_self};

    #[test]
    fn test_skip_self() {
        let expected = quote! {
            a: u32, b: String
        };

        let signature = quote! {
            fn some_function(&self, a: u32, b: String) -> i32;
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let item = skip_self(&signature.sig.inputs);

        let actual = quote! {
            #item
        };

        assert_eq!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_extract_return_type_default() {
        let expected = quote! {
            ()
        };

        let signature = quote! {
            fn some_function(&self, a: u32, b: String);
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let return_type = extract_return_type(&signature.sig.output).unwrap();

        assert_eq!(expected.to_string(), return_type.to_string());
    }

    #[test]
    fn test_extract_return_type_regular() {
        let expected = quote! {
            i32
        };

        let signature = quote! {
            fn some_function(&self, a: u32, b: String) -> i32;
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let return_type = extract_return_type(&signature.sig.output).unwrap();

        assert_eq!(expected.to_string(), return_type.to_string());
    }

    #[test]
    fn test_extract_return_type_stream() {
        let expected = quote! {
            usize
        };

        let signature = quote! {
            fn some_function(&self, a: u32, b: String) -> impl Stream<Item = usize>;
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let return_type = extract_return_type(&signature.sig.output).unwrap();

        assert_eq!(expected.to_string(), return_type.to_string());
    }

    #[test]
    fn test_actual_new_param_name() {
        let test_struct = quote! {
            struct SomeStruct {
                some_vec: Vec<usize>,
            }
        };
        let parsed = parse2::<ItemStruct>(test_struct).unwrap();
        let actual = actual_new_param_name(&parsed.generics, "C");
        assert_eq!(actual, format_ident!("C"));

        let test_struct = quote! {
            struct SomeGenericStruct<C> {
                some_vec: Vec<C>,
            }
        };
        let parsed = parse2::<ItemStruct>(test_struct).unwrap();
        let actual = actual_new_param_name(&parsed.generics, "C");
        assert_eq!(actual, format_ident!("C2"));

        let test_struct = quote! {
            struct SomeGenericStruct<C, C2> {
                some_vec: Vec<C>,
                some_other_vec: Vec<C2>,
            }
        };
        let parsed = parse2::<ItemStruct>(test_struct).unwrap();
        let actual = actual_new_param_name(&parsed.generics, "C");
        assert_eq!(actual, format_ident!("C3"));
    }

    #[test]
    fn test_is_stream_false() {
        let signature = quote! {
            fn some_function(&self, a: u32, b: String) -> i32;
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let is_stream = is_stream(&signature.sig.output);

        assert!(!is_stream);
    }

    #[test]
    fn test_is_stream_true() {
        let signature = quote! {
            fn some_function(&self, a: u32, b: String) -> impl Stream<Item = i32>;
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let is_stream = is_stream(&signature.sig.output);

        assert!(is_stream);
    }

    #[test]
    fn test_patternize_arguments() {
        let expected = quote! {
            a, b
        };

        let signature = quote! {
            fn some_function(&self, a: u32, b: String);
        };

        let signature = parse2::<TraitItemFn>(signature).unwrap();
        let item = patternize_arguments(&signature.sig.inputs);

        let actual = quote! {
            #item
        };

        assert_eq!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_is_no_ack_false() {
        let method = quote! {
            async fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_no_ack = is_no_ack(&method).unwrap();

        assert!(!is_no_ack);
    }

    #[test]
    fn test_is_no_ack_true() {
        let method = quote! {
            #[no_ack]
            fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_no_ack = is_no_ack(&method).unwrap();

        assert!(is_no_ack);
    }

    #[test]
    fn test_is_no_ack_true_full_path() {
        let method = quote! {
            #[::dnet_rpc::no_ack]
            fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_no_ack = is_no_ack(&method).unwrap();

        assert!(is_no_ack);
    }

    #[test]
    fn test_is_no_ack_true_full_path_from_dnet() {
        let method = quote! {
            #[::dnet::rpc::no_ack]
            fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_no_ack = is_no_ack(&method).unwrap();

        assert!(is_no_ack);
    }

    #[test]
    #[should_panic = "#[no_ack] functions must not have a return type"]
    fn test_is_no_ack_panic() {
        let method = quote! {
            #[no_ack]
            fn some_function(&self, a: u32, b: String) -> u32;
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_no_ack = is_no_ack(&method).unwrap();

        assert!(is_no_ack);
    }

    #[test]
    fn test_is_abortable_false() {
        let method = quote! {
            async fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_abortable = is_abortable(&method);

        assert!(!is_abortable);
    }

    #[test]
    fn test_is_abortable_true() {
        let method = quote! {
            #[abortable]
            async fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_abortable = is_abortable(&method);

        assert!(is_abortable);
    }

    #[test]
    fn test_is_abortable_true_full_path() {
        let method = quote! {
            #[::dnet_rpc::abortable]
            async fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_abortable = is_abortable(&method);

        assert!(is_abortable);
    }

    #[test]
    fn test_is_abortable_true_full_path_from_dnet() {
        let method = quote! {
            #[::dnet::rpc::abortable]
            async fn some_function(&self, a: u32, b: String);
        };

        let method = parse2::<TraitItemFn>(method).unwrap();
        let is_abortable = is_abortable(&method);

        assert!(is_abortable);
    }
}
