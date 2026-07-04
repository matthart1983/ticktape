//! `#[derive(Encode, Decode)]` for Ticktape's canonical fixed-layout codec.
//!
//! Encoding is little-endian, in declaration order. Enums encode a `u16`
//! discriminant (the variant's declaration index) followed by the variant's
//! fields in order — so variant order is part of the wire contract; append
//! new variants, never reorder.
//!
//! Determinism enforcement is structural: `HashMap`, `HashSet`, and floats
//! simply have no `Encode`/`Decode` impls, so a field of those types fails
//! to compile.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Index};

const CORE: &str = "ticktape_core";

fn core_path() -> TokenStream2 {
    let ident = format_ident!("{}", CORE);
    quote!(::#ident)
}

#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    expand_encode(&ast)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(Decode)]
pub fn derive_decode(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    expand_decode(&ast)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn check_no_generics(ast: &DeriveInput) -> syn::Result<()> {
    if ast.generics.params.is_empty() {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            &ast.generics,
            "ticktape codec derives do not support generic types yet",
        ))
    }
}

fn variant_count_check(ast: &DeriveInput, count: usize) -> syn::Result<()> {
    if count > u16::MAX as usize {
        return Err(syn::Error::new_spanned(
            ast,
            "enum has more than u16::MAX variants",
        ));
    }
    Ok(())
}

fn expand_encode(ast: &DeriveInput) -> syn::Result<TokenStream2> {
    check_no_generics(ast)?;
    let core = core_path();
    let name = &ast.ident;

    let (encode_body, len_body) = match &ast.data {
        Data::Struct(data) => {
            let accessors = field_accessors(&data.fields);
            let encodes = accessors.iter().map(|acc| {
                quote! { #core::Encode::encode(&self.#acc, out); }
            });
            let lens = accessors.iter().map(|acc| {
                quote! { + #core::Encode::encoded_len(&self.#acc) }
            });
            (quote! { #(#encodes)* }, quote! { 0usize #(#lens)* })
        }
        Data::Enum(data) => {
            variant_count_check(ast, data.variants.len())?;
            let mut encode_arms = Vec::new();
            let mut len_arms = Vec::new();
            for (idx, variant) in data.variants.iter().enumerate() {
                let disc = idx as u16;
                let vname = &variant.ident;
                let (pattern, bindings) = variant_pattern(&variant.fields);
                let encodes = bindings.iter().map(|b| {
                    quote! { #core::Encode::encode(#b, out); }
                });
                let lens = bindings.iter().map(|b| {
                    quote! { + #core::Encode::encoded_len(#b) }
                });
                encode_arms.push(quote! {
                    Self::#vname #pattern => {
                        #core::Encode::encode(&#disc, out);
                        #(#encodes)*
                    }
                });
                len_arms.push(quote! {
                    Self::#vname #pattern => { 2usize #(#lens)* }
                });
            }
            (
                quote! { match self { #(#encode_arms)* } },
                quote! { match self { #(#len_arms)* } },
            )
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(ast, "unions cannot derive Encode"));
        }
    };

    Ok(quote! {
        impl #core::Encode for #name {
            fn encode(&self, out: &mut ::std::vec::Vec<u8>) {
                #encode_body
            }
            fn encoded_len(&self) -> usize {
                #len_body
            }
        }
    })
}

fn expand_decode(ast: &DeriveInput) -> syn::Result<TokenStream2> {
    check_no_generics(ast)?;
    let core = core_path();
    let name = &ast.ident;

    let body = match &ast.data {
        Data::Struct(data) => {
            let construct = decode_fields(&data.fields, &core);
            quote! { ::core::result::Result::Ok(Self #construct) }
        }
        Data::Enum(data) => {
            variant_count_check(ast, data.variants.len())?;
            let arms = data.variants.iter().enumerate().map(|(idx, variant)| {
                let disc = idx as u16;
                let vname = &variant.ident;
                let construct = decode_fields(&variant.fields, &core);
                quote! { #disc => ::core::result::Result::Ok(Self::#vname #construct), }
            });
            quote! {
                let discriminant = <u16 as #core::Decode>::decode(buf)?;
                match discriminant {
                    #(#arms)*
                    _ => ::core::result::Result::Err(
                        #core::CodecError::InvalidValue("enum discriminant"),
                    ),
                }
            }
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(ast, "unions cannot derive Decode"));
        }
    };

    Ok(quote! {
        impl #core::Decode for #name {
            fn decode(buf: &mut &[u8]) -> ::core::result::Result<Self, #core::CodecError> {
                #body
            }
        }
    })
}

/// Accessor tokens for struct fields: idents for named, indices for tuple.
fn field_accessors(fields: &Fields) -> Vec<TokenStream2> {
    match fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|f| {
                let ident = f.ident.as_ref().unwrap();
                quote!(#ident)
            })
            .collect(),
        Fields::Unnamed(unnamed) => (0..unnamed.unnamed.len())
            .map(|i| {
                let idx = Index::from(i);
                quote!(#idx)
            })
            .collect(),
        Fields::Unit => Vec::new(),
    }
}

/// Match pattern + bound identifiers for an enum variant.
fn variant_pattern(fields: &Fields) -> (TokenStream2, Vec<TokenStream2>) {
    match fields {
        Fields::Named(named) => {
            let idents: Vec<_> = named
                .named
                .iter()
                .map(|f| f.ident.clone().unwrap())
                .collect();
            (
                quote! { { #(#idents),* } },
                idents.iter().map(|i| quote!(#i)).collect(),
            )
        }
        Fields::Unnamed(unnamed) => {
            let idents: Vec<_> = (0..unnamed.unnamed.len())
                .map(|i| format_ident!("field{}", i))
                .collect();
            (
                quote! { ( #(#idents),* ) },
                idents.iter().map(|i| quote!(#i)).collect(),
            )
        }
        Fields::Unit => (quote! {}, Vec::new()),
    }
}

/// Constructor tokens decoding each field in declaration order.
fn decode_fields(fields: &Fields, core: &TokenStream2) -> TokenStream2 {
    match fields {
        Fields::Named(named) => {
            let inits = named.named.iter().map(|f| {
                let ident = f.ident.as_ref().unwrap();
                let ty = &f.ty;
                quote! { #ident: <#ty as #core::Decode>::decode(buf)?, }
            });
            quote! { { #(#inits)* } }
        }
        Fields::Unnamed(unnamed) => {
            let inits = unnamed.unnamed.iter().map(|f| {
                let ty = &f.ty;
                quote! { <#ty as #core::Decode>::decode(buf)?, }
            });
            quote! { ( #(#inits)* ) }
        }
        Fields::Unit => quote! {},
    }
}
