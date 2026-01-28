use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{parse_macro_input, parse_quote, Attribute, ItemStruct, LitStr, Meta};

#[proc_macro_attribute]
pub fn prefixed_args(attr: TokenStream, item: TokenStream) -> TokenStream {
    let prefix = parse_prefix(attr);
    let mut input = parse_macro_input!(item as ItemStruct);

    if let syn::Fields::Named(fields) = &mut input.fields {
        for field in fields.named.iter_mut() {
            let mut skip = false;
            let mut override_name: Option<String> = None;

            let mut next_attrs: Vec<Attribute> = Vec::with_capacity(field.attrs.len());
            for attr in field.attrs.drain(..) {
                if attr.path().is_ident("prefixed_arg") {
                    let _ = attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("skip") {
                            skip = true;
                            return Ok(());
                        }
                        if meta.path.is_ident("rename") {
                            let value = meta.value()?;
                            let lit: LitStr = value.parse()?;
                            override_name = Some(lit.value());
                            return Ok(());
                        }
                        Ok(())
                    });
                    continue;
                }

                next_attrs.push(attr);
            }
            field.attrs = next_attrs;

            if skip {
                continue;
            }

            if has_arg_long(&field.attrs) {
                continue;
            }

            let field_name = override_name
                .or_else(|| serde_rename(&field.attrs))
                .or_else(|| field.ident.as_ref().map(|ident| ident.to_string()));

            let field_name = match field_name {
                Some(name) => name,
                None => continue,
            };

            let long_value = format!("{}.{}", prefix, field_name);
            let long_lit = LitStr::new(&long_value, proc_macro2::Span::call_site());
            let id_lit = LitStr::new(&long_value, proc_macro2::Span::call_site());
            let long_attr: Attribute = parse_quote!(#[arg(long = #long_lit, id = #id_lit)]);
            field.attrs.push(long_attr);
        }
    }

    TokenStream::from(quote!(#input))
}

fn parse_prefix(attr: TokenStream) -> String {
    let parser = syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated;
    let meta = match parser.parse(attr) {
        Ok(meta) => meta,
        Err(_) => return String::new(),
    };
    for nested in meta {
        if let Meta::NameValue(name_value) = nested {
            if !name_value.path.is_ident("prefix") {
                continue;
            }
            if let syn::Expr::Lit(expr_lit) = name_value.value {
                if let syn::Lit::Str(lit_str) = expr_lit.lit {
                    return lit_str.value();
                }
            }
        }
    }

    String::new()
}

fn has_arg_long(attrs: &[Attribute]) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("arg") {
            continue;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("long") {
                found = true;
            }
            Ok(())
        });
        if found {
            return true;
        }
    }
    false
}

fn serde_rename(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let mut rename_value = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                rename_value = Some(lit.value());
            }
            Ok(())
        });
        if rename_value.is_some() {
            return rename_value;
        }
    }
    None
}
