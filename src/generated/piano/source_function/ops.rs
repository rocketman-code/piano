#[allow(unused_imports)]
use crate::*;

use ra_ap_syntax::ast::HasName;
use ra_ap_syntax::{AstNode, SourceFile, ast};

pub(crate) fn from_fn(func: ast::Fn, fn_name: String) -> Option<crate::SourceFunction> {
    if crate::resolve::classify_cst_fn(&func) != crate::resolve::Classification::Instrumentable {
        return None;
    }

    let body = func.body()?;
    let stmt_list = body.stmt_list()?;
    Some(super::SourceFunction::new(func, stmt_list, fn_name))
}

pub(crate) fn fixture_source_function(source: &str, fn_name: &str) -> crate::SourceFunction {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let func = parse
        .tree()
        .syntax()
        .descendants()
        .find_map(ast::Fn::cast)
        .expect("fixture should contain a function");
    from_fn(func, fn_name.to_string()).expect("fixture function should be instrumentable")
}

pub fn detect_instrumentable(func: crate::Fn) -> crate::SourceFunction {
    assert_eq!(
        crate::resolve::classify_cst_fn(&func),
        crate::resolve::Classification::Instrumentable,
        "detect_instrumentable requires an instrumentable function"
    );
    let fn_name = func
        .name()
        .map(|name| name.text().to_string())
        .expect("instrumentable function should have a name");
    from_fn(func, fn_name).expect("instrumentable function should produce a SourceFunction")
}
