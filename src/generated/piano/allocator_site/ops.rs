#[allow(unused_imports)]
use crate::*;

use ra_ap_syntax::AstNode;

pub(crate) fn from_parts(
    start: usize,
    end: usize,
    name: String,
    type_expr: String,
    init_expr: String,
    form_gate: Option<String>,
    cfg_attr: Option<String>,
) -> crate::AllocatorSite {
    super::AllocatorSite::new(start, end, name, type_expr, init_expr, form_gate, cfg_attr)
}

pub fn detect_allocator(file: &crate::SourceFile) -> crate::AllocatorSite {
    let source = file.syntax().text().to_string();
    crate::rewrite::find_global_allocator_site(file, &source)
        .expect("detect_allocator requires a global allocator site")
}
