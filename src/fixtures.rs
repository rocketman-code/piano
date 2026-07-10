//! Test fixtures for carve-generated invariants.

use ra_ap_syntax::{AstNode, SourceFile, ast};

pub fn append_default_allocator_subject() {}

#[allow(dead_code)]
pub fn apply_cpu_bias_correction_subject() -> (crate::ParsedCpu, crate::ParsedCpu, u64) {
    (
        crate::ParsedCpu::new(80_000),
        crate::ParsedCpu::new(5_000),
        3,
    )
}

#[allow(dead_code)]
pub fn apply_wall_bias_correction_subject() -> (crate::ParsedWall, crate::ParsedWall, u64) {
    (
        crate::ParsedWall::new(1_000),
        crate::ParsedWall::new(10_000),
        3,
    )
}

pub fn classify_async_subject() -> crate::Selected {
    crate::apply_filter(
        crate::generated::piano::source_function::ops::fixture_source_function(
            "async fn fetch() {\n    let value = 1;\n    let _ = value;\n}\n",
            "fetch",
        ),
        1,
    )
}

pub fn classify_body_line_subject() -> crate::NdjsonLine {
    crate::NdjsonLine::new(golden_lines()[1].to_string())
}

pub fn classify_impl_future_subject() -> crate::Selected {
    crate::apply_filter(
        crate::generated::piano::source_function::ops::fixture_source_function(
            "fn fetch() -> impl std::future::Future<Output = i32> {\n    async { 42 }\n}\n",
            "fetch",
        ),
        2,
    )
}

pub fn classify_sync_subject() -> crate::Selected {
    crate::apply_filter(
        crate::generated::piano::source_function::ops::fixture_source_function(
            "fn work() -> i32 {\n    42\n}\n",
            "work",
        ),
        3,
    )
}

pub fn detect_instrumentable_subject() -> crate::Fn {
    parse_first_fn("fn work() {\n    let value = 1;\n    let _ = value;\n}\n")
}

pub fn parse_header_subject() -> crate::NdjsonLine {
    crate::NdjsonLine::new(golden_lines()[0].to_string())
}

pub fn wrap_cfg_attr_subject() -> crate::CfgAttrApplied {
    crate::classify_cfg_attr_applied(allocator_site(
        "ALLOC",
        "MyAlloc",
        "MyAlloc",
        Some("#[cfg(feature = \"x\")]"),
        Some("#[cfg_attr(feature = \"y\", global_allocator)]"),
    ))
}

pub fn wrap_cfg_gated_subject() -> crate::CfgGated {
    crate::classify_cfg_gated(allocator_site(
        "ALLOC",
        "MyAlloc",
        "MyAlloc",
        Some("#[cfg(feature = \"x\")]"),
        None,
    ))
}

pub fn wrap_unconditional_subject() -> crate::Unconditional {
    crate::classify_unconditional(allocator_site("ALLOC", "MyAlloc", "MyAlloc", None, None))
}

fn allocator_site(
    name: &str,
    type_expr: &str,
    init_expr: &str,
    form_gate: Option<&str>,
    cfg_attr: Option<&str>,
) -> crate::AllocatorSite {
    crate::generated::piano::allocator_site::ops::from_parts(
        0,
        0,
        name.to_string(),
        type_expr.to_string(),
        init_expr.to_string(),
        form_gate.map(ToOwned::to_owned),
        cfg_attr.map(ToOwned::to_owned),
    )
}

fn parse_first_fn(source: &str) -> crate::Fn {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    parse
        .tree()
        .syntax()
        .descendants()
        .find_map(ast::Fn::cast)
        .expect("fixture should contain a function")
}

fn golden_lines() -> Vec<&'static str> {
    include_str!("../piano-runtime/tests/data/golden_run.ndjson")
        .lines()
        .collect()
}
