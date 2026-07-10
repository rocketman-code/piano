pub mod build;
pub mod error;
pub(crate) mod macro_expand;
pub mod naming;
pub mod report;
pub mod resolve;
pub mod rewrite;
pub mod source_map;
pub mod staging;
pub(crate) mod types;
pub mod wrapper;

#[cfg(test)]
mod fixtures;
#[allow(dead_code)]
pub mod generated;
mod generated_root_ops;
#[cfg(test)]
mod invariants;
pub mod predicates;

pub use ra_ap_syntax::SourceFile;
pub use ra_ap_syntax::ast::Fn;

pub use generated::piano::{
    allocator_site::AllocatorSite,
    body_record::BodyRecord,
    build_result::BuildResult,
    cfg_attr_applied::CfgAttrApplied,
    cfg_gated::CfgGated,
    classified_async::ClassifiedAsync,
    classified_impl_future::ClassifiedImplFuture,
    classified_sync::ClassifiedSync,
    corrected_cpu::CorrectedCpu,
    corrected_wall::CorrectedWall,
    function_identity::FunctionIdentity,
    ndjson_aggregate::NdjsonAggregate,
    ndjson_line::{NdjsonLine, parsed_header::ParsedHeader},
    parsed_alloc::ParsedAlloc,
    parsed_cpu::ParsedCpu,
    parsed_id::ParsedId,
    parsed_wall::ParsedWall,
    run_id::RunId,
    selected::Selected,
    source_function::SourceFunction,
    stable_identity::StableIdentity,
    tag_name::TagName,
    unconditional::Unconditional,
    wrapped_allocator::WrappedAllocator,
};
pub use generated::piano::{
    allocator_site::ops::detect_allocator,
    build_result::ops::{classify_failure, classify_success, invoke_compiler},
    cfg_attr_applied::ops::classify_cfg_attr_applied,
    cfg_gated::ops::classify_cfg_gated,
    classified_async::ops::classify_async,
    classified_impl_future::ops::classify_impl_future,
    classified_sync::ops::classify_sync,
    parsed_alloc::ops::take_alloc,
    selected::ops::apply_filter,
    source_function::ops::detect_instrumentable,
    stable_identity::ops::store_stable_identity,
    unconditional::ops::classify_unconditional,
    wrapped_allocator::ops::{
        append_default_allocator, wrap_cfg_attr, wrap_cfg_gated, wrap_unconditional,
    },
};
pub use generated_root_ops::{
    apply_cpu_bias_correction, apply_wall_bias_correction, classify_body_line, inject_guard,
    load_tagged_run, parse_header, report_build_failure, resolve_tag, save_tag, take_name_id,
    take_self_cpu, take_self_wall, validate_tag, wrap_async, wrap_impl_future,
};
