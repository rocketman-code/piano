//! Predicate oracles for carve-generated invariants.

use ra_ap_syntax::AstNode;

pub trait AllocatorWitnessSite {
    fn allocator_site(&self) -> &crate::AllocatorSite;
}

impl AllocatorWitnessSite for crate::CfgGated {
    fn allocator_site(&self) -> &crate::AllocatorSite {
        self.site()
    }
}

impl AllocatorWitnessSite for crate::CfgAttrApplied {
    fn allocator_site(&self) -> &crate::AllocatorSite {
        self.site()
    }
}

pub fn is_cfg_gate_preserved<T: AllocatorWitnessSite>(
    arg0: &T,
    arg1: &crate::WrappedAllocator,
) -> bool {
    let Some(input_gate) = arg0.allocator_site().form_gate() else {
        return true;
    };

    let rendered = crate::generated::piano::wrapped_allocator::ops::render_wrapped_static(arg1);
    arg1.site().form_gate() == arg0.allocator_site().form_gate()
        && rendered.lines().any(|line| line.trim() == input_gate)
}

pub fn is_classification_total(arg0: &crate::BodyRecord) -> bool {
    match arg0 {
        crate::BodyRecord::Trailer { .. }
        | crate::BodyRecord::Aggregate { .. }
        | crate::BodyRecord::Measurement { .. }
        | crate::BodyRecord::Unrecognized => true,
    }
}

pub fn is_coroutine_by_desugaring(arg0: &crate::ClassifiedAsync) -> bool {
    arg0.source().func().async_token().is_some()
}

pub fn is_delegable_future(arg0: &crate::ClassifiedImplFuture) -> bool {
    arg0.source().func().async_token().is_none()
        && crate::rewrite::returns_impl_future(arg0.source().func())
}

pub fn is_instrumentable_sound(arg0: &crate::SourceFunction) -> bool {
    crate::resolve::classify_cst_fn(arg0.func()) == crate::resolve::Classification::Instrumentable
        && arg0
            .func()
            .body()
            .and_then(|body| body.stmt_list())
            .is_some()
        && !arg0.fn_name().is_empty()
}

pub fn is_nonnegative_cpu(_arg0: &crate::CorrectedCpu) -> bool {
    // Structurally discharged: CorrectedCpu wraps u64, which cannot be
    // negative. The real risk (underflowing correction) is saturating by
    // construction and pinned by the exact-value saturation test.
    true
}

pub fn is_nonnegative_wall(_arg0: &crate::CorrectedWall) -> bool {
    // Structurally discharged: CorrectedWall wraps u64, which cannot be
    // negative. The real risk (underflowing correction) is saturating by
    // construction and pinned by the exact-value saturation test.
    true
}

pub fn is_scope_bounded(arg0: &crate::ClassifiedSync) -> bool {
    arg0.source().func().async_token().is_none()
        && !crate::rewrite::returns_impl_future(arg0.source().func())
        && arg0.source().stmt_list().syntax().text().contains_char('{')
}

pub fn is_single_global_allocator(arg0: &crate::WrappedAllocator) -> bool {
    let site = arg0.site();
    let bundle = crate::generated::piano::wrapped_allocator::ops::render_allocator_bundle(arg0);

    match (site.form_gate(), site.cfg_attr()) {
        (None, None) => {
            bundle.matches("global_allocator").count() == 1
                && bundle.contains("static ")
                && !bundle.contains("#[cfg(")
        }
        (Some(form_gate), None) => {
            let Some(negated) = expected_negated_active_cfg(site) else {
                return false;
            };
            bundle.contains(form_gate)
                && bundle.contains(&negated)
                && bundle.matches("#[global_allocator]").count() == 2
        }
        (None, Some(cfg_attr)) => {
            let Some(negated) = expected_negated_active_cfg(site) else {
                return false;
            };
            bundle.contains(cfg_attr)
                && bundle.contains(&negated)
                && bundle.matches("#[global_allocator]").count() == 1
                && bundle.contains("static __PIANO_ALLOC:")
        }
        (Some(form_gate), Some(cfg_attr)) => {
            let Some(negated) = expected_negated_active_cfg(site) else {
                return false;
            };
            bundle.contains(form_gate)
                && bundle.contains(cfg_attr)
                && bundle.contains(&negated)
                && bundle.matches("#[global_allocator]").count() == 1
                && bundle.contains("static __PIANO_ALLOC:")
        }
    }
}

fn expected_negated_active_cfg(site: &crate::AllocatorSite) -> Option<String> {
    let active = match (site.form_gate(), site.cfg_attr()) {
        (None, None) => return None,
        (Some(form_gate), None) => cfg_predicate(form_gate)?.to_string(),
        (None, Some(cfg_attr)) => cfg_attr_predicate(cfg_attr)?,
        (Some(form_gate), Some(cfg_attr)) => {
            format!(
                "all({}, {})",
                cfg_predicate(form_gate)?,
                cfg_attr_predicate(cfg_attr)?
            )
        }
    };
    Some(format!("#[cfg(not({active}))]"))
}

fn cfg_predicate(cfg: &str) -> Option<&str> {
    cfg.strip_prefix("#[cfg(")?
        .strip_suffix(")]")
        .map(str::trim)
}

fn cfg_attr_predicate(cfg_attr: &str) -> Option<String> {
    let rest = cfg_attr.strip_prefix("#[cfg_attr(")?;
    let mut depth = 0u32;
    for (idx, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Some(rest[..idx].trim().to_string()),
            _ => {}
        }
    }
    None
}

pub fn is_structurally_valid(arg0: &crate::ParsedHeader) -> bool {
    crate::report::load::is_generated_header_structurally_valid(arg0)
}
