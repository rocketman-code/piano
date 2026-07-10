#[allow(unused_imports)]
use crate::*;

const CFG_ATTR_FALLBACK_NAME: &str = "__PIANO_ALLOC";

pub fn wrap_unconditional(a: crate::Unconditional) -> crate::WrappedAllocator {
    super::WrappedAllocator::new(a.site().clone())
}

pub fn wrap_cfg_gated(a: crate::CfgGated) -> crate::WrappedAllocator {
    super::WrappedAllocator::new(a.site().clone())
}

pub fn wrap_cfg_attr(a: crate::CfgAttrApplied) -> crate::WrappedAllocator {
    super::WrappedAllocator::new(a.site().clone())
}

pub fn append_default_allocator() -> crate::WrappedAllocator {
    let site = crate::generated::piano::allocator_site::ops::from_parts(
        0,
        0,
        CFG_ATTR_FALLBACK_NAME.to_string(),
        "std::alloc::System".to_string(),
        "std::alloc::System".to_string(),
        None,
        None,
    );
    super::WrappedAllocator::new(site)
}

pub(crate) fn render_wrapped_static(wrapped: &crate::WrappedAllocator) -> String {
    let site = wrapped.site();
    let mut rendered = String::new();

    if let Some(form_gate) = site.form_gate() {
        rendered.push_str(form_gate);
        rendered.push('\n');
    }

    if let Some(cfg_attr) = site.cfg_attr() {
        rendered.push_str(cfg_attr);
    } else {
        rendered.push_str("#[global_allocator]");
    }
    rendered.push('\n');

    rendered.push_str(&format!(
        "static {name}: piano_runtime::PianoAllocator<{ty}> = piano_runtime::PianoAllocator::new({init});",
        name = site.name(),
        ty = site.type_expr(),
        init = site.init_expr(),
    ));
    rendered
}

pub(crate) fn render_fallback_static(wrapped: &crate::WrappedAllocator) -> Option<String> {
    let site = wrapped.site();
    let negated_active_cfg = negated_active_allocator_cfg(site)?;
    let fallback_name = if site.cfg_attr().is_some() {
        CFG_ATTR_FALLBACK_NAME
    } else {
        site.name()
    };

    Some(format!(
        "\n{negated_active_cfg}\n#[global_allocator]\nstatic {fallback_name}: piano_runtime::PianoAllocator<std::alloc::System> = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
    ))
}

pub(crate) fn render_allocator_bundle(wrapped: &crate::WrappedAllocator) -> String {
    let mut rendered = render_wrapped_static(wrapped);
    if let Some(fallback) = render_fallback_static(wrapped) {
        rendered.push_str(&fallback);
    }
    rendered
}

pub(crate) fn active_allocator_cfg(site: &crate::AllocatorSite) -> Option<String> {
    match (site.form_gate(), site.cfg_attr()) {
        (None, None) => None,
        (Some(form_gate), None) => cfg_predicate(form_gate).map(ToOwned::to_owned),
        (None, Some(cfg_attr)) => cfg_attr_predicate(cfg_attr),
        (Some(form_gate), Some(cfg_attr)) => {
            let form_predicate = cfg_predicate(form_gate)?;
            let attr_predicate = cfg_attr_predicate(cfg_attr)?;
            Some(format!("all({form_predicate}, {attr_predicate})"))
        }
    }
}

pub(crate) fn negated_active_allocator_cfg(site: &crate::AllocatorSite) -> Option<String> {
    active_allocator_cfg(site).map(|predicate| format!("#[cfg(not({predicate}))]"))
}

fn cfg_predicate(cfg: &str) -> Option<&str> {
    cfg.strip_prefix("#[cfg(")?.strip_suffix(")]").map(str::trim)
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
