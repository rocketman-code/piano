#[allow(unused_imports)]
use crate::*;

pub fn classify_cfg_gated(a: crate::AllocatorSite) -> crate::CfgGated {
    assert!(
        a.form_gate().is_some() && a.cfg_attr().is_none(),
        "classify_cfg_gated requires a direct global allocator with a cfg form gate"
    );
    super::CfgGated::new(a)
}
