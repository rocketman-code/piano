#[allow(unused_imports)]
use crate::*;

pub fn classify_cfg_attr_applied(a: crate::AllocatorSite) -> crate::CfgAttrApplied {
    assert!(
        a.cfg_attr().is_some(),
        "classify_cfg_attr_applied requires cfg_attr(..., global_allocator)"
    );
    super::CfgAttrApplied::new(a)
}
