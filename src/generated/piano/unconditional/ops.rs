#[allow(unused_imports)]
use crate::*;

pub fn classify_unconditional(a: crate::AllocatorSite) -> crate::Unconditional {
    assert!(
        a.form_gate().is_none() && a.cfg_attr().is_none(),
        "classify_unconditional requires an ungated direct global allocator"
    );
    super::Unconditional::new(a)
}
