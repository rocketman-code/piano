#[allow(unused_imports)]
use crate::*;

pub fn apply_filter(f: crate::SourceFunction, name_id: u32) -> crate::Selected {
    super::Selected::new(f, name_id)
}
