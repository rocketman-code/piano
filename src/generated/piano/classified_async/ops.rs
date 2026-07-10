#[allow(unused_imports)]
use crate::*;

pub fn classify_async(f: crate::Selected) -> crate::ClassifiedAsync {
    assert!(
        f.source().func().async_token().is_some(),
        "classify_async requires an async fn"
    );
    super::ClassifiedAsync::new(f.source().clone(), f.name_id())
}
