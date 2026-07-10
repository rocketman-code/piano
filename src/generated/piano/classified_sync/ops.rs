#[allow(unused_imports)]
use crate::*;

pub fn classify_sync(f: crate::Selected) -> crate::ClassifiedSync {
    assert!(
        f.source().func().async_token().is_none()
            && !crate::rewrite::returns_impl_future(f.source().func()),
        "classify_sync requires a non-async fn that does not return impl Future"
    );
    super::ClassifiedSync::new(f.source().clone(), f.name_id())
}
