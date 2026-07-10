#[allow(unused_imports)]
use crate::*;

pub fn classify_impl_future(f: crate::Selected) -> crate::ClassifiedImplFuture {
    assert!(
        f.source().func().async_token().is_none()
            && crate::rewrite::returns_impl_future(f.source().func()),
        "classify_impl_future requires a non-async fn returning impl Future"
    );
    super::ClassifiedImplFuture::new(f.source().clone(), f.name_id())
}
