#[allow(unused_imports)]
use crate::*;

pub fn store_stable_identity(name: &crate::FunctionIdentity) -> crate::StableIdentity {
    super::StableIdentity::new(name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_identity_preserves_function_identity() {
        let name = crate::FunctionIdentity::new("crate::module::work".to_string());

        let stable = store_stable_identity(&name);

        assert_eq!(stable.identity().value(), "crate::module::work");
    }
}
