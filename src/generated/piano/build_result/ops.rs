#[allow(unused_imports)]
use crate::*;

pub fn invoke_compiler() -> Result<crate::BuildResult, std::io::Error> {
    let output = crate::build::invoke_compiler_for_generated_ops()?;
    Ok(super::BuildResult::new(output.status, output.diagnostics))
}

pub fn classify_success(result: crate::BuildResult) -> crate::BuildResult {
    assert!(
        result.status().success(),
        "classify_success requires a successful compiler result"
    );
    result
}

pub fn classify_failure(result: crate::BuildResult) -> crate::BuildResult {
    assert!(
        !result.status().success(),
        "classify_failure requires a failed compiler result"
    );
    result
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::process::Command;

    use super::*;

    fn rustc_status(args: &[&str]) -> std::process::ExitStatus {
        Command::new("rustc")
            .args(args)
            .output()
            .expect("rustc should run in the test environment")
            .status
    }

    #[test]
    fn classify_success_accepts_zero_status() {
        let status = rustc_status(&["--version"]);
        let result = super::super::BuildResult::new(status, String::new());

        let classified = classify_success(result);

        assert!(classified.status().success());
    }

    #[test]
    fn classify_failure_accepts_nonzero_status() {
        let status = rustc_status(&["--definitely-not-a-real-rustc-flag"]);
        let result = super::super::BuildResult::new(status, "bad flag".to_string());

        let classified = classify_failure(result);

        assert!(!classified.status().success());
        assert_eq!(classified.diagnostics(), "bad flag");
    }

    #[test]
    fn compiler_invocation_helper_returns_spawn_error_before_status() {
        let mut command = Command::new("/definitely/not/a/cargo/binary");

        let err = crate::build::run_compiler_for_generated_ops(&mut command)
            .expect_err("missing program is a spawn error");

        assert_eq!(err.kind(), ErrorKind::NotFound);
    }
}
