use ra_ap_syntax::{AstNode, SyntaxKind, T, ast};

fn find_close_brace(stmt_list: &ast::StmtList, fn_name: &str) -> usize {
    stmt_list
        .syntax()
        .children_with_tokens()
        .filter(|t| t.kind() == T!['}'])
        .last()
        .unwrap_or_else(|| panic!("no closing brace for fn {fn_name}"))
        .text_range()
        .start()
        .into()
}

fn brace_offset_after_inner_attrs(stmt_list: &ast::StmtList) -> usize {
    let open_brace = stmt_list
        .syntax()
        .children_with_tokens()
        .find(|t| t.kind() == T!['{'])
        .expect("statement list should have an opening brace");
    let mut offset: usize = open_brace.text_range().end().into();

    for child in stmt_list.syntax().children() {
        if child.kind() == SyntaxKind::ATTR {
            let text = child.text().to_string();
            if text.starts_with("#!") {
                offset = child.text_range().end().into();
            }
        }
    }
    offset
}

pub fn inject_guard(
    mut inj: crate::source_map::StringInjector,
    f: &crate::ClassifiedSync,
) -> crate::source_map::StringInjector {
    let name_id = f.name_id();
    let offset = brace_offset_after_inner_attrs(f.source().stmt_list());
    inj.insert(
        offset,
        format!(" let __piano_guard = piano_runtime::enter({name_id});"),
    );
    inj
}

pub fn wrap_async(
    mut inj: crate::source_map::StringInjector,
    f: &crate::ClassifiedAsync,
) -> crate::source_map::StringInjector {
    let name_id = f.name_id();
    let offset = brace_offset_after_inner_attrs(f.source().stmt_list());
    let close_offset = find_close_brace(f.source().stmt_list(), f.source().fn_name());
    inj.insert(
        offset,
        format!(" piano_runtime::enter_async({name_id}, async move {{"),
    );
    inj.insert(close_offset, "}).await");
    inj
}

pub fn wrap_impl_future(
    mut inj: crate::source_map::StringInjector,
    f: &crate::ClassifiedImplFuture,
) -> crate::source_map::StringInjector {
    let name_id = f.name_id();
    let offset = brace_offset_after_inner_attrs(f.source().stmt_list());
    let close_offset = find_close_brace(f.source().stmt_list(), f.source().fn_name());
    inj.insert(offset, format!(" piano_runtime::enter_async({name_id},"));
    inj.insert(close_offset, ")");
    inj
}

pub fn validate_tag(name: &crate::TagName) -> Result<crate::TagName, crate::error::Error> {
    crate::report::tag::validate_tag_name(name.value())?;
    Ok(crate::TagName::new(name.value().to_string()))
}

pub fn save_tag(
    name: &crate::TagName,
    run: &crate::RunId,
    store: &std::path::Path,
) -> Result<(), crate::error::Error> {
    crate::report::tag::save_tag(store, name.value(), run.value())
}

pub fn resolve_tag(
    name: &crate::TagName,
    store: &std::path::Path,
) -> Result<crate::RunId, crate::error::Error> {
    let run_id = crate::report::tag::resolve_tag(store, name.value())?;
    Ok(crate::RunId::new(run_id))
}

pub fn load_tagged_run(
    run: &crate::RunId,
    runs: &std::path::Path,
) -> Result<(), crate::error::Error> {
    crate::report::load::load_run_by_id(runs, run.value()).map(|_| ())
}

pub fn report_build_failure(result: &crate::BuildResult) {
    if !result.status().success() && !result.diagnostics().is_empty() {
        eprintln!("{}", result.diagnostics());
    }
}

pub fn parse_header(line: crate::NdjsonLine) -> Result<crate::ParsedHeader, crate::error::Error> {
    crate::report::load::parse_generated_header(line.value())
}

pub fn classify_body_line(line: &crate::NdjsonLine) -> crate::BodyRecord {
    crate::report::load::classify_generated_body_line(line.value())
}

pub fn take_self_wall(rec: &crate::NdjsonAggregate) -> crate::ParsedWall {
    rec.self_ns().clone()
}

pub fn take_self_cpu(rec: &crate::NdjsonAggregate) -> crate::ParsedCpu {
    rec.cpu_self_ns().clone()
}

pub fn take_name_id(rec: &crate::NdjsonAggregate) -> crate::ParsedId {
    crate::ParsedId::new(rec.name_id())
}

pub fn apply_wall_bias_correction(
    v: crate::ParsedWall,
    bias: &crate::ParsedWall,
    calls: u64,
) -> crate::CorrectedWall {
    let corrected = crate::types::apply_wall_bias(
        crate::report::load::ParsedWall::from_raw(v.value()),
        crate::report::load::ParsedWall::from_raw(bias.value()),
        calls,
    );
    crate::CorrectedWall::new(corrected.raw())
}

pub fn apply_cpu_bias_correction(
    v: crate::ParsedCpu,
    bias: &crate::ParsedCpu,
    calls: u64,
) -> crate::CorrectedCpu {
    let corrected = crate::types::apply_cpu_bias(
        crate::report::load::ParsedCpu::from_raw(v.value()),
        crate::report::load::ParsedCpu::from_raw(bias.value()),
        calls,
    );
    crate::CorrectedCpu::new(corrected.raw())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn generated_bias_correction_saturates_like_real_reader() {
        let corrected = apply_wall_bias_correction(
            crate::ParsedWall::new(1_000),
            &crate::ParsedWall::new(10_000),
            3,
        );
        assert_eq!(corrected.value(), 0);

        let corrected = apply_cpu_bias_correction(
            crate::ParsedCpu::new(80_000),
            &crate::ParsedCpu::new(5_000),
            3,
        );
        assert_eq!(corrected.value(), 65_000);
    }

    #[test]
    fn generated_tag_ops_save_resolve_and_load_run_id() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();
        fs::write(
            runs_dir.join("1000.json"),
            r#"{"run_id":"abc_1000","timestamp_ms":1000,"functions":[{"name":"work","calls":1,"total_ms":1.0,"self_ms":1.0}]}"#,
        )
        .unwrap();

        let tag = crate::TagName::new("baseline".to_string());
        let run = crate::RunId::new("abc_1000".to_string());

        save_tag(&tag, &run, &tags_dir).unwrap();

        let tag_file = fs::read_to_string(tags_dir.join("baseline")).unwrap();
        assert_eq!(tag_file, "abc_1000");

        let resolved = resolve_tag(&tag, &tags_dir).unwrap();
        assert_eq!(resolved.value(), "abc_1000");

        load_tagged_run(&resolved, &runs_dir).unwrap();
    }

    #[test]
    fn generated_tag_ops_reject_invalid_tag_before_writing() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let tag = crate::TagName::new("../bad".to_string());
        let run = crate::RunId::new("abc_1000".to_string());

        let err = save_tag(&tag, &run, &tags_dir).unwrap_err();

        assert!(matches!(err, crate::error::Error::InvalidTagName(_)));
        assert!(!tags_dir.exists());
    }

    #[test]
    fn generated_load_tagged_run_errors_for_missing_run_id() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();
        let tag = crate::TagName::new("baseline".to_string());
        let run = crate::RunId::new("deleted_1000".to_string());

        save_tag(&tag, &run, &tags_dir).unwrap();
        let resolved = resolve_tag(&tag, &tags_dir).unwrap();
        let err = load_tagged_run(&resolved, &runs_dir).unwrap_err();

        assert!(matches!(err, crate::error::Error::NoRuns));
    }
}
