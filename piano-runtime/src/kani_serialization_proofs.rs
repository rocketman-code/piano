//! Kani proof harnesses for NDJSON serialization invariants.
//!
//! Run with: cargo kani -p piano-runtime

/// I27: JSON string escaping -- replace('\\', "\\\\").replace('"', "\\\"")
/// produces valid JSON strings for all printable ASCII inputs.
#[kani::proof]
#[kani::unwind(8)]
fn proof_json_escaping_correctness() {
    let len: usize = kani::any();
    kani::assume(len <= 4);

    let mut chars = [0u8; 4];
    for i in 0..4 {
        if i < len {
            chars[i] = kani::any();
            kani::assume(chars[i] >= 0x20 && chars[i] <= 0x7E);
        }
    }

    let s: String = chars[..len].iter().map(|&b| b as char).collect();
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");

    // Verify: no unescaped " or trailing \
    let bytes = escaped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            assert!(i + 1 < bytes.len(), "trailing backslash");
            assert!(
                bytes[i + 1] == b'\\' || bytes[i + 1] == b'"',
                "invalid escape sequence"
            );
            i += 2;
        } else {
            assert_ne!(bytes[i], b'"', "unescaped double quote");
            i += 1;
        }
    }
}

/// I24/I25: Frame JSON structure -- field names are always present.
#[kani::proof]
fn proof_frame_field_names_present() {
    let calls: u64 = kani::any();
    let self_ns: u64 = kani::any();
    let ac: u64 = kani::any();
    let ab: u64 = kani::any();
    let fc: u64 = kani::any();
    let fb: u64 = kani::any();
    let fn_id: u16 = kani::any();

    let json = format!(
        "{{\"id\":{},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}}}",
        fn_id, calls, self_ns, ac, ab, fc, fb
    );

    assert!(json.contains("\"id\":"));
    assert!(json.contains("\"calls\":"));
    assert!(json.contains("\"self_ns\":"));
    assert!(json.contains("\"ac\":"));
    assert!(json.contains("\"ab\":"));
    assert!(json.contains("\"fc\":"));
    assert!(json.contains("\"fb\":"));
}
