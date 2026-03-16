//! NDJSON output serialization.
//!
//! Pure serialization -- no state, no I/O policy. Takes a writer
//! and data, produces NDJSON lines. Testable with Vec<u8>.
//!
//! Invariants:
//! - Each line is self-contained (valid JSON, terminated by newline).
//! - Header and trailer are identical in structure (dual name table
//!   for crash resilience). Only the "type" field differs.
//! - Function names are JSON-escaped (quotes, backslashes, control chars).
//! - No JSON library dependency (hand-written formatting).
//! - Never panics. Write errors are propagated as io::Result.

use crate::measurement::Measurement;
use std::io::{self, Write};

/// Write the name table as a header line.
///
/// Written eagerly at startup. Complete from the first byte.
pub fn write_header(w: &mut impl Write, names: &[(u32, &str)], bias_ns: u64) -> io::Result<()> {
    write_name_table(w, "header", names, bias_ns)
}

/// Write the name table as a trailer line.
///
/// Written by whichever cleanup path runs last (Ctx::drop or atexit).
/// Identical to header in content. Duplicate trailers are harmless --
/// the reader takes the last valid one.
pub fn write_trailer(w: &mut impl Write, names: &[(u32, &str)], bias_ns: u64) -> io::Result<()> {
    write_name_table(w, "trailer", names, bias_ns)
}

/// Write a single measurement as an NDJSON event line.
///
/// One line per completed function call. Each line is self-contained
/// valid JSON terminated by a newline.
pub fn write_measurement(w: &mut impl Write, m: &Measurement) -> io::Result<()> {
    writeln!(
        w,
        concat!(
            "{{\"span_id\":{},\"parent_span_id\":{},\"name_id\":{},",
            "\"start_ns\":{},\"end_ns\":{},\"thread_id\":{},",
            "\"cpu_start_ns\":{},\"cpu_end_ns\":{},",
            "\"alloc_count\":{},\"alloc_bytes\":{},",
            "\"free_count\":{},\"free_bytes\":{}}}"
        ),
        m.span_id,
        m.parent_span_id,
        m.name_id,
        m.start_ns,
        m.end_ns,
        m.thread_id,
        m.cpu_start_ns,
        m.cpu_end_ns,
        m.alloc_count,
        m.alloc_bytes,
        m.free_count,
        m.free_bytes,
    )
}

/// Write a batch of measurements as NDJSON event lines.
///
/// Used by flush (threshold-triggered) and shutdown (drain remaining).
pub fn write_measurements(w: &mut impl Write, measurements: &[Measurement]) -> io::Result<()> {
    for m in measurements {
        write_measurement(w, m)?;
    }
    Ok(())
}

fn write_name_table(w: &mut impl Write, kind: &str, names: &[(u32, &str)], bias_ns: u64) -> io::Result<()> {
    write!(w, "{{\"type\":\"{kind}\",\"bias_ns\":{bias_ns},\"names\":{{")?;
    for (i, (id, name)) in names.iter().enumerate() {
        if i > 0 {
            write!(w, ",")?;
        }
        write!(w, "\"{id}\":\"",)?;
        write_json_escaped(w, name)?;
        write!(w, "\"")?;
    }
    writeln!(w, "}}}}")
}

/// Write a string with JSON escaping.
///
/// Handles: double quotes, backslashes, and control characters.
/// Rust function paths are ASCII identifiers separated by :: so
/// escaping is trivial in practice, but the writer must handle
/// the general case.
fn write_json_escaped(w: &mut impl Write, s: &str) -> io::Result<()> {
    for byte in s.bytes() {
        match byte {
            b'"' => w.write_all(b"\\\"")?,
            b'\\' => w.write_all(b"\\\\")?,
            b'\n' => w.write_all(b"\\n")?,
            b'\r' => w.write_all(b"\\r")?,
            b'\t' => w.write_all(b"\\t")?,
            b if b < 0x20 => write!(w, "\\u{b:04x}")?,
            _ => w.write_all(&[byte])?,
        }
    }
    Ok(())
}
