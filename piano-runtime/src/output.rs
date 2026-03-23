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

/// Serialize the trailer to a byte vector for signal-safe writing.
/// Called once at startup; the bytes are stored in atomic global storage
/// and written via raw write() in the signal handler.
pub fn serialize_trailer(names: &[(u32, &str)], bias_ns: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    // Leading newline: if a signal interrupts a write mid-line, the
    // trailer must start on its own line for the reader to parse it.
    buf.push(b'\n');
    // write_trailer can't fail on a Vec (infallible writer)
    let _ = write_trailer(&mut buf, names, bias_ns);
    buf
}

// ---------------------------------------------------------------------------
// Signal-safe serialization (no allocation, stack buffer only)
// ---------------------------------------------------------------------------

/// Format a u64 as decimal digits into a byte buffer. Returns the number
/// of bytes written. No allocation. Signal-safe.
fn itoa(mut n: u64, buf: &mut [u8]) -> usize {
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 20]; // u64::MAX is 20 digits
    let mut i = 0;
    while n > 0 {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    for j in 0..i {
        buf[j] = tmp[i - 1 - j];
    }
    i
}

/// Serialize a measurement as an NDJSON line into a stack-allocated buffer.
/// Returns the number of bytes written. Max output: 395 bytes (all u64::MAX).
/// No allocation, no format!(), no String. Signal-safe.
pub fn serialize_measurement_to_stack(buf: &mut [u8; 512], m: &Measurement) -> usize {
    let mut pos = 0;

    macro_rules! put {
        ($bytes:expr) => {
            let b: &[u8] = $bytes;
            buf[pos..pos + b.len()].copy_from_slice(b);
            pos += b.len();
        };
    }
    macro_rules! put_u64 {
        ($n:expr) => {
            pos += itoa($n, &mut buf[pos..]);
        };
    }

    put!(b"{\"span_id\":");        put_u64!(m.span_id);
    put!(b",\"parent_span_id\":"); put_u64!(m.parent_span_id);
    put!(b",\"name_id\":");        put_u64!(m.name_id as u64);
    put!(b",\"start_ns\":");       put_u64!(m.start_ns);
    put!(b",\"end_ns\":");         put_u64!(m.end_ns);
    put!(b",\"thread_id\":");      put_u64!(m.thread_id);
    put!(b",\"cpu_start_ns\":");   put_u64!(m.cpu_start_ns);
    put!(b",\"cpu_end_ns\":");     put_u64!(m.cpu_end_ns);
    put!(b",\"alloc_count\":");    put_u64!(m.alloc_count);
    put!(b",\"alloc_bytes\":");    put_u64!(m.alloc_bytes);
    put!(b",\"free_count\":");     put_u64!(m.free_count);
    put!(b",\"free_bytes\":");     put_u64!(m.free_bytes);
    put!(b"}\n");

    pos
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
