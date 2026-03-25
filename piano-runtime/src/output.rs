//! NDJSON output serialization.
//!
//! Pure serialization -- no state, no I/O policy. Takes a writer
//! and data, produces NDJSON lines.
//!
//! Invariants:
//! - Each line is self-contained (valid JSON, terminated by newline).
//! - Header and trailer are identical in structure (dual name table
//!   for crash resilience). Only the "type" field differs.
//! - Function names are JSON-escaped (quotes, backslashes, control chars).
//! - No JSON library dependency (hand-written formatting).
//! - Never panics. Write errors are propagated as io::Result.

use std::io::{self, Write};

/// Write the name table as a header line, including run metadata.
pub fn write_header(
    w: &mut impl Write,
    names: &[(u32, &str)],
    bias_ns: u64,
    cpu_bias_ns: u64,
    run_id: &str,
    timestamp_ms: u128,
) -> io::Result<()> {
    write!(
        w,
        "{{\"type\":\"header\",\"run_id\":\"{run_id}\",\"timestamp_ms\":{timestamp_ms},"
    )?;
    write!(
        w,
        "\"bias_ns\":{bias_ns},\"cpu_bias_ns\":{cpu_bias_ns},\"names\":{{"
    )?;
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

/// Write the name table as a trailer line.
pub fn write_trailer(
    w: &mut impl Write,
    names: &[(u32, &str)],
    bias_ns: u64,
    cpu_bias_ns: u64,
) -> io::Result<()> {
    write_name_table(w, "trailer", names, bias_ns, cpu_bias_ns)
}

/// Serialize the trailer to a byte vector for signal-safe writing.
pub fn serialize_trailer(names: &[(u32, &str)], bias_ns: u64, cpu_bias_ns: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(b'\n');
    let _ = write_trailer(&mut buf, names, bias_ns, cpu_bias_ns);
    buf
}

/// Write aggregated per-function summaries as NDJSON lines.
/// Each inner Vec is one thread's data. Thread index is assigned
/// from position in the outer slice.
pub fn write_aggregates(
    w: &mut impl Write,
    per_thread: &[Vec<crate::aggregator::FnAgg>],
) -> io::Result<()> {
    for (thread_idx, thread_agg) in per_thread.iter().enumerate() {
        for a in thread_agg {
            writeln!(
                w,
                concat!(
                    "{{\"thread\":{},\"name_id\":{},\"calls\":{},\"self_ns\":{},\"inclusive_ns\":{},",
                    "\"cpu_self_ns\":{},",
                    "\"alloc_count\":{},\"alloc_bytes\":{},",
                    "\"free_count\":{},\"free_bytes\":{}}}"
                ),
                thread_idx,
                a.name_id,
                a.calls,
                a.self_ns,
                a.inclusive_ns,
                a.cpu_self_ns,
                a.alloc_count,
                a.alloc_bytes,
                a.free_count,
                a.free_bytes,
            )?;
        }
    }
    Ok(())
}

/// Format a u64 as decimal digits into a byte buffer. Returns bytes written.
/// No allocation. Signal-safe.
fn itoa(mut n: u64, buf: &mut [u8]) -> usize {
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 20];
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

/// Serialize a FnAgg as NDJSON into a stack-allocated buffer.
/// No allocation, no format!(). Signal-safe.
pub fn serialize_aggregate_to_stack(
    buf: &mut [u8; 512],
    a: &crate::aggregator::FnAgg,
    thread_idx: u64,
) -> usize {
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

    put!(b"{\"thread\":");
    put_u64!(thread_idx);
    put!(b",\"name_id\":");
    put_u64!(a.name_id as u64);
    put!(b",\"calls\":");
    put_u64!(a.calls);
    put!(b",\"self_ns\":");
    put_u64!(a.self_ns);
    put!(b",\"inclusive_ns\":");
    put_u64!(a.inclusive_ns);
    put!(b",\"cpu_self_ns\":");
    put_u64!(a.cpu_self_ns);
    put!(b",\"alloc_count\":");
    put_u64!(a.alloc_count);
    put!(b",\"alloc_bytes\":");
    put_u64!(a.alloc_bytes);
    put!(b",\"free_count\":");
    put_u64!(a.free_count);
    put!(b",\"free_bytes\":");
    put_u64!(a.free_bytes);
    put!(b"}\n");

    pos
}

fn write_name_table(
    w: &mut impl Write,
    kind: &str,
    names: &[(u32, &str)],
    bias_ns: u64,
    cpu_bias_ns: u64,
) -> io::Result<()> {
    write!(
        w,
        "{{\"type\":\"{kind}\",\"bias_ns\":{bias_ns},\"cpu_bias_ns\":{cpu_bias_ns},\"names\":{{"
    )?;
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
