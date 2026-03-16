//! Prove O4: output never panics, write errors propagated as io::Result.
//!
//! All write operations in output.rs return io::Result. This test
//! uses a writer that always fails and verifies errors are returned,
//! never panicked.

use piano_runtime::measurement::Measurement;
use piano_runtime::output::{write_header, write_measurement, write_measurements, write_trailer};
use std::io::{self, Write};

/// A writer that fails on every write operation.
struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "intentional failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "intentional failure"))
    }
}

/// A writer that fails after N bytes.
struct PartialWriter {
    remaining: usize,
}

impl Write for PartialWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "quota exhausted"));
        }
        let n = buf.len().min(self.remaining);
        self.remaining -= n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sample_measurement() -> Measurement {
    Measurement {
        span_id: 1,
        parent_span_id: 0,
        name_id: 0,
        start_ns: 100,
        end_ns: 200,
        thread_id: 1,
        cpu_start_ns: 0,
        cpu_end_ns: 0,
        alloc_count: 5,
        alloc_bytes: 1024,
        free_count: 0,
        free_bytes: 0,
    }
}

#[test]
fn write_header_returns_err_on_failure() {
    let names = &[(0u32, "foo"), (1, "bar")];
    let result = write_header(&mut FailingWriter, names, 0);
    assert!(result.is_err(), "write_header should return Err, not panic");
}

#[test]
fn write_trailer_returns_err_on_failure() {
    let names = &[(0u32, "foo")];
    let result = write_trailer(&mut FailingWriter, names, 0);
    assert!(result.is_err(), "write_trailer should return Err, not panic");
}

#[test]
fn write_measurement_returns_err_on_failure() {
    let m = sample_measurement();
    let result = write_measurement(&mut FailingWriter, &m);
    assert!(result.is_err(), "write_measurement should return Err, not panic");
}

#[test]
fn write_measurements_returns_err_on_failure() {
    let m = sample_measurement();
    let result = write_measurements(&mut FailingWriter, &[m]);
    assert!(result.is_err(), "write_measurements should return Err, not panic");
}

#[test]
fn write_header_returns_err_on_partial_write() {
    let names = &[(0u32, "foo"), (1, "bar")];
    // Allow a few bytes then fail
    let result = write_header(&mut PartialWriter { remaining: 5 }, names, 0);
    assert!(result.is_err(), "partial write should return Err");
}

#[test]
fn write_measurement_returns_err_on_partial_write() {
    let m = sample_measurement();
    let result = write_measurement(&mut PartialWriter { remaining: 10 }, &m);
    assert!(result.is_err(), "partial write should return Err");
}

#[test]
fn write_header_succeeds_with_good_writer() {
    let names = &[(0u32, "foo"), (1, "bar")];
    let mut buf = Vec::new();
    let result = write_header(&mut buf, names, 0);
    assert!(result.is_ok(), "write_header should succeed with Vec<u8>");
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("\"type\":\"header\""));
    assert!(s.ends_with('\n'));
}

#[test]
fn write_measurement_succeeds_with_good_writer() {
    let m = sample_measurement();
    let mut buf = Vec::new();
    let result = write_measurement(&mut buf, &m);
    assert!(result.is_ok(), "write_measurement should succeed with Vec<u8>");
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("\"span_id\":1"));
    assert!(s.ends_with('\n'));
}
