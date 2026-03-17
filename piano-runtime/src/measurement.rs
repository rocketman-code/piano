/// A completed function measurement -- one record per function invocation.
///
/// All fields are pub for serialization; construction is the caller's
/// responsibility (Guard, PianoFuture).
///
/// No shared state, no TLS, no globals. Pure data struct.

#[derive(Default)]
pub struct Measurement {
    pub span_id: u64,
    pub parent_span_id: u64,
    pub name_id: u32,
    pub start_ns: u64,
    pub end_ns: u64,
    pub thread_id: u64,
    pub cpu_start_ns: u64,
    pub cpu_end_ns: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}
