/// A completed function measurement -- one record per function invocation.
///
/// Invariants:
/// - wall_time_ns() and cpu_time_ns() never panic (saturating_sub).
/// - All fields are pub for serialization; construction is the caller's
///   responsibility (Guard, PianoFuture).
///
/// Enforcement: saturating arithmetic in convenience methods.
/// No shared state, no TLS, no globals. Pure data struct.

#[derive(Default)]
#[repr(C)]
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

impl Measurement {
    pub fn wall_time_ns(&self) -> u64 {
        self.end_ns.saturating_sub(self.start_ns)
    }

    pub fn cpu_time_ns(&self) -> u64 {
        self.cpu_end_ns.saturating_sub(self.cpu_start_ns)
    }

    pub fn is_root(&self) -> bool {
        self.parent_span_id == 0
    }
}
