// thread_id module is pub(crate) -- tested indirectly via guard/piano_future.
// Same rationale as time.rs: 15 lines, no branches beyond the TLS cache check,
// correctness follows from AtomicU64::fetch_add uniqueness guarantee.
//
// The invariants (unique, non-zero, stable per thread) will be verified
// through Guard and PianoFuture tests that check Measurement.thread_id.
