use std::cell::UnsafeCell;
use std::sync::Once;

/// A thread-safe cell that is initialized exactly once.
///
/// Equivalent to `OnceLock` (stabilized in 1.70) but works on Rust 1.59+.
/// The `Once` primitive guarantees single-writer semantics; after initialization
/// the value is read-only, so there are no data races.
pub(crate) struct SyncOnceCell<T> {
    once: Once,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: `Once` synchronizes initialization. After `call_once` completes,
// the inner value is only read, never mutated, so `Sync` is sound.
unsafe impl<T: Send + Sync> Sync for SyncOnceCell<T> {}

impl<T> SyncOnceCell<T> {
    pub(crate) const fn new() -> Self {
        Self {
            once: Once::new(),
            value: UnsafeCell::new(None),
        }
    }

    pub(crate) fn get_or_init(&self, f: impl FnOnce() -> T) -> &T {
        self.once.call_once(|| {
            // SAFETY: `call_once` guarantees this runs exactly once, with
            // all subsequent callers blocking until it completes.
            unsafe { *self.value.get() = Some(f()) };
        });
        // SAFETY: After `call_once` returns (on any thread), `value` is
        // `Some` and never mutated again.
        unsafe { (*self.value.get()).as_ref().unwrap() }
    }
}
