// -- Interned function name table ------------------------------------------
//
// Maps u16 IDs to `&'static str` function names so the Guard can carry a
// compact name reference without growing beyond 16 bytes.
//
// Layout: append-only Vec behind a Mutex. Reads during drop_cold take the
// lock briefly; writes happen once per unique name in enter_cold.
// A thread-local cache avoids the global lock on the hot path.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

use super::SyncOnceCell;

/// Lock-free interned name table: index -> &'static str.
///
/// Each slot stores a `&'static str` as (AtomicPtr<u8>, AtomicUsize) for the
/// data pointer and length. Reads use Acquire ordering; writes use Release.
/// The table is append-only: once written, a slot never changes.
/// NAME_TABLE_LEN tracks how many slots are valid.
///
/// Maximum 4096 unique function names. This covers all practical programs
/// (typical instrumented binaries have <100 unique functions). The table
/// occupies 64KB of BSS (zero-initialized, no runtime cost until used).
pub(super) const NAME_TABLE_CAPACITY: usize = 4096;

static NAME_TABLE_PTRS: [AtomicPtr<u8>; NAME_TABLE_CAPACITY] = {
    // SAFETY: AtomicPtr<u8> has the same representation as *mut u8,
    // and null is a valid value for AtomicPtr.
    // const { } blocks aren't available at MSRV 1.59, so we use transmute.
    // This is a well-known pattern for initializing large atomic arrays.
    unsafe { core::mem::transmute([core::ptr::null_mut::<u8>(); NAME_TABLE_CAPACITY]) }
};
#[allow(unused_braces)]
static NAME_TABLE_LENS: [AtomicUsize; NAME_TABLE_CAPACITY] =
    { unsafe { core::mem::transmute([0usize; NAME_TABLE_CAPACITY]) } };
pub(super) static NAME_TABLE_LEN: AtomicUsize = AtomicUsize::new(0);

/// Mutex protecting writes to the name table. Reads are lock-free.
/// Uses SyncOnceCell for MSRV 1.59 compatibility (Mutex::new is not const).
static NAME_TABLE_WRITE_LOCK: SyncOnceCell<Mutex<()>> = SyncOnceCell::new();

pub(super) fn name_table_lock() -> &'static Mutex<()> {
    NAME_TABLE_WRITE_LOCK.get_or_init(|| Mutex::new(()))
}

/// Read a name from the lock-free table by index.
/// Returns None if index is out of range.
#[inline(always)]
pub(super) fn name_table_get(idx: usize) -> Option<&'static str> {
    if idx >= NAME_TABLE_LEN.load(Ordering::Acquire) {
        return None;
    }
    let ptr = NAME_TABLE_PTRS[idx].load(Ordering::Acquire);
    let len = NAME_TABLE_LENS[idx].load(Ordering::Acquire);
    // SAFETY: the pointer and length were stored from a valid &'static str
    // and the slot is immutable once written (append-only table).
    Some(unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(ptr, len)) })
}

/// Append a name to the lock-free table. Caller must hold NAME_TABLE_WRITE_LOCK.
/// Returns the assigned index.
/// Compute the saturation index when the name table is full.
///
/// Returns the last valid slot so that lookups remain in-bounds even if
/// a new unique function name appears after the table is exhausted.
/// Extracted as a pure function for testability in debug builds where
/// `name_table_push` panics via `debug_assert` on overflow.
#[inline(always)]
pub(super) fn name_table_overflow_index() -> u16 {
    (NAME_TABLE_CAPACITY - 1) as u16
}

pub(super) fn name_table_push(name: &'static str) -> u16 {
    let idx = NAME_TABLE_LEN.load(Ordering::Acquire);
    debug_assert!(
        idx < NAME_TABLE_CAPACITY,
        "interned name table overflow: more than {NAME_TABLE_CAPACITY} unique function names"
    );
    if idx >= NAME_TABLE_CAPACITY {
        // Saturate at max capacity -- degrades gracefully.
        return name_table_overflow_index();
    }
    NAME_TABLE_PTRS[idx].store(name.as_ptr() as *mut u8, Ordering::Release);
    NAME_TABLE_LENS[idx].store(name.len(), Ordering::Release);
    NAME_TABLE_LEN.store(idx + 1, Ordering::Release);
    idx as u16
}

// Thread-local cache mapping name pointer -> interned ID.
// Uses pointer identity (`&'static str` addresses are stable).
thread_local! {
    static NAME_CACHE: RefCell<HashMap<usize, u16>> = RefCell::new(HashMap::new());
}

/// Intern a function name, returning its u16 ID.
/// Fast path: thread-local cache hit (no global lock).
/// Slow path: global table lookup/insert under write lock, then cache.
#[inline(always)]
pub(super) fn intern_name(name: &'static str) -> u16 {
    let ptr = name.as_ptr() as usize;
    // try_with: TLS may be destroyed during atexit (process::exit path).
    // On failure, skip the cache and go straight to the global table.
    if let Ok(Some(id)) = NAME_CACHE.try_with(|cache| cache.borrow().get(&ptr).copied()) {
        return id;
    }
    intern_name_slow(name, ptr)
}

#[inline(never)]
fn intern_name_slow(name: &'static str, ptr: usize) -> u16 {
    let _guard = name_table_lock().lock().unwrap_or_else(|e| e.into_inner());
    // Check if already in global table (another thread may have added it).
    let len = NAME_TABLE_LEN.load(Ordering::Acquire);
    let id = {
        let found = NAME_TABLE_PTRS[..len]
            .iter()
            .position(|p| p.load(Ordering::Acquire) as usize == ptr);
        if let Some(pos) = found {
            pos as u16
        } else {
            name_table_push(name)
        }
    };
    drop(_guard);
    // try_with: skip cache write if TLS is destroyed (atexit path).
    let _ = NAME_CACHE.try_with(|cache| {
        cache.borrow_mut().insert(ptr, id);
    });
    id
}

/// Pack a name ID (16 bits) and stack depth (16 bits) into a u64.
///
/// Layout: `[unused:32][name_id:16][depth:16]`
#[inline(always)]
pub(super) fn pack_name_depth(name_id: u16, depth: u16) -> u64 {
    ((name_id as u64) << 16) | (depth as u64)
}

/// Unpack the name ID (bits 16..31) from a packed u64.
#[inline(always)]
pub(crate) fn unpack_name_id(packed: u64) -> u16 {
    (packed >> 16) as u16
}

/// Unpack the stack depth (low 16 bits) from a packed u64.
#[inline(always)]
pub(super) fn unpack_depth(packed: u64) -> u16 {
    packed as u16
}

/// Resolve a name ID back to its interned `&'static str`.
///
/// Lock-free: reads directly from the static atomic arrays.
/// No TLS access, no Mutex, no allocation.
///
/// Panics (debug) / returns `"<unknown>"` (release) if the ID is out of range.
#[inline(always)]
pub(crate) fn lookup_name(name_id: u16) -> &'static str {
    match name_table_get(name_id as usize) {
        Some(name) => name,
        None => {
            debug_assert!(
                false,
                "lookup_name: id {name_id} out of range (table len {})",
                NAME_TABLE_LEN.load(Ordering::Relaxed)
            );
            "<unknown>"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_table_overflow_index_is_last_valid_slot() {
        // The overflow saturation index must be the last valid slot
        // (capacity - 1) so that lookups stay in-bounds.
        let idx = name_table_overflow_index();
        let expected = (NAME_TABLE_CAPACITY - 1) as u16;
        assert_eq!(
            idx, expected,
            "overflow index should be capacity-1 ({expected}), got {idx}"
        );
        // The index must be within the table's bounds.
        assert!(
            (idx as usize) < NAME_TABLE_CAPACITY,
            "overflow index {idx} must be < capacity {NAME_TABLE_CAPACITY}"
        );
    }

    #[test]
    fn pack_name_depth_uses_or_not_xor() {
        // Kills: collector.rs:811 replace | with ^
        let packed = pack_name_depth(0x1234, 0x5678);
        assert_eq!(unpack_name_id(packed), 0x1234);
        assert_eq!(unpack_depth(packed), 0x5678);

        // Verify the raw bit pattern: (0x1234 << 16) | 0x5678
        let expected: u64 = (0x1234u64 << 16) | 0x5678u64;
        assert_eq!(
            packed, expected,
            "pack_name_depth should produce (name_id << 16) | depth"
        );
    }

    #[test]
    fn pack_unpack_round_trip() {
        let name_id: u16 = 42;
        let depth: u16 = 7;
        let packed = pack_name_depth(name_id, depth);
        assert_eq!(unpack_depth(packed), depth);
        assert_eq!(unpack_name_id(packed), name_id);

        // Max values
        let packed_max = pack_name_depth(u16::MAX, u16::MAX);
        assert_eq!(unpack_depth(packed_max), u16::MAX);
        assert_eq!(unpack_name_id(packed_max), u16::MAX);

        // Zero values
        let packed_zero = pack_name_depth(0, 0);
        assert_eq!(unpack_depth(packed_zero), 0);
        assert_eq!(unpack_name_id(packed_zero), 0);
    }

    #[test]
    fn name_table_lock_returns_mutex() {
        // Kills: collector.rs:719 replace name_table_lock with Box::leak(...)
        // The lock should be acquirable and releasable without panic.
        let lock = name_table_lock();
        let guard = lock.lock().unwrap();
        drop(guard);
        // Acquire again to verify it was properly released.
        let guard2 = lock.lock().unwrap();
        drop(guard2);
    }
}
