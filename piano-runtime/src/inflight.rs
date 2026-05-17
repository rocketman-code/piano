#![allow(unsafe_code)]

use core::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crate::time::Ticks;
use crate::NameId;

struct InFlightSlots {
    depth: Box<[AtomicU32]>,
    start: Box<[AtomicU64]>,
    len: usize,
}

static SLOTS: AtomicPtr<InFlightSlots> = AtomicPtr::new(core::ptr::null_mut());

pub(crate) fn init(num_functions: usize) {
    let depth: Vec<AtomicU32> = (0..num_functions).map(|_| AtomicU32::new(0)).collect();
    let start: Vec<AtomicU64> = (0..num_functions).map(|_| AtomicU64::new(0)).collect();
    let slots = Box::new(InFlightSlots {
        depth: depth.into_boxed_slice(),
        start: start.into_boxed_slice(),
        len: num_functions,
    });
    SLOTS.store(Box::into_raw(slots), Ordering::Release);
}

#[inline(always)]
pub(crate) fn enter(name_id: NameId, start_ticks: Ticks) {
    let ptr = SLOTS.load(Ordering::Relaxed);
    if ptr.is_null() {
        return;
    }
    let idx = name_id.0 as usize;
    // SAFETY: ptr was set by init() via Box::into_raw and is never freed.
    // idx is bounds-checked against slots.len before array access.
    unsafe {
        let slots = &*ptr;
        if idx >= slots.len {
            return;
        }
        let prev = slots.depth[idx].fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            slots.start[idx].store(start_ticks.0, Ordering::Relaxed);
        }
    }
}

#[inline(always)]
pub(crate) fn exit(name_id: NameId) {
    let ptr = SLOTS.load(Ordering::Relaxed);
    if ptr.is_null() {
        return;
    }
    let idx = name_id.0 as usize;
    // SAFETY: ptr was set by init() via Box::into_raw and is never freed.
    // idx is bounds-checked against slots.len before array access.
    unsafe {
        let slots = &*ptr;
        if idx >= slots.len {
            return;
        }
        let prev = slots.depth[idx].fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            slots.start[idx].store(0, Ordering::Relaxed);
        }
    }
}

pub(crate) struct InterruptedEntry {
    pub(crate) name_id: NameId,
    pub(crate) start_ticks: Ticks,
    pub(crate) depth: u32,
}

pub(crate) fn drain() -> Vec<InterruptedEntry> {
    let ptr = SLOTS.load(Ordering::Acquire);
    if ptr.is_null() {
        return Vec::new();
    }
    // SAFETY: ptr was set by init() via Box::into_raw, never freed.
    let slots = unsafe { &*ptr };
    let mut entries = Vec::new();
    for i in 0..slots.len {
        let depth = slots.depth[i].load(Ordering::Relaxed);
        if depth > 0 {
            let start = slots.start[i].load(Ordering::Relaxed);
            if start > 0 {
                entries.push(InterruptedEntry {
                    name_id: NameId(i as u32),
                    start_ticks: Ticks(start),
                    depth,
                });
            }
        }
    }
    entries
}
