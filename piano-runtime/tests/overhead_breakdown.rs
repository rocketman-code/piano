//! Overhead breakdown: isolate per-component cost of enter()/drop().
//!
//! Measures proxy operations that replicate the exact work done in
//! enter_cold and drop_cold, giving a component-level cost breakdown
//! of the ~128ns total overhead.
//!
//! Run with: cargo test -p piano-runtime --release --test overhead_breakdown -- --ignored --nocapture

#![allow(
    clippy::incompatible_msrv,
    clippy::missing_const_for_thread_local,
    dead_code
)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const N: u64 = 2_000_000;

fn ns_per_call(elapsed: std::time::Duration) -> f64 {
    elapsed.as_nanos() as f64 / N as f64
}

/// Baseline: empty loop with black_box to prevent optimization.
fn measure_baseline() -> f64 {
    let start = Instant::now();
    for _ in 0..N {
        black_box(42u64);
    }
    ns_per_call(start.elapsed())
}

/// TSC read: single hardware instruction (cntvct_el0 / rdtsc).
fn measure_tsc_read() -> f64 {
    let start = Instant::now();
    for _ in 0..N {
        #[cfg(target_arch = "aarch64")]
        {
            let val: u64;
            unsafe { core::arch::asm!("mrs {}, cntvct_el0", out(reg) val) };
            black_box(val);
        }
        #[cfg(target_arch = "x86_64")]
        {
            black_box(unsafe { core::arch::x86_64::_rdtsc() });
        }
    }
    ns_per_call(start.elapsed())
}

/// TLS access: thread_local! with .with(|x| *x) on a Copy type.
/// enter/drop does this ~6 times total (THREAD_COOKIE, NAME_CACHE, ALLOC_COUNTERS, STACK, RECORDS, FRAME_BUFFER).
fn measure_tls_access() -> f64 {
    thread_local! {
        static TLS_VAL: u64 = 42;
    }
    let start = Instant::now();
    for _ in 0..N {
        black_box(TLS_VAL.with(|v| *v));
    }
    ns_per_call(start.elapsed())
}

/// TLS Cell get/set: replicates ALLOC_COUNTERS save/restore.
/// enter saves + zeroes, drop reads + restores = 4 Cell ops across 2 TLS accesses.
fn measure_tls_cell_get_set() -> f64 {
    #[derive(Clone, Copy, Default)]
    struct Snap {
        a: u64,
        b: u64,
        c: u64,
        d: u64,
    }

    thread_local! {
        static COUNTERS: Cell<Snap> = Cell::new(Snap::default());
    }

    let start = Instant::now();
    for _ in 0..N {
        // enter: save + zero
        let saved = COUNTERS.with(|cell| {
            let snap = cell.get();
            cell.set(Snap::default());
            snap
        });
        black_box(saved);
        // drop: read + restore
        COUNTERS.with(|cell| {
            let _scope = cell.get();
            cell.set(saved);
        });
    }
    ns_per_call(start.elapsed())
}

/// RefCell<Vec> push+pop: replicates STACK operations.
/// enter: borrow().len() + borrow_mut().push()
/// drop: borrow_mut() orphan check + borrow_mut().pop() + borrow_mut().last_mut()
fn measure_refcell_vec_push_pop() -> f64 {
    struct Entry {
        _name: &'static str,
        _children_ms: f64,
        _saved: [u64; 4],
        _packed: u64,
    }

    thread_local! {
        static STACK: RefCell<Vec<Entry>> = RefCell::new(Vec::new());
    }

    let start = Instant::now();
    for i in 0..N {
        STACK.with(|stack| {
            // enter: read depth + push
            let depth = stack.borrow().len();
            stack.borrow_mut().push(Entry {
                _name: "test",
                _children_ms: 0.0,
                _saved: [0; 4],
                _packed: i | ((depth as u64) << 48),
            });
        });
        STACK.with(|stack| {
            // drop: pop + parent update
            let entry = stack.borrow_mut().pop();
            black_box(entry);
            if let Some(parent) = stack.borrow_mut().last_mut() {
                parent._children_ms += 0.001;
            }
        });
    }
    ns_per_call(start.elapsed())
}

/// Name interning cache hit: RefCell<HashMap> lookup.
/// enter calls intern_name() which does TLS + RefCell + HashMap::get.
fn measure_name_intern_cache_hit() -> f64 {
    thread_local! {
        static CACHE: RefCell<HashMap<usize, u16>> = {
            let mut m = HashMap::new();
            m.insert(0x12345678usize, 0u16);
            RefCell::new(m)
        };
    }

    let ptr = 0x12345678usize;
    let start = Instant::now();
    for _ in 0..N {
        let id = CACHE.with(|cache| cache.borrow().get(&ptr).copied());
        black_box(id);
    }
    ns_per_call(start.elapsed())
}

/// Mutex<Vec> lock + push: replicates RECORDS.
/// drop_cold locks RECORDS mutex and pushes a RawRecord every call.
fn measure_mutex_vec_push() -> f64 {
    #[derive(Clone)]
    struct RawRecord {
        _name: &'static str,
        _elapsed_ms: f64,
        _children_ms: f64,
    }

    thread_local! {
        static RECORDS: Arc<Mutex<Vec<RawRecord>>> = Arc::new(Mutex::new(Vec::new()));
    }

    let start = Instant::now();
    for _ in 0..N {
        RECORDS.with(|records| {
            records.lock().unwrap().push(RawRecord {
                _name: "test",
                _elapsed_ms: 0.001,
                _children_ms: 0.0,
            });
        });
    }
    ns_per_call(start.elapsed())
}

/// Mutex lock only (no push): isolate lock acquisition cost.
fn measure_mutex_lock_only() -> f64 {
    let m: Mutex<u64> = Mutex::new(0);
    let start = Instant::now();
    for _ in 0..N {
        let guard = m.lock().unwrap();
        black_box(&*guard);
    }
    ns_per_call(start.elapsed())
}

/// FRAME_BUFFER push: RefCell<Vec<InvocationRecord>> push on every drop.
fn measure_frame_buffer_push() -> f64 {
    struct InvRecord {
        _name: &'static str,
        _start_ns: u64,
        _elapsed_ns: u64,
        _self_ns: u64,
        _alloc_count: u64,
        _alloc_bytes: u64,
        _free_count: u64,
        _free_bytes: u64,
        _depth: u16,
    }

    thread_local! {
        static BUF: RefCell<Vec<InvRecord>> = RefCell::new(Vec::new());
    }

    let start = Instant::now();
    for _ in 0..N {
        BUF.with(|buf| {
            buf.borrow_mut().push(InvRecord {
                _name: "test",
                _start_ns: 100,
                _elapsed_ns: 50,
                _self_ns: 30,
                _alloc_count: 0,
                _alloc_bytes: 0,
                _free_count: 0,
                _free_bytes: 0,
                _depth: 0,
            });
        });
    }
    ns_per_call(start.elapsed())
}

/// AtomicBool load: replicates HAS_PHANTOM_CLEANUP fast-path check.
fn measure_atomic_bool_load() -> f64 {
    static FLAG: AtomicBool = AtomicBool::new(false);
    let start = Instant::now();
    for _ in 0..N {
        black_box(FLAG.load(Ordering::Relaxed));
    }
    ns_per_call(start.elapsed())
}

/// u128 arithmetic: replicates elapsed_ns() tick-to-ns conversion (old path).
/// 2 atomic loads + u128 multiply + u128 divide (calls ___udivti3).
fn measure_elapsed_ns_u128() -> f64 {
    static NUMER: AtomicU64 = AtomicU64::new(125);
    static DENOM: AtomicU64 = AtomicU64::new(3);

    let start_tsc: u64 = 1_000_000;
    let end_tsc: u64 = 1_003_000;

    let start = Instant::now();
    for _ in 0..N {
        let ticks = end_tsc.wrapping_sub(start_tsc);
        let n = NUMER.load(Ordering::Relaxed);
        let d = DENOM.load(Ordering::Relaxed);
        let ns = (ticks as u128 * n as u128 / d as u128) as u64;
        black_box(ns);
    }
    ns_per_call(start.elapsed())
}

/// u64 arithmetic: optimized elapsed_ns() tick-to-ns conversion.
/// 2 atomic loads + u64 wrapping_mul + u64 divide (native udiv).
fn measure_elapsed_ns_u64() -> f64 {
    static NUMER: AtomicU64 = AtomicU64::new(125);
    static DENOM: AtomicU64 = AtomicU64::new(3);

    let start_tsc: u64 = 1_000_000;
    let end_tsc: u64 = 1_003_000;

    let start = Instant::now();
    for _ in 0..N {
        let ticks = end_tsc.wrapping_sub(start_tsc);
        let n = NUMER.load(Ordering::Relaxed);
        let d = DENOM.load(Ordering::Relaxed);
        let ns = ticks.wrapping_mul(n) / d;
        black_box(ns);
    }
    ns_per_call(start.elapsed())
}

/// Full Piano enter/drop at depth 0 (triggers frame aggregation every call).
fn measure_piano_depth0() -> f64 {
    piano_runtime::reset();
    let start = Instant::now();
    for _ in 0..N {
        let _g = piano_runtime::enter("overhead_target");
    }
    let elapsed = start.elapsed();
    piano_runtime::reset();
    ns_per_call(elapsed)
}

/// Piano enter/drop at depth 1 (inside an outer guard, skips frame aggregation).
fn measure_piano_depth1() -> f64 {
    piano_runtime::reset();
    let _outer = piano_runtime::enter("outer");
    let start = Instant::now();
    for _ in 0..N {
        let _g = piano_runtime::enter("inner");
    }
    let elapsed = start.elapsed();
    drop(_outer);
    piano_runtime::reset();
    ns_per_call(elapsed)
}

/// Frame aggregation: HashMap alloc + insert + collect, replicating
/// what aggregate_frame() does on every depth-0 drop.
fn measure_frame_aggregation() -> f64 {
    struct InvRecord {
        name: &'static str,
        self_ns: u64,
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    }

    struct Summary {
        _name: &'static str,
        _calls: u32,
        _self_ns: u64,
        _alloc_count: u64,
        _alloc_bytes: u64,
        _free_count: u64,
        _free_bytes: u64,
    }

    // Single-entry frame (typical for flat loop)
    let records = vec![InvRecord {
        name: "test",
        self_ns: 100,
        alloc_count: 0,
        alloc_bytes: 0,
        free_count: 0,
        free_bytes: 0,
    }];

    let start = Instant::now();
    for _ in 0..N {
        // Replicate aggregate_frame: HashMap insert + collect
        let mut map: HashMap<&'static str, Summary> = HashMap::new();
        for rec in &records {
            let entry = map.entry(rec.name).or_insert(Summary {
                _name: rec.name,
                _calls: 0,
                _self_ns: 0,
                _alloc_count: 0,
                _alloc_bytes: 0,
                _free_count: 0,
                _free_bytes: 0,
            });
            entry._calls += 1;
            entry._self_ns += rec.self_ns;
            entry._alloc_count += rec.alloc_count;
            entry._alloc_bytes += rec.alloc_bytes;
            entry._free_count += rec.free_count;
            entry._free_bytes += rec.free_bytes;
        }
        let result: Vec<Summary> = map.into_values().collect();
        black_box(result);
    }
    ns_per_call(start.elapsed())
}

/// drain().collect() on a 1-element Vec: replicates FRAME_BUFFER drain on depth-0 drop.
fn measure_drain_collect() -> f64 {
    struct InvRecord {
        _name: &'static str,
        _start_ns: u64,
        _elapsed_ns: u64,
        _self_ns: u64,
        _alloc_count: u64,
        _alloc_bytes: u64,
        _free_count: u64,
        _free_bytes: u64,
        _depth: u16,
    }

    thread_local! {
        static BUF: RefCell<Vec<InvRecord>> = RefCell::new(Vec::new());
    }

    let start = Instant::now();
    for _ in 0..N {
        BUF.with(|buf| {
            buf.borrow_mut().push(InvRecord {
                _name: "test",
                _start_ns: 100,
                _elapsed_ns: 50,
                _self_ns: 30,
                _alloc_count: 0,
                _alloc_bytes: 0,
                _free_count: 0,
                _free_bytes: 0,
                _depth: 0,
            });
            let drained: Vec<InvRecord> = buf.borrow_mut().drain(..).collect();
            black_box(drained);
        });
    }
    ns_per_call(start.elapsed())
}

/// Multiple TLS variables accessed together: measures cache thrashing
/// from accessing 8 different thread-locals in sequence.
fn measure_multi_tls() -> f64 {
    thread_local! {
        static T1: Cell<u64> = Cell::new(1);
        static T2: RefCell<Vec<u64>> = RefCell::new(Vec::new());
        static T3: RefCell<HashMap<usize, u16>> = RefCell::new(HashMap::new());
        static T4: Cell<u64> = Cell::new(4);
        static T5: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        static T6: RefCell<Vec<u64>> = RefCell::new(Vec::new());
        static T7: Cell<u64> = Cell::new(7);
        static T8: RefCell<Vec<u64>> = RefCell::new(Vec::new());
    }

    let start = Instant::now();
    for _ in 0..N {
        black_box(T1.with(|c| c.get()));
        T2.with(|v| black_box(v.borrow().len()));
        T3.with(|m| black_box(m.borrow().len()));
        black_box(T4.with(|c| c.get()));
        T5.with(|m| black_box(m.lock().unwrap().len()));
        T6.with(|v| black_box(v.borrow().len()));
        black_box(T7.with(|c| c.get()));
        T8.with(|v| black_box(v.borrow().len()));
    }
    ns_per_call(start.elapsed())
}

#[test]
#[ignore]
fn overhead_breakdown() {
    // Warm up TLS and caches
    piano_runtime::reset();
    for _ in 0..1000 {
        let _g = piano_runtime::enter("warmup");
    }
    piano_runtime::reset();

    let baseline = measure_baseline();
    let tsc = measure_tsc_read();
    let tls = measure_tls_access();
    let cell = measure_tls_cell_get_set();
    let stack = measure_refcell_vec_push_pop();
    let intern = measure_name_intern_cache_hit();
    let mutex_push = measure_mutex_vec_push();
    let mutex_only = measure_mutex_lock_only();
    let frame_buf = measure_frame_buffer_push();
    let atomic = measure_atomic_bool_load();
    let math_u128 = measure_elapsed_ns_u128();
    let math_u64 = measure_elapsed_ns_u64();
    let depth0 = measure_piano_depth0();
    let depth1 = measure_piano_depth1();
    let frame_agg = measure_frame_aggregation();
    let drain = measure_drain_collect();
    let multi_tls = measure_multi_tls();

    eprintln!();
    eprintln!("--- Component Costs ({N} iterations, release build) ---");
    eprintln!();
    eprintln!("  {:<45} {:>8.1}ns", "baseline (black_box)", baseline);
    eprintln!("  {:<45} {:>8.1}ns", "tsc read (single)", tsc);
    eprintln!("  {:<45} {:>8.1}ns", "TLS access (single .with())", tls);
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "TLS Cell get+set (alloc snapshot)", cell
    );
    eprintln!("  {:<45} {:>8.1}ns", "RefCell<Vec> push+pop (STACK)", stack);
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "HashMap get (name intern cache)", intern
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "Mutex<Vec> lock+push (RECORDS)", mutex_push
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "Mutex lock only (no push)", mutex_only
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "RefCell<Vec> push (FRAME_BUFFER)", frame_buf
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "AtomicBool load (phantom check)", atomic
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "u128 math (___udivti3 path)", math_u128
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "u64 math (native udiv path)", math_u64
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "u128 vs u64 delta",
        math_u128 - math_u64
    );
    eprintln!();
    eprintln!("--- Depth-0 Aggregation Path ---");
    eprintln!();
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "HashMap alloc+insert+collect (agg frame)", frame_agg
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "drain().collect() (frame buffer)", drain
    );
    eprintln!();
    eprintln!("--- Interaction Effects ---");
    eprintln!();
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "8 TLS vars accessed together", multi_tls
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "8 x single TLS (no interaction)",
        tls * 8.0
    );
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "TLS interaction overhead",
        multi_tls - tls * 8.0
    );
    eprintln!();
    eprintln!("--- Piano Full Path ---");
    eprintln!();
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "piano depth 0 (with frame agg)", depth0
    );
    eprintln!("  {:<45} {:>8.1}ns", "piano depth 1 (no frame agg)", depth1);
    eprintln!(
        "  {:<45} {:>8.1}ns",
        "depth-0 penalty (frame agg cost)",
        depth0 - depth1
    );
    eprintln!();
}
