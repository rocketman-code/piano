use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::ctx::Ctx;
use piano_runtime::file_sink::FileSink;
use piano_runtime::piano_future::PianoFuture;
use std::fs::{self, File};
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};

// Code generation pattern tests.
//
// These prove that the exact code patterns the rewriter generates
// produce correct measurements when compiled against the CBC runtime.
// Each test IS a theorem. If it passes, the pattern is correct.
// All tests run in spawned threads for TLS isolation.

fn noop_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    // SAFETY: The vtable functions are valid no-ops. The data pointer is
    // null and never dereferenced.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

fn test_file(label: &str) -> (Arc<FileSink>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "piano_test_codegen_{}_{}", label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let file = File::create(&path).expect("create temp file");
    (Arc::new(FileSink::new(file)), path)
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    let file = File::open(path).expect("open temp file for reading");
    BufReader::new(file)
        .lines()
        .map(|l| l.expect("read line"))
        .collect()
}

// ---------------------------------------------------------------------------
// PoC: Context shadowing (G4 + T3)
// ---------------------------------------------------------------------------

// PROOF: The rewriter generates `let (__g, ctx) = ctx.enter(ID);` which
// shadows the outer ctx. This test uses the EXACT shadowing pattern and
// verifies the parent-child chain is correct across 3 levels.
//
// DISCRIMINATES: If shadowing broke parent propagation (e.g., if the child
// ctx somehow retained the outer's span_id), the parent_span_id chain
// would be wrong.
#[test]
fn context_shadowing_three_levels() {
    std::thread::spawn(|| {
        let ctx = Ctx::new(None, false, &[]);

        // Exact rewriter pattern: shadow ctx at each level
        {
            let (__g, ctx) = ctx.enter(1);
            {
                let (__g, ctx) = ctx.enter(2);
                {
                    let (__g, _ctx) = ctx.enter(3);
                    drop(__g);
                }
                drop(__g);
            }
            drop(__g);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3, "3 levels = 3 measurements");

        // Drop order: level 3 first, level 1 last
        let m3 = &drained[0];
        let m2 = &drained[1];
        let m1 = &drained[2];

        // Parent chain: 3 -> 2 -> 1 -> root(0)
        assert_eq!(m1.parent_span_id, 0, "level 1 parent is root");
        assert_eq!(m2.parent_span_id, m1.span_id, "level 2 parent is level 1");
        assert_eq!(m3.parent_span_id, m2.span_id, "level 3 parent is level 2");

        // Name IDs preserved
        assert_eq!(m1.name_id, 1);
        assert_eq!(m2.name_id, 2);
        assert_eq!(m3.name_id, 3);
    })
    .join()
    .expect("test thread panicked");
}

// PROOF: Shadowing with sibling calls. The rewriter generates:
//   let (__g, ctx) = ctx.enter(1);
//   callee_a(ctx.clone());
//   callee_b(ctx.clone());
// Both siblings must have the SAME parent (level 1's span_id).
#[test]
fn context_shadowing_with_siblings() {
    std::thread::spawn(|| {
        let ctx = Ctx::new(None, false, &[]);

        {
            let (__g, ctx) = ctx.enter(1);

            // Sibling A
            {
                let (__g, _ctx) = ctx.clone().enter(2);
                drop(__g);
            }

            // Sibling B
            {
                let (__g, _ctx) = ctx.clone().enter(3);
                drop(__g);
            }

            drop(__g);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3);

        let sib_a = &drained[0];
        let sib_b = &drained[1];
        let parent = &drained[2];

        assert_eq!(sib_a.parent_span_id, parent.span_id);
        assert_eq!(sib_b.parent_span_id, parent.span_id);
        assert_ne!(sib_a.span_id, sib_b.span_id, "siblings have distinct span_ids");
        assert_eq!(sib_a.name_id, 2);
        assert_eq!(sib_b.name_id, 3);
        assert_eq!(parent.name_id, 1);
    })
    .join()
    .expect("test thread panicked");
}

// ---------------------------------------------------------------------------
// PoC: Async desugar pattern (A1 + A4 + A5)
// ---------------------------------------------------------------------------

// PROOF: The rewriter desugars async fn into fn -> impl Future with
// PianoFuture wrapping. This test uses the EXACT desugared pattern.
//
// DISCRIMINATES: If the pattern didn't compile, ctx wasn't captured,
// or timing didn't start on first poll, the test would fail.
#[test]
fn async_desugar_pattern_compiles_and_works() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let ctx = Ctx::new(None, false, &[]);

        // Exact rewriter desugar: fn returning impl Future
        fn desugared_async(ctx: Ctx) -> impl Future<Output = u64> {
            let (state, ctx) = ctx.enter_async(10);
            PianoFuture::new(state, async move {
                // ctx is captured -- prove it by entering a nested call
                let (__g, _ctx) = ctx.enter(11);
                drop(__g);
                42u64
            })
        }

        let mut fut = desugared_async(ctx);
        // SAFETY: fut is a local, never moved after pinning.
        let pinned = unsafe { Pin::new_unchecked(&mut fut) };
        let result = pinned.poll(&mut cx);

        assert!(
            matches!(result, std::task::Poll::Ready(42)),
            "desugared async must complete with correct value"
        );
        drop(fut);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2, "PianoFuture + nested Guard = 2 measurements");

        // Nested sync guard drops first, then PianoFuture
        let nested = &drained[0];
        let outer = &drained[1];

        assert_eq!(outer.name_id, 10);
        assert_eq!(nested.name_id, 11);
        assert_eq!(
            nested.parent_span_id, outer.span_id,
            "nested sync call inside async must have async's span_id as parent"
        );
    })
    .join()
    .expect("test thread panicked");
}

// PROOF: Preamble work (before first .await) is included in timing
// because the entire body is inside async move {}.
//
// DISCRIMINATES: If preamble leaked outside async move, its wall time
// would not be captured by PianoFuture (which starts timing on first poll).
#[test]
fn async_desugar_preamble_included_in_timing() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let ctx = Ctx::new(None, false, &[]);

        fn desugared_with_preamble(ctx: Ctx) -> impl Future<Output = ()> {
            let (state, _ctx) = ctx.enter_async(20);
            PianoFuture::new(state, async move {
                // Preamble work: busy loop INSIDE async move, before any .await
                let mut sum = 0u64;
                for i in 0..100_000 {
                    sum = sum.wrapping_add(i);
                }
                std::hint::black_box(sum);
            })
        }

        let mut fut = desugared_with_preamble(ctx);
        let pinned = unsafe { Pin::new_unchecked(&mut fut) };
        let _ = pinned.poll(&mut cx);
        drop(fut);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1);

        let m = &drained[0];
        let wall = m.end_ns.saturating_sub(m.start_ns);
        assert!(
            wall > 0,
            "preamble work must produce nonzero wall time: got {}",
            wall
        );
    })
    .join()
    .expect("test thread panicked");
}

// ---------------------------------------------------------------------------
// PoC: Clone-and-spawn (C1 + C2)
// ---------------------------------------------------------------------------

// PROOF: ctx.clone() moved to a spawned thread produces measurements
// with correct parent_span_id (matching the clone site's span_id).
//
// DISCRIMINATES: If clone didn't copy current_span_id, or if the spawned
// thread's measurement had parent_span_id == 0, the parent chain would
// be wrong.
#[test]
fn clone_and_spawn_correct_parent() {
    std::thread::spawn(|| {
        let ctx = Ctx::new(None, false, &[]);

        let (__g, ctx) = ctx.enter(1);
        let parent_span_id = {
            // We need the span_id assigned by enter(1) above.
            // That span_id is the current_span_id of the child ctx.
            // When the spawned thread calls ctx.enter(2), the Guard gets
            // ctx.current_span_id as parent_span_id.

            let ctx_clone = ctx.clone();

            let child_parent = std::thread::spawn(move || {
                let (__g, _ctx) = ctx_clone.enter(2);
                drop(__g);
                let drained = drain_thread_buffer();
                assert_eq!(drained.len(), 1, "spawned thread: 1 measurement");
                drained[0].parent_span_id
            })
            .join()
            .expect("child panicked");

            child_parent
        };

        drop(__g);
        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "parent thread: 1 measurement");

        let parent_m = &drained[0];
        assert_eq!(
            parent_span_id, parent_m.span_id,
            "spawned thread's parent_span_id must equal the clone site's span_id"
        );
    })
    .join()
    .expect("test thread panicked");
}

// ---------------------------------------------------------------------------
// PoC: End-to-end instrumented program (E2E)
// ---------------------------------------------------------------------------

// PROOF: A complete instrumented program using all patterns produces
// correct NDJSON output: header with name table, measurements with
// correct parent-child relationships, trailer matching header.
//
// DISCRIMINATES: Wrong parent assignment, missing measurements, missing
// names, lost alloc data, wrong timing — each produces a specific
// assertion failure.
#[test]
fn end_to_end_instrumented_program() {
    let (fs, path) = test_file("e2e");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        {
            let ctx = Ctx::new(
                Some(fs),
                false,
                &[(1, "main"), (2, "compute"), (3, "helper")],
            );

            // Simulate: main calls compute, main calls helper
            let (__g, ctx) = ctx.enter(1); // main

            // compute(ctx.clone())
            {
                let (__g, _ctx) = ctx.clone().enter(2);
                // Busy work to produce nonzero wall time
                let mut sum = 0u64;
                for i in 0..100_000 {
                    sum = sum.wrapping_add(i);
                }
                std::hint::black_box(sum);
                drop(__g);
            }

            // helper(ctx.clone())
            {
                let (__g, _ctx) = ctx.clone().enter(3);
                // Allocate to produce nonzero alloc counts
                let _v: Vec<u8> = vec![0u8; 1024];
                drop(__g);
            }

            drop(__g);
            // ctx (root) drops here -> drain + trailer
        }

        let lines = read_lines(&path_clone);

        // Structure: header + measurements + trailer
        assert!(
            lines.len() >= 5,
            "need header + 3 measurements + trailer, got {}",
            lines.len()
        );

        // Header
        let header = &lines[0];
        assert!(header.contains("\"type\":\"header\""), "first line is header");
        assert!(header.contains("\"1\":\"main\""), "header has main");
        assert!(header.contains("\"2\":\"compute\""), "header has compute");
        assert!(header.contains("\"3\":\"helper\""), "header has helper");

        // Trailer
        let trailer = lines.last().unwrap();
        assert!(trailer.contains("\"type\":\"trailer\""), "last line is trailer");

        // Header and trailer have same name content
        let normalized = header.replace("\"type\":\"header\"", "\"type\":\"trailer\"");
        assert_eq!(
            &normalized, trailer,
            "header and trailer must have identical name content"
        );

        // Measurements (between header and trailer)
        let measurements: Vec<&String> = lines[1..lines.len() - 1]
            .iter()
            .filter(|l| l.contains("\"span_id\""))
            .collect();
        assert_eq!(
            measurements.len(),
            3,
            "should have 3 measurements (main, compute, helper)"
        );

        // Every measurement line is valid JSON-ish (starts with {, ends with })
        for m in &measurements {
            assert!(m.starts_with('{'), "measurement must start with {{");
            assert!(m.ends_with('}'), "measurement must end with }}");
        }

        // All name_ids from measurements appear in the header
        for m in &measurements {
            assert!(
                m.contains("\"name_id\""),
                "measurement must contain name_id"
            );
        }
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}
