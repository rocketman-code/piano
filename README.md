# piano

Automated instrumentation-based profiling for Rust. Point it at some functions, get back timing, percentiles, and allocation counts per frame.

```
$ piano build --fn parse --fn resolve
found 3 function(s) across 2 file(s)
built: target/piano/debug/my-project

$ ./target/piano/debug/my-project
... normal program output ...

$ piano report
Function                                 Calls       Self        p50        p99    Allocs      Bytes
----------------------------------------------------------------------------------------------------
parse                                       12    341.21ms     28.11ms     32.44ms        840    62.5KB
resolve                                     47    141.13ms      2.98ms      3.22ms        329    24.1KB
parse_item                                 108     94.58ms      0.87ms      1.14ms       1620   126.3KB

3000 frames
```

Piano rewrites your source at the AST level to inject RAII timing guards and an allocation-tracking allocator, builds the instrumented binary, and flushes per-frame results to `~/.piano/runs/` on process exit. Your original source is never modified.

## Install

```
cargo install piano
```

Requires Rust 2024 edition (1.85+).

## Usage

### Instrument and build

Target functions by name, file, or module:

```
$ piano build --fn parse                    # functions containing "parse"
$ piano build --fn "Parser::parse"          # specific impl method
$ piano build --file src/lexer.rs           # all functions in a file
$ piano build --mod resolver                # all functions in a module
$ piano build --fn parse --fn resolve       # multiple patterns
```

The instrumented binary is written to `target/piano/debug/<name>`.

### Per-frame profiling

Each instrumented run records per-frame timing and allocation data. The default `piano report` shows aggregate percentiles (above). Use `--frames` for a per-frame breakdown with spike detection:

```
$ piano report --frames
 Frame      Total        parse      resolve   parse_item   Allocs      Bytes
-----------------------------------------------------------------------------
     1     48.12ms      28.11ms       2.98ms       0.87ms       93    21.3KB
     2     49.01ms      28.50ms       3.01ms       0.91ms       95    21.8KB
     3     47.88ms      27.94ms       2.95ms       0.85ms       91    20.9KB
     4    102.33ms      71.22ms       3.10ms       1.05ms      210    48.7KB <<
     5     48.55ms      28.30ms       3.00ms       0.89ms       94    21.5KB

5 frames | 1 spikes (>2x median)
```

Frames exceeding 2x the median total time are marked with `<<`.

### Tag and compare runs

```
$ piano tag baseline
tagged 'baseline' -> 98321_1740000000000

# ... make changes, rebuild, re-run ...

$ piano tag current
tagged 'current' -> 98321_1740000060000

$ piano diff baseline current
Function                                     Before      After      Delta     Allocs    A.Delta
----------------------------------------------------------------------------------------------
parse                                       341.21ms   198.44ms  -142.77ms        840       -200
resolve                                     141.13ms   141.09ms    -0.04ms        329         +0
parse_item                                   94.58ms    88.12ms    -6.46ms       1620       -380
```

`piano report` and `piano diff` accept file paths or tag names.

### Multi-threaded programs

Programs using rayon or `std::thread::spawn` work out of the box. Each thread writes its own timing data with a shared `run_id`. `piano report` consolidates all files from the same run automatically.

## How it works

1. `piano build` copies your project to a staging directory
2. Parses Rust source with `syn`, finds functions matching your patterns
3. Injects `let _guard = piano_runtime::enter("name")` at the top of each function
4. Sets `PianoAllocator` as the global allocator to track per-frame heap activity
5. Adds `piano-runtime` as a dependency in the staged `Cargo.toml`
6. Builds with `cargo build`

Each guard records wall-clock time on construction and drop. Self-time is computed by subtracting children's time from total time. The allocator counts allocations and bytes between frame boundaries with zero configuration.

Two crates: `piano` (CLI, AST rewriting, build orchestration) and `piano-runtime` (zero-dependency timing and allocation runtime injected into user projects). The runtime has zero external dependencies to avoid version conflicts.

## Limitations

- Wall-clock timing, not CPU time. Sleeping or blocked I/O counts as elapsed time.
- Functions shorter than the guard overhead (~120ns) will have noisy measurements.
- Each thread's call tree is independent. Cross-thread relationships (spawned tasks, async executors) appear as separate profiles.
- Allocation tracking counts heap operations only (`alloc`/`dealloc`). Stack allocations and memory-mapped regions are not tracked.

## License

MIT
