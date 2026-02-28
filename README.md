# piano

Automated instrumentation-based profiling for Rust. Point it at your project, get back timing, percentiles, and allocation counts per frame.

```
$ piano profile
found 5 function(s) across 3 file(s)
built: target/piano/debug/my-project
... normal program output ...

Function                                    Calls       Self        p50        p99   Allocs      Bytes
----------------------------------------------------------------------------------------------------
parse                                          12   341.21ms    28.11ms    32.44ms      840     62.5KB
resolve                                        47   141.13ms     2.98ms     3.22ms      329     24.1KB
parse_item                                    108    94.58ms     0.87ms     1.14ms     1620    126.3KB

3000 frames
```

Or step by step:

```
$ piano build
found 5 function(s) across 3 file(s)
built: target/piano/debug/my-project

$ piano run
... normal program output ...

$ piano report
```

Piano rewrites your source at the AST level to inject RAII timing guards and an allocation-tracking allocator, builds the instrumented binary, and flushes per-frame results to `target/piano/runs/` on process exit. Your original source is never modified.

## Install

```
cargo install piano
```

Requires Rust 1.88+.

## Usage

### Instrument and build

By default, `piano build` instruments all functions in your project:

```
$ piano build
found 5 function(s) across 3 file(s)
built: target/piano/debug/my-project
```

Narrow scope by name, file, or module:

```
$ piano build --fn parse                    # functions containing "parse"
$ piano build --fn "Parser::parse"          # specific impl method
$ piano build --file src/lexer.rs           # all functions in a file
$ piano build --mod resolver                # all functions in a module
$ piano build --fn parse --fn resolve       # multiple patterns
```

The instrumented binary is written to `target/piano/debug/<name>`.

### Execute the instrumented binary

`piano run` finds and executes the most recently built instrumented binary:

```
$ piano run
$ piano run -- --input data.csv --verbose    # pass arguments after --
```

It looks in `target/piano/debug/` and picks the most recent executable. Piano exits with the binary's exit code.

### One-step profiling

`piano profile` combines build, execute, and report into one command:

```
$ piano profile --fn parse -- --input data.csv
```

### Per-frame profiling

Each instrumented run records per-frame timing and allocation data. The default `piano report` shows aggregate percentiles (above). Use `--frames` for a per-frame breakdown with spike detection:

```
$ piano report --frames
 Frame      Total        parse      resolve   parse_item   Allocs      Bytes
-----------------------------------------------------------------------
     1    48.12ms      28.11ms       2.98ms       0.87ms       93     21.3KB
     2    49.01ms      28.50ms       3.01ms       0.91ms       95     21.8KB
     3    47.88ms      27.94ms       2.95ms       0.85ms       91     20.9KB
     4   102.33ms      71.22ms       3.10ms       1.05ms      210     48.7KB <<
     5    48.55ms      28.30ms       3.00ms       0.89ms       94     21.5KB

5 frames | 1 spikes (>2x median)
```

Frames exceeding 2x the median total time are marked with `<<`.

### Tag and compare runs

```
$ piano tag baseline
tagged 'baseline'

# ... make changes, rebuild, re-run ...

$ piano tag current
tagged 'current'

$ piano diff baseline current
Function                                     Before      After      Delta     Allocs    A.Delta
----------------------------------------------------------------------------------------------
parse                                       341.21ms    198.44ms   -142.77ms        640       -200
resolve                                     141.13ms    141.09ms     -0.04ms        329         +0
parse_item                                   94.58ms     88.12ms     -6.46ms       1240       -380
```

`piano report` and `piano diff` accept file paths or tag names.

### CPU time

Add `--cpu-time` to measure per-thread CPU time alongside wall-clock time (Linux + macOS, 64-bit only):

```
$ piano build --cpu-time
```

The report adds CPU columns so you can distinguish computation from I/O or sleeping.

### Multi-threaded programs

Programs using rayon or `std::thread::spawn` work out of the box. Each thread writes its own timing data with a shared `run_id`. `piano report` consolidates all files from the same run automatically.

## How it works

1. `piano build` copies your project to a staging directory
2. Adds `piano-runtime` as a dependency in the staged `Cargo.toml`
3. Parses Rust source with `syn`, finds functions matching your patterns, and injects `let _guard = piano_runtime::enter("name")` at the top of each
4. Sets `PianoAllocator` as the global allocator to track per-frame heap activity
5. Builds with `cargo build`

Each guard records wall-clock time on construction and drop. Self-time is computed by subtracting children's time from total time. The allocator attributes heap operations to the currently executing instrumented function, and frame summaries are computed when top-level guards complete.

Two crates: `piano` (CLI, AST rewriting, build orchestration) and `piano-runtime` (zero-dependency timing and allocation runtime injected into user projects). The runtime has zero external dependencies to avoid version conflicts.

## Limitations

- Wall-clock timing by default. Use `--cpu-time` to add per-thread CPU time (Linux + macOS only).
- Functions shorter than the guard overhead (~12ns on x86-64, sub-nanosecond on Apple Silicon) will have noisy measurements.
- Async functions record wall time only when futures migrate across threads (self-time may overcount).
- Allocation tracking counts heap operations only (`alloc`/`dealloc`). Stack allocations and memory-mapped regions are not tracked.

## License

MIT
