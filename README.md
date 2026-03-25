# piano

[![Crates.io](https://img.shields.io/crates/v/piano.svg)](https://crates.io/crates/piano)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Automatic instrumentation-based profiler for Rust. Measures self-time, call counts, and heap allocations per function across sync, threaded, and async code.

Given this program:

```rust
#[tokio::main]
async fn main() {
    let config = load_config("settings.toml");
    let data = fetch_all(&config.urls).await;
    let report = analyze(data);
    write_output(report);
}
```

`piano` reports:

```
Function                                       Self    Calls   Allocs  Alloc Bytes
----------------------------------------------------------------------------------
fetch_all                                   341.21ms        1      840       62.5KB
analyze                                     141.13ms        1      329       24.1KB
load_config                                  12.44ms        1       52        4.1KB
write_output                                  3.02ms        1       12        1.2KB
```

Self-time is time spent in the function itself, excluding called functions. `fetch_all` took 341ms of its own work. The `.await` points and thread migrations are tracked automatically. Programs using threads (rayon, std::thread) and async runtimes (tokio, async-std) work automatically.

Output shows the top 10 functions by self-time. Use `--all` to see every instrumented function, or `--top N` to adjust the limit.

## Install

```
cargo install piano
```

Requires Rust 1.88+.

## Usage

Piano instruments binary crates only. Your project must have a `src/main.rs` or a `[[bin]]` target; library crates cannot be profiled directly.

Three steps: instrument, run, report.

```
$ piano build                                # instrument all functions and compile
$ piano run                                  # execute the instrumented binary
$ piano report                               # display results
```

If your program takes arguments, pass them after `--`:

```
$ piano run -- --input data.csv --verbose
```

`piano profile` chains all three steps into one command:

```
$ piano profile                              # build + run + report
$ piano profile -- --input data.csv          # with args passed to your binary
```

Narrow instrumentation to specific functions, files, or modules:

```
$ piano build --fn parse                     # functions matching "parse"
$ piano build --fn parse --exact             # only functions named exactly "parse"
$ piano build --fn "Parser::parse"           # specific impl method
$ piano build --file src/lexer.rs            # all functions in a file
$ piano build --mod resolver                 # all functions in a module
```

These flags work with `piano profile` too: `piano profile --fn parse -- --input data.csv`.

Add `--cpu-time` to measure per-thread CPU time alongside wall time (Linux + macOS, 64-bit only).

### Timed profiling

Run the profiled program for a fixed duration, then stop and report:

```
$ piano profile --duration 10                   # stop after 10 seconds
$ piano profile --duration 2.5                  # fractional seconds supported
```

### Structured output

```
$ piano profile --json                          # JSON to stdout (pipeable)
$ piano report --json                           # same for report
$ piano diff baseline current --json            # same for diff
```

### Multi-binary projects

For workspace crates with multiple binaries:

```
$ piano profile --bin my-server                 # profile a specific binary
```

Piano discovers binaries from `[[bin]]` entries and from `src/bin/` automatically.

### Per-thread breakdown

```
$ piano report --threads                        # show per-thread timing
```

### Raw timing data

By default, piano subtracts its own measurement overhead from reported times (about 8ns per call on x86-64). To see the uncorrected measured values:

```
$ piano report --uncorrected
```

### Compare runs

Tag a run, make changes, profile again, diff:

```
$ piano tag baseline
# ... make changes ...
$ piano profile
$ piano tag current
$ piano diff baseline current
Function                                   baseline    current      Delta     Allocs    A.Delta
----------------------------------------------------------------------------------------------
parse                                      341.21ms    198.44ms   -142.77ms        640       -200
parse_item                                  94.58ms     88.12ms     -6.46ms       1240       -380
resolve                                    141.13ms    141.09ms     -0.04ms        329         +0
```

## How it works

1. Runs `cargo build --release` with a compiler wrapper that instruments each crate during compilation. Your source is never modified; instrumented code goes to temporary files that are cleaned up automatically.
2. Injects `piano-runtime` via `--extern` flag (no changes to your Cargo.toml or lockfile)
3. Parses source with `ra_ap_syntax` and injects timing guards into matched functions (including `impl Future` functions, trait implementations, and functions generated by `macro_rules!` via compile-time expansion)
4. Wraps your global allocator for heap tracking. If the allocator is behind `#[cfg(...)]` (e.g., tikv-jemallocator on Linux only), piano wraps it under the same cfg gate and injects a fallback wrapper for all other platforms automatically.
5. Builds to `target/piano/`. The path is stable across runs, so cargo's incremental cache applies; dependencies compile once and only instrumented source files recompile.

The runtime has zero external dependencies to avoid conflicts with your project.

## When to use something else

`piano` adds ~15ns per instrumented function call (measured on AMD Ryzen 7 3700X at 4.2 GHz). You can narrow instrumentation with `--fn`, `--file`, or `--mod` to reduce this. For programs with millions of short function calls, a sampling profiler like `perf` or `cargo-flamegraph` will add less overhead. `piano` is a better fit when you want exact call counts, self-time breakdowns, or allocation tracking without configuring OS-level tooling.

## Limitations

- Measurement bias is calibrated at startup (two rdtsc reads, ~8ns on x86-64). After bias correction, residual error is under 2ns. Functions faster than the residual floor will show inflated times.
- `const fn` and `extern fn` are skipped (use `piano build --list-skipped` to see them)
- Allocation tracking covers heap operations only (stack allocations are not tracked)
- Binary crates only (no libraries)

## License

MIT
