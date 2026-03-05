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

## Install

```
cargo install piano
```

Requires Rust 1.88+.

## Usage

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
$ piano build --fn "Parser::parse"           # specific impl method
$ piano build --file src/lexer.rs            # all functions in a file
$ piano build --mod resolver                 # all functions in a module
```

These flags work with `piano profile` too: `piano profile --fn parse -- --input data.csv`.

Add `--cpu-time` to measure per-thread CPU time alongside wall time (Linux + macOS, 64-bit only).

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

### Per-frame breakdown

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

Frames exceeding 2x the median are flagged with `<<` for spike detection.

## How it works

1. Copies your project to a staging directory (your source is never modified)
2. Adds `piano-runtime` as a dependency in the staged Cargo.toml
3. Parses source with `syn` and injects timing guards into matched functions
4. Wraps your global allocator (including cfg-gated ones) for heap tracking
5. Builds with `cargo build --release`, outputs to `target/piano/`

The runtime has zero external dependencies to avoid conflicts with your project.

## When to use something else

`piano` adds ~69ns/call overhead on Apple Silicon and ~303ns/call on x86-64. You can narrow instrumentation with `--fn`, `--file`, or `--mod` to reduce this. For programs with millions of short function calls, a sampling profiler like `perf` or `cargo-flamegraph` will add less overhead. `piano` is a better fit when you want exact call counts, self-time breakdowns, or allocation tracking without configuring OS-level tooling.

## Limitations

- Measurement bias is calibrated at startup; residual bias is ~0.3ns Apple Silicon, ~1.8ns x86-64. Functions faster than the residual floor will show inflated times.
- `const fn`, `unsafe fn`, and `extern fn` are skipped (use `piano build --list-skipped` to see them)
- Allocation tracking covers heap operations only (stack allocations are not tracked)
- Binary crates only (no libraries)

## License

MIT
