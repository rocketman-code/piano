# piano

Automated instrumentation-based profiling for Rust. Point it at some functions, get back where your program spends its time. No sampling, no kernel access, no manual annotations.

```
$ piano build --fn parse --fn resolve
found 3 function(s) across 2 file(s)
built: target/piano/debug/my-project

$ ./target/piano/debug/my-project
... normal program output ...

$ piano report
Function                                    Calls      Total       Self
------------------------------------------------------------------------
parse                                          12   482.33ms   341.21ms
resolve                                        47   141.13ms   141.13ms
```

Piano rewrites your source at the AST level to inject RAII timing guards, builds the instrumented binary, and flushes results to `~/.piano/runs/` on process exit. Your original source is never modified.

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

### Tag and compare runs

```
$ piano tag baseline
tagged 'baseline' -> 98321_1740000000000

# ... make changes, rebuild, re-run ...

$ piano tag current
tagged 'current' -> 98321_1740000060000

$ piano diff baseline current
Function                                     Before      After      Delta
--------------------------------------------------------------------------
parse                                      341.21ms   198.44ms  -142.77ms
resolve                                    141.13ms   141.09ms    -0.04ms
```

`piano report` and `piano diff` accept file paths or tag names.

### Multi-threaded programs

Programs using rayon or `std::thread::spawn` work out of the box. Each thread writes its own timing data with a shared `run_id`. `piano report` consolidates all files from the same run automatically.

Functions that are instrumented but never called appear in the report with `calls: 0`.

## How it works

1. `piano build` copies your project to a staging directory
2. Parses Rust source with `syn`, finds functions matching your patterns
3. Injects `let _guard = piano_runtime::enter("name")` at the top of each function
4. Adds `piano-runtime` as a dependency in the staged `Cargo.toml`
5. Builds with `cargo build`

Each guard records wall-clock time on construction and drop. Self-time is computed by subtracting children's time from total time -- if `main` calls `parse` which takes 300ms, that 300ms is subtracted from `main`'s self-time.

Two crates: `piano` (CLI, AST rewriting, build orchestration) and `piano-runtime` (zero-dependency timing runtime injected into user projects). The runtime has zero external dependencies to avoid version conflicts when injected into arbitrary projects.

## Accuracy

Piano's self-time percentages match macOS's native `sample` profiler within 10 percentage points on compute-bound workloads. Guard overhead is approximately 100-120ns per instrumented function call.

## Limitations

- Wall-clock timing, not CPU time. Sleeping or blocked I/O counts as elapsed time.
- Functions shorter than the guard overhead (~120ns) will have noisy measurements.
- Each thread's call tree is independent. Cross-thread relationships (spawned tasks, async executors) appear as separate profiles.

## License

MIT
