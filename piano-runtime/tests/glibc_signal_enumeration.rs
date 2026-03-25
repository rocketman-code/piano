//! Enumeration test pinned to glibc signal headers.
//!
//! Reads /usr/include/bits/signum-generic.h and signum-arch.h to verify
//! piano handles the correct subset of POSIX signals. If glibc adds a
//! new signal, this test fails and forces a classification decision.
//!
//! Requires: glibc development headers (glibc-devel / libc6-dev).

#![cfg(target_os = "linux")]

#[test]
fn posix_signals_exhaustive() {
    // Authoritative source: IEEE 1003.1-2017 (POSIX.1), Section 2.4.
    // Pinned via glibc's bits/signum-generic.h + bits/signum-arch.h.
    //
    // Each signal is classified by its default action (T=terminate,
    // C=core dump, S=stop, I=ignore) and whether piano should handle it.

    struct Signal {
        name: &'static str,
        number: i32,
        piano_handles: bool,
    }

    let signals = [
        // ISO C99 signals
        // T=terminate, C=core dump, S=stop, I=ignore (POSIX default actions)
        Signal {
            name: "SIGHUP",
            number: 1,
            piano_handles: false,
        }, // T: programs use for config reload
        Signal {
            name: "SIGINT",
            number: 2,
            piano_handles: true,
        }, // T: user interrupt (Ctrl+C)
        Signal {
            name: "SIGQUIT",
            number: 3,
            piano_handles: false,
        }, // C: user wants core dump
        Signal {
            name: "SIGILL",
            number: 4,
            piano_handles: false,
        }, // C: program bug
        Signal {
            name: "SIGTRAP",
            number: 5,
            piano_handles: false,
        }, // C: debugger breakpoint
        Signal {
            name: "SIGABRT",
            number: 6,
            piano_handles: false,
        }, // C: abort()
        Signal {
            name: "SIGBUS",
            number: 7,
            piano_handles: false,
        }, // C: hardware fault
        Signal {
            name: "SIGFPE",
            number: 8,
            piano_handles: false,
        }, // C: arithmetic fault
        Signal {
            name: "SIGKILL",
            number: 9,
            piano_handles: false,
        }, // T: uncatchable
        Signal {
            name: "SIGUSR1",
            number: 10,
            piano_handles: false,
        }, // T: user-defined
        Signal {
            name: "SIGSEGV",
            number: 11,
            piano_handles: false,
        }, // C: program bug
        Signal {
            name: "SIGUSR2",
            number: 12,
            piano_handles: false,
        }, // T: user-defined
        Signal {
            name: "SIGPIPE",
            number: 13,
            piano_handles: false,
        }, // T: Rust ignores by default
        Signal {
            name: "SIGALRM",
            number: 14,
            piano_handles: false,
        }, // T: timer
        Signal {
            name: "SIGTERM",
            number: 15,
            piano_handles: true,
        }, // T: graceful termination
        // Linux-specific (from signum-arch.h)
        Signal {
            name: "SIGSTKFLT",
            number: 16,
            piano_handles: false,
        }, // T: obsolete
        Signal {
            name: "SIGCHLD",
            number: 17,
            piano_handles: false,
        }, // I: not termination
        Signal {
            name: "SIGCONT",
            number: 18,
            piano_handles: false,
        }, // I: resumes stopped process
        Signal {
            name: "SIGSTOP",
            number: 19,
            piano_handles: false,
        }, // S: uncatchable
        Signal {
            name: "SIGTSTP",
            number: 20,
            piano_handles: false,
        }, // S: Ctrl+Z
        Signal {
            name: "SIGTTIN",
            number: 21,
            piano_handles: false,
        }, // S: background read
        Signal {
            name: "SIGTTOU",
            number: 22,
            piano_handles: false,
        }, // S: background write
        Signal {
            name: "SIGURG",
            number: 23,
            piano_handles: false,
        }, // I: not termination
        Signal {
            name: "SIGXCPU",
            number: 24,
            piano_handles: false,
        }, // C: resource limit
        Signal {
            name: "SIGXFSZ",
            number: 25,
            piano_handles: false,
        }, // C: resource limit
        Signal {
            name: "SIGVTALRM",
            number: 26,
            piano_handles: false,
        }, // T: virtual timer
        Signal {
            name: "SIGPROF",
            number: 27,
            piano_handles: false,
        }, // T: profiling timer
        Signal {
            name: "SIGWINCH",
            number: 28,
            piano_handles: false,
        }, // I: terminal resize
        Signal {
            name: "SIGPOLL",
            number: 29,
            piano_handles: false,
        }, // T: pollable event
        Signal {
            name: "SIGPWR",
            number: 30,
            piano_handles: false,
        }, // T: power failure
        Signal {
            name: "SIGSYS",
            number: 31,
            piano_handles: false,
        }, // C: bad syscall
    ];

    // Verify the signal constants match the system headers.
    // On multiarch systems (Ubuntu 24.04+), headers live under the
    // architecture-specific include path.
    let include_dirs = [
        "/usr/include/bits",
        "/usr/include/x86_64-linux-gnu/bits",
        "/usr/include/aarch64-linux-gnu/bits",
    ];
    let find_header = |name: &str| -> String {
        for dir in &include_dirs {
            let path = format!("{dir}/{name}");
            if let Ok(content) = std::fs::read_to_string(&path) {
                return content;
            }
        }
        panic!(
            "{name} not found in any of {include_dirs:?} (is libc6-dev installed?)"
        );
    };
    let header = find_header("signum-generic.h");
    let arch_header = find_header("signum-arch.h");
    let all_headers = format!("{header}\n{arch_header}");

    for sig in &signals {
        let pattern = format!("#define\t{}\t", sig.name);
        let alt_pattern = format!("#define {}\t", sig.name);
        let found = all_headers.contains(&pattern) || all_headers.contains(&alt_pattern);
        assert!(
            found,
            "{} (number {}) not found in glibc headers. \
             Has the POSIX signal set changed?",
            sig.name, sig.number
        );
    }

    // Verify piano handles exactly the right signals
    let shutdown_source = include_str!("../src/shutdown.rs");
    let handled: Vec<&str> = signals
        .iter()
        .filter(|s| s.piano_handles)
        .map(|s| s.name)
        .collect();

    assert_eq!(
        handled,
        vec!["SIGINT", "SIGTERM"],
        "piano signal handling set has changed. \
         Update this test's classification."
    );

    // Verify the actual signal numbers in shutdown.rs match
    assert!(
        shutdown_source.contains("const SIGINT: std::os::raw::c_int = 2;"),
        "SIGINT constant missing or wrong in shutdown.rs"
    );
    assert!(
        shutdown_source.contains("const SIGTERM: std::os::raw::c_int = 15;"),
        "SIGTERM constant missing or wrong in shutdown.rs"
    );

    // Verify piano registers handlers for exactly these signals
    assert!(
        shutdown_source.contains("signal(SIGINT, handler as usize)"),
        "SIGINT handler registration missing in shutdown.rs"
    );
    assert!(
        shutdown_source.contains("signal(SIGTERM, handler as usize)"),
        "SIGTERM handler registration missing in shutdown.rs"
    );
}
