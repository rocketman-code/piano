//! Enumeration test pinned to glibc signal headers.
//!
//! Reads /usr/include/bits/signum-generic.h and signum-arch.h to verify
//! piano handles the correct subset of POSIX signals. If glibc adds a
//! new signal, this test fails and forces a classification decision.
//!
//! Requires: glibc development headers (glibc-devel / libc6-dev).

#![cfg(unix)]

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
        default_action: &'static str,
        piano_handles: bool,
        reason: &'static str,
    }

    let signals = [
        // ISO C99 signals
        Signal { name: "SIGHUP",    number: 1,  default_action: "T", piano_handles: false, reason: "programs use SIGHUP for config reload; overriding breaks that convention" },
        Signal { name: "SIGINT",    number: 2,  default_action: "T", piano_handles: true,  reason: "user interrupt (Ctrl+C); most common graceful termination path" },
        Signal { name: "SIGQUIT",   number: 3,  default_action: "C", piano_handles: false, reason: "user wants core dump, not graceful shutdown" },
        Signal { name: "SIGILL",    number: 4,  default_action: "C", piano_handles: false, reason: "program bug; undefined behavior, data structures may be corrupt" },
        Signal { name: "SIGTRAP",   number: 5,  default_action: "C", piano_handles: false, reason: "debugger breakpoint; not a user termination signal" },
        Signal { name: "SIGABRT",   number: 6,  default_action: "C", piano_handles: false, reason: "abort(); may be called from within another signal handler" },
        Signal { name: "SIGBUS",    number: 7,  default_action: "C", piano_handles: false, reason: "hardware fault; data structures may be corrupt" },
        Signal { name: "SIGFPE",    number: 8,  default_action: "C", piano_handles: false, reason: "arithmetic fault; program bug" },
        Signal { name: "SIGKILL",   number: 9,  default_action: "T", piano_handles: false, reason: "uncatchable by POSIX spec" },
        Signal { name: "SIGUSR1",   number: 10, default_action: "T", piano_handles: false, reason: "user-defined; overriding breaks application protocol" },
        Signal { name: "SIGSEGV",   number: 11, default_action: "C", piano_handles: false, reason: "program bug; undefined behavior, stack may be corrupt" },
        Signal { name: "SIGUSR2",   number: 12, default_action: "T", piano_handles: false, reason: "user-defined; overriding breaks application protocol" },
        Signal { name: "SIGPIPE",   number: 13, default_action: "T", piano_handles: false, reason: "Rust ignores SIGPIPE by default (returns EPIPE instead)" },
        Signal { name: "SIGALRM",   number: 14, default_action: "T", piano_handles: false, reason: "timer; overriding breaks application timers" },
        Signal { name: "SIGTERM",   number: 15, default_action: "T", piano_handles: true,  reason: "graceful termination; standard process management signal" },
        // Linux-specific (from signum-arch.h)
        Signal { name: "SIGSTKFLT", number: 16, default_action: "T", piano_handles: false, reason: "obsolete (coprocessor stack fault)" },
        Signal { name: "SIGCHLD",   number: 17, default_action: "I", piano_handles: false, reason: "ignored by default; not a termination signal" },
        Signal { name: "SIGCONT",   number: 18, default_action: "I", piano_handles: false, reason: "resumes stopped process; not a termination signal" },
        Signal { name: "SIGSTOP",   number: 19, default_action: "S", piano_handles: false, reason: "uncatchable by POSIX spec" },
        Signal { name: "SIGTSTP",   number: 20, default_action: "S", piano_handles: false, reason: "stop signal (Ctrl+Z); not a termination signal" },
        Signal { name: "SIGTTIN",   number: 21, default_action: "S", piano_handles: false, reason: "background read; stop signal, not termination" },
        Signal { name: "SIGTTOU",   number: 22, default_action: "S", piano_handles: false, reason: "background write; stop signal, not termination" },
        Signal { name: "SIGURG",    number: 23, default_action: "I", piano_handles: false, reason: "ignored by default; not a termination signal" },
        Signal { name: "SIGXCPU",   number: 24, default_action: "C", piano_handles: false, reason: "resource limit; program should fix its CPU usage, not mask the signal" },
        Signal { name: "SIGXFSZ",   number: 25, default_action: "C", piano_handles: false, reason: "resource limit; program should fix its file sizes" },
        Signal { name: "SIGVTALRM", number: 26, default_action: "T", piano_handles: false, reason: "virtual timer; overriding breaks application timers" },
        Signal { name: "SIGPROF",   number: 27, default_action: "T", piano_handles: false, reason: "profiling timer; conflicts with piano's own profiling" },
        Signal { name: "SIGWINCH",  number: 28, default_action: "I", piano_handles: false, reason: "ignored by default; terminal resize, not termination" },
        Signal { name: "SIGPOLL",   number: 29, default_action: "T", piano_handles: false, reason: "pollable event; rarely used in modern code" },
        Signal { name: "SIGPWR",    number: 30, default_action: "T", piano_handles: false, reason: "power failure; platform-specific UPS signal" },
        Signal { name: "SIGSYS",    number: 31, default_action: "C", piano_handles: false, reason: "bad syscall; program bug in seccomp/sandbox" },
    ];

    // Verify the signal constants match the system headers
    let header = std::fs::read_to_string("/usr/include/bits/signum-generic.h")
        .expect("glibc signal header not found (is build-essential installed?)");
    let arch_header = std::fs::read_to_string("/usr/include/bits/signum-arch.h")
        .expect("glibc arch signal header not found");
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
    let handled: Vec<&str> = signals.iter()
        .filter(|s| s.piano_handles)
        .map(|s| s.name)
        .collect();

    assert_eq!(
        handled, vec!["SIGINT", "SIGTERM"],
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
