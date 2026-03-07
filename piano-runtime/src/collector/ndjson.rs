use std::collections::HashSet;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use super::name_table::{intern_name, name_table_get, NAME_TABLE_LEN};
use super::{
    collect_all_fnagg, collect_frames_with_tid, run_id, stream_file, synthesize_frame_from_agg,
    timestamp_ms, FrameFnSummary, FRAMES, SHUTDOWN_DONE, STREAMING_ENABLED, THREAD_INDEX,
};

/// State for the streaming NDJSON file.
pub(super) struct StreamState {
    pub(super) file: std::io::BufWriter<std::fs::File>,
    pub(super) path: PathBuf,
    pub(super) frame_count: usize,
}

/// Open a new streaming NDJSON file and write the v4 header.
pub(super) fn open_stream_file(dir: &std::path::Path) -> std::io::Result<StreamState> {
    std::fs::create_dir_all(dir)?;
    let ts = timestamp_ms();
    let path = dir.join(format!("{ts}.ndjson"));
    let mut file = std::io::BufWriter::new(std::fs::File::create(&path)?);
    let run_id = run_id();

    write!(
        file,
        "{{\"format_version\":4,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts}"
    )?;
    #[cfg(feature = "cpu-time")]
    write!(file, ",\"has_cpu_time\":true")?;
    writeln!(file, "}}")?;

    Ok(StreamState {
        file,
        path,
        frame_count: 0,
    })
}

/// Write a frame to an already-locked StreamState.
pub(super) fn stream_frame_to_writer(state: &mut StreamState, buf: &[FrameFnSummary]) {
    let frame_idx = state.frame_count;
    let tid = THREAD_INDEX.with(|c| c.get());
    let _ = write!(
        state.file,
        "{{\"frame\":{frame_idx},\"tid\":{tid},\"fns\":["
    );
    for (i, entry) in buf.iter().enumerate() {
        if i > 0 {
            let _ = write!(state.file, ",");
        }
        let fn_id = intern_name(entry.name);
        let _ = write!(
            state.file,
            "{{\"id\":{},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}",
            fn_id,
            entry.calls,
            entry.self_ns,
            entry.alloc_count,
            entry.alloc_bytes,
            entry.free_count,
            entry.free_bytes
        );
        #[cfg(feature = "cpu-time")]
        let _ = write!(state.file, ",\"csn\":{}", entry.cpu_self_ns);
        let _ = write!(state.file, "}}");
    }
    let _ = writeln!(state.file, "]}}");
    state.frame_count += 1;
}

/// Push a frame into the thread-local in-memory buffer (fallback path).
pub(super) fn push_to_frames(buf: &[FrameFnSummary]) {
    FRAMES.with(|frames| {
        frames
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(buf.to_vec());
    });
}

/// Stream a completed frame to disk, or fall back to in-memory FRAMES.
///
/// When streaming is enabled (init() was called), writes one NDJSON line
/// per frame to the global stream file. When not enabled (tests), pushes
/// to the thread-local FRAMES vec as before.
pub(super) fn stream_frame(buf: &[FrameFnSummary]) {
    if !STREAMING_ENABLED.load(Ordering::Relaxed) {
        push_to_frames(buf);
        return;
    }

    let dir = match super::runs_dir() {
        Some(d) => d,
        None => {
            push_to_frames(buf);
            return;
        }
    };

    let mut state = stream_file().lock().unwrap_or_else(|e| e.into_inner());

    if state.is_none() {
        // After shutdown, the stream file has been closed and the trailer
        // written. Don't open a new orphan file (it would have no trailer
        // and the BufWriter might not flush before process exit).
        if SHUTDOWN_DONE.load(Ordering::Relaxed) {
            return;
        }
        match open_stream_file(&dir) {
            Ok(s) => *state = Some(s),
            Err(e) => {
                eprintln!("piano: failed to open stream file: {e}");
                drop(state);
                push_to_frames(buf);
                return;
            }
        }
    }

    if let Some(ref mut s) = *state {
        stream_frame_to_writer(s, buf);
    }
}

/// Write the function name table as a trailer line and flush.
pub(super) fn write_stream_trailer(state: &mut StreamState) -> std::io::Result<()> {
    let len = NAME_TABLE_LEN.load(Ordering::Acquire);
    write!(state.file, "{{\"functions\":[")?;
    for i in 0..len {
        if i > 0 {
            write!(state.file, ",")?;
        }
        let name = name_table_get(i).unwrap_or("<unknown>");
        let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(state.file, "\"{escaped}\"")?;
    }
    writeln!(state.file, "]}}")?;
    state.file.flush()?;
    Ok(())
}

/// Write an NDJSON file with frame-level data.
///
/// Line 1: header with metadata and function name table.
/// Lines 2+: one line per frame with per-function summaries.
pub(super) fn write_ndjson(
    frames: &[(usize, Vec<FrameFnSummary>)],
    fn_names: &[&str],
    path: &std::path::Path,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = BufWriter::new(std::fs::File::create(path)?);
    let ts = timestamp_ms();
    let run_id = run_id();

    // v4 header: metadata only (no functions -- those go in the trailer)
    write!(
        f,
        "{{\"format_version\":4,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts}"
    )?;
    #[cfg(feature = "cpu-time")]
    write!(f, ",\"has_cpu_time\":true")?;
    writeln!(f, "}}")?;

    // Build index for O(1) fn_id lookup
    let fn_id_map: std::collections::HashMap<&str, usize> =
        fn_names.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // One line per frame
    for (frame_idx, (tid, frame)) in frames.iter().enumerate() {
        write!(f, "{{\"frame\":{frame_idx},\"tid\":{tid},\"fns\":[")?;
        for (i, s) in frame.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            let fn_id = fn_id_map.get(s.name).copied().unwrap_or(0);
            write!(
                f,
                "{{\"id\":{},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}",
                fn_id, s.calls, s.self_ns, s.alloc_count, s.alloc_bytes, s.free_count, s.free_bytes
            )?;
            #[cfg(feature = "cpu-time")]
            write!(f, ",\"csn\":{}", s.cpu_self_ns)?;
            write!(f, "}}")?;
        }
        writeln!(f, "]}}")?;
    }

    // v4 trailer: function name table
    write!(f, "{{\"functions\":[")?;
    for (i, name) in fn_names.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        let name = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(f, "\"{name}\"")?;
    }
    writeln!(f, "]}}")?;

    Ok(())
}

/// Shared implementation for `flush()` (non-streaming path) and `flush_to()`.
///
/// Collects (clones) frames from all threads -- we intentionally do NOT drain
/// other threads' frames here. Only the local thread's state is cleared via
/// `reset()`. Other threads' frames persist for the final `shutdown()` write.
pub(super) fn flush_impl(dir: &std::path::Path) {
    let mut frames = collect_frames_with_tid();

    // Synthesize from aggregates if no frames exist (same as shutdown_impl_inner).
    if frames.is_empty() {
        let agg = collect_all_fnagg();
        if agg.is_empty() {
            return;
        }
        frames.push((0, synthesize_frame_from_agg(&agg)));
    }

    let mut seen = HashSet::new();
    let mut fn_names: Vec<&str> = Vec::new();
    for (_, frame) in &frames {
        for s in frame {
            if seen.insert(s.name) {
                fn_names.push(s.name);
            }
        }
    }
    let path = dir.join(format!("{}.ndjson", timestamp_ms()));
    if let Err(e) = write_ndjson(&frames, &fn_names, &path) {
        eprintln!(
            "piano: failed to write profiling data to {}: {e}",
            path.display()
        );
    }
    // Clear only the local thread's records, stack, and frames so subsequent
    // enter() calls start fresh. Other threads' state is left intact.
    super::reset();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::name_table::intern_name;
    use crate::collector::{burn_cpu, collect_frames, enter, register, reset};
    use serial_test::serial;

    #[test]
    #[serial]
    fn stream_writes_valid_v4_ndjson() {
        reset();
        let tmp = std::env::temp_dir().join(format!("piano_stream_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Test the streaming infrastructure directly by calling
        // open_stream_file / stream_frame_to_writer / write_stream_trailer.
        // This avoids setting the global STREAMING_ENABLED flag, which would
        // interfere with parallel tests that expect the FRAMES path.
        let mut state = open_stream_file(&tmp).unwrap();

        // Generate 3 frames worth of data
        for _ in 0..3 {
            let _g = enter("stream_test_fn");
            burn_cpu(5_000);
        }

        // Collect the frames that were pushed to FRAMES (streaming disabled).
        let frames = collect_frames();
        let my_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "stream_test_fn"))
            .collect();
        assert_eq!(
            my_frames.len(),
            3,
            "should have 3 frames with stream_test_fn"
        );

        // Write each frame to the stream file
        for frame in &my_frames {
            stream_frame_to_writer(&mut state, frame);
        }

        // Write trailer and flush
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        // Find the .ndjson file
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        assert_eq!(files.len(), 1, "expected exactly one ndjson file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Header (line 0): metadata, no functions
        assert!(
            lines[0].contains("\"format_version\":4"),
            "header should have format_version 4: {}",
            lines[0]
        );
        assert!(lines[0].contains("\"run_id\""));
        assert!(!lines[0].contains("\"functions\""));

        // Frames (lines 1-3)
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));
        assert!(lines[3].contains("\"frame\":2"));

        // Trailer (last line): functions array
        let last = lines.last().unwrap();
        assert!(last.contains("\"functions\""));
        assert!(last.contains("stream_test_fn"));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn shutdown_streaming_writes_trailer() {
        // Test that the streaming shutdown path produces a complete v4 file.
        //
        // Uses local StreamState (not the global STREAM_FILE) to avoid
        // racing with frames_on_disk_before_shutdown. Constructs frame
        // data directly to avoid collect_frames() interference.
        let tmp =
            std::env::temp_dir().join(format!("piano_shutdown_trailer_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Register the function name so write_stream_trailer includes it.
        register("shutdown_trailer_fn");

        // Build frame data directly -- no reliance on global FRAMES.
        let frame = vec![FrameFnSummary {
            name: "shutdown_trailer_fn",
            calls: 1,
            self_ns: 1_000_000,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 500_000,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];

        // Simulate the streaming path: open file, write 2 frames, write trailer.
        let mut state = open_stream_file(&tmp).unwrap();
        stream_frame_to_writer(&mut state, &frame);
        stream_frame_to_writer(&mut state, &frame);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        assert_eq!(files.len(), 1);

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();

        // Header + 2 frames + trailer = 4 lines
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("\"format_version\":4"));
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));
        assert!(lines[3].contains("\"functions\""));
        assert!(lines[3].contains("shutdown_trailer_fn"));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn stream_frame_field_values_round_trip() {
        let tmp = std::env::temp_dir().join(format!("piano_rt_roundtrip_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        let frame_data = vec![FrameFnSummary {
            name: "roundtrip_fn",
            calls: 42,
            self_ns: 123_456_789,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 100_000_000,
            alloc_count: 7,
            alloc_bytes: 2048,
            free_count: 3,
            free_bytes: 1024,
        }];

        stream_frame_to_writer(&mut state, &frame_data);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        let frame_line = lines[1];
        assert!(frame_line.contains("\"frame\":0"), "frame index");

        let fns_start = frame_line.find("\"fns\":[").unwrap() + "\"fns\":[".len();
        let fns_end = frame_line[fns_start..].rfind(']').unwrap();
        let entry_str = &frame_line[fns_start..fns_start + fns_end];

        fn extract(s: &str, key: &str) -> u64 {
            let start = s.find(key).unwrap() + key.len();
            let end = s[start..]
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(s.len() - start);
            s[start..start + end].parse().unwrap()
        }

        assert_eq!(extract(entry_str, "\"calls\":"), 42, "calls");
        assert_eq!(extract(entry_str, "\"self_ns\":"), 123_456_789, "self_ns");
        assert_eq!(extract(entry_str, "\"ac\":"), 7, "alloc_count");
        assert_eq!(extract(entry_str, "\"ab\":"), 2048, "alloc_bytes");
        assert_eq!(extract(entry_str, "\"fc\":"), 3, "free_count");
        assert_eq!(extract(entry_str, "\"fb\":"), 1024, "free_bytes");

        let trailer = *lines.last().unwrap();
        assert!(
            trailer.contains("roundtrip_fn"),
            "trailer should contain function name"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn stream_frame_to_writer_comma_separation() {
        // Kills: collector.rs:171 replace > with ==/</>=
        // With 2+ entries, commas should separate them. With 1 entry, no comma.
        let tmp = std::env::temp_dir().join(format!("piano_comma_sep_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        // Single entry: no comma
        let single = vec![FrameFnSummary {
            name: "comma_a",
            calls: 1,
            self_ns: 100,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];
        stream_frame_to_writer(&mut state, &single);

        // Multiple entries: commas between them
        let multi = vec![
            FrameFnSummary {
                name: "comma_b",
                calls: 1,
                self_ns: 100,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: "comma_c",
                calls: 2,
                self_ns: 200,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
        ];
        stream_frame_to_writer(&mut state, &multi);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Frame 0 (single entry): no comma in fns array, no leading comma.
        // Mutant `i > 0` -> `i >= 0` would produce [,{...}] (leading comma).
        let frame0_fns = &lines[1][lines[1].find("\"fns\":[").unwrap()..];
        let comma_count_0 = frame0_fns.matches("},{").count();
        assert_eq!(
            comma_count_0, 0,
            "single-entry frame should have no comma between entries"
        );
        assert!(
            frame0_fns.contains("\"fns\":[{"),
            "fns array should start with [{{ not [,{{: {frame0_fns}"
        );

        // Frame 1 (two entries): exactly one comma between entries, no leading comma.
        let frame1_fns = &lines[2][lines[2].find("\"fns\":[").unwrap()..];
        let comma_count_1 = frame1_fns.matches("},{").count();
        assert_eq!(
            comma_count_1, 1,
            "two-entry frame should have exactly one comma separator"
        );
        assert!(
            frame1_fns.contains("\"fns\":[{"),
            "fns array should start with [{{ not [,{{: {frame1_fns}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn write_stream_trailer_comma_separation() {
        // Kills: collector.rs:253 replace > with >=
        // Verifies commas between function names in the trailer.
        let tmp = std::env::temp_dir().join(format!("piano_trailer_comma_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Intern at least 2 names so the trailer has commas.
        let _id1 = intern_name("trailer_comma_a");
        let _id2 = intern_name("trailer_comma_b");

        let mut state = open_stream_file(&tmp).unwrap();
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let trailer = content.lines().last().unwrap();

        // With >= instead of >, a leading comma would appear: [,"name1","name2"]
        assert!(
            !trailer.contains("[,"),
            "trailer should not start with a comma: {trailer}"
        );
        // With > replaced by ==, only the first entry would get a comma prefix
        // (when i == 0, which is wrong). Check structure is valid.
        assert!(
            trailer.contains("\"functions\":[\""),
            "trailer should have functions array starting with a quote: {trailer}"
        );
    }

    #[test]
    #[serial]
    fn write_ndjson_comma_separation() {
        // Kills: collector.rs:1303 and 1319 replace > with ==/</>=
        let tmp = std::env::temp_dir().join(format!("piano_ndjson_comma_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let fn_names = vec!["ndjson_fn_a", "ndjson_fn_b"];
        let frames = vec![(
            0,
            vec![
                FrameFnSummary {
                    name: "ndjson_fn_a",
                    calls: 1,
                    self_ns: 100,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns: 0,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                },
                FrameFnSummary {
                    name: "ndjson_fn_b",
                    calls: 2,
                    self_ns: 200,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns: 0,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                },
            ],
        )];

        let path = tmp.join("test.ndjson");
        write_ndjson(&frames, &fn_names, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // v4: header has no functions array; trailer (last line) has it.
        let header = lines[0];
        assert!(
            !header.contains("\"functions\""),
            "v4 header should not contain functions: {header}"
        );
        let trailer = *lines.last().unwrap();
        assert!(
            trailer.contains("\"functions\":[\"ndjson_fn_a\",\"ndjson_fn_b\"]"),
            "trailer functions array should have proper comma separation: {trailer}"
        );

        // Frame line: fns array should have comma between entries, not before first.
        let frame = lines[1];
        let fns_section = &frame[frame.find("\"fns\":[").unwrap()..];
        assert!(
            !fns_section.starts_with("\"fns\":[,"),
            "fns array should not start with comma: {fns_section}"
        );
        let entry_count = fns_section.matches("\"id\":").count();
        assert_eq!(entry_count, 2, "should have 2 fn entries");
        let comma_between = fns_section.matches("},{").count();
        assert_eq!(
            comma_between, 1,
            "should have exactly 1 comma between 2 entries"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn trailer_fn_id_round_trip() {
        let tmp = std::env::temp_dir().join(format!("piano_trailer_rt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        let name_plain: &'static str = "simple_fn";
        let name_generic: &'static str = "Vec<String>::push";
        let name_backslash: &'static str = "path\\to\\fn";

        let id_plain = intern_name(name_plain);
        let id_generic = intern_name(name_generic);
        let id_backslash = intern_name(name_backslash);

        let frame = vec![
            FrameFnSummary {
                name: name_plain,
                calls: 1,
                self_ns: 100,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: name_generic,
                calls: 2,
                self_ns: 200,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: name_backslash,
                calls: 3,
                self_ns: 300,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
        ];
        stream_frame_to_writer(&mut state, &frame);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        let trailer = *lines.last().unwrap();

        // Parse trailer: {"functions":["name0","name1",...]}
        let fns_start = trailer.find("\"functions\":[").unwrap() + "\"functions\":[".len();
        let fns_end = trailer[fns_start..].find(']').unwrap();
        let fns_str = &trailer[fns_start..fns_start + fns_end];

        let parsed: Vec<String> = fns_str
            .split("\",\"")
            .map(|s| {
                s.trim_matches('"')
                    .replace("\\\\", "\\")
                    .replace("\\\"", "\"")
            })
            .collect();

        assert_eq!(
            parsed.get(id_plain as usize).map(|s| s.as_str()),
            Some("simple_fn"),
            "plain name at id {id_plain}"
        );
        assert_eq!(
            parsed.get(id_generic as usize).map(|s| s.as_str()),
            Some("Vec<String>::push"),
            "generic name at id {id_generic}"
        );
        assert_eq!(
            parsed.get(id_backslash as usize).map(|s| s.as_str()),
            Some("path\\to\\fn"),
            "backslash name at id {id_backslash}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
