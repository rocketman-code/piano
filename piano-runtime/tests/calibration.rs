// Empirical measurement: how much does bias correction actually help?
//
// This test measures what the Guard ITSELF reports via the Measurement struct
// (end_ns - start_ns, cpu_end_ns - cpu_start_ns) -- the exact values the user
// sees in the profiling report. Previous version measured from OUTSIDE the
// Guard, which includes overhead not relevant to the user-visible data.

use piano_runtime::alloc::PianoAllocator;
use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::cpu_clock;
use piano_runtime::guard::Guard;
use piano_runtime::time;
use std::alloc::System;
use std::hint::black_box;

#[global_allocator]
static ALLOC: PianoAllocator<System> = PianoAllocator::new(System);

#[test]
fn bias_impact() {
    // ---- Step 1: Trigger startup calibration ----
    time::calibrate();
    time::calibrate_bias();
    cpu_clock::calibrate_bias();

    // ---- Step 2: Read calibrated bias values ----
    let tsc_bias_ticks = time::bias_ticks();
    let tsc_bias_ns = time::ticks_to_ns(tsc_bias_ticks);
    let cpu_bias_ns = cpu_clock::bias_ns();

    println!("\n========================================");
    println!("  CALIBRATED BIAS VALUES");
    println!("========================================");
    println!("  TSC bias:      {} ticks = {} ns", tsc_bias_ticks, tsc_bias_ns);
    println!("  CPU-time bias: {} ns", cpu_bias_ns);
    println!();

    // ---- Step 3: Empty Guard wall time FROM THE MEASUREMENT ----
    // Create Guard, immediately drop. Guard pushes Measurement to buffer.
    // Drain buffer, read wall_time_ns() from the Measurement itself.
    // This is what the user sees in the profiling report.
    let wall_results = std::thread::spawn(|| {
        const N: usize = 10_000;
        let mut wall_times: Vec<u64> = Vec::with_capacity(N);

        for i in 0..N {
            let mut g = Guard::new_uninstrumented((i + 1) as u64, 0, 0, false);
            g.stamp();
            drop(black_box(g));

            let measurements = drain_thread_buffer();
            assert_eq!(measurements.len(), 1, "expected exactly 1 measurement");
            wall_times.push(measurements[0].end_ns.saturating_sub(measurements[0].start_ns));
        }

        let sum: u64 = wall_times.iter().sum();
        let mean = sum as f64 / N as f64;
        wall_times.sort_unstable();
        let median = wall_times[N / 2];
        let p99 = wall_times[N * 99 / 100];

        (mean, median, p99)
    })
    .join()
    .expect("wall time thread panicked");

    let (mean_raw_wall, median_wall, p99_wall) = wall_results;
    let tsc_bias_ns_f64 = tsc_bias_ns as f64;
    let corrected_wall = (mean_raw_wall - tsc_bias_ns_f64).max(0.0);

    println!("========================================");
    println!("  A. EMPTY GUARD -- WALL TIME (from Measurement)");
    println!("========================================");
    println!("  Iterations:            {}", 10_000);
    println!("  Raw mean:              {:.2} ns", mean_raw_wall);
    println!("  Raw median:            {} ns", median_wall);
    println!("  Raw p99:               {} ns", p99_wall);
    println!("  TSC bias:              {:.2} ns", tsc_bias_ns_f64);
    println!("  Corrected mean:        {:.2} ns", corrected_wall);
    if mean_raw_wall > 0.0 {
        println!("  Bias as pct of raw:    {:.1}%", (tsc_bias_ns_f64 / mean_raw_wall) * 100.0);
    }
    println!();

    // ---- Step 4: Empty Guard CPU time FROM THE MEASUREMENT ----
    let cpu_results = std::thread::spawn(|| {
        const N: usize = 10_000;
        let mut cpu_times: Vec<u64> = Vec::with_capacity(N);

        for i in 0..N {
            let mut g = Guard::new_uninstrumented((i + 1) as u64, 0, 0, true);
            g.stamp();
            drop(black_box(g));

            let measurements = drain_thread_buffer();
            assert_eq!(measurements.len(), 1, "expected exactly 1 measurement");
            cpu_times.push(measurements[0].cpu_end_ns.saturating_sub(measurements[0].cpu_start_ns));
        }

        let sum: u64 = cpu_times.iter().sum();
        let mean = sum as f64 / N as f64;
        cpu_times.sort_unstable();
        let median = cpu_times[N / 2];
        let p99 = cpu_times[N * 99 / 100];

        (mean, median, p99)
    })
    .join()
    .expect("cpu time thread panicked");

    let (mean_raw_cpu, median_cpu, p99_cpu) = cpu_results;
    let cpu_bias_ns_f64 = cpu_bias_ns as f64;
    let corrected_cpu = (mean_raw_cpu - cpu_bias_ns_f64).max(0.0);

    println!("========================================");
    println!("  B. EMPTY GUARD -- CPU TIME (from Measurement)");
    println!("========================================");
    println!("  Iterations:            {}", 10_000);
    println!("  Raw mean:              {:.2} ns", mean_raw_cpu);
    println!("  Raw median:            {} ns", median_cpu);
    println!("  Raw p99:               {} ns", p99_cpu);
    println!("  CPU bias:              {:.2} ns", cpu_bias_ns_f64);
    println!("  Corrected mean:        {:.2} ns", corrected_cpu);
    if mean_raw_cpu > 0.0 {
        println!("  Bias as pct of raw:    {:.1}%", (cpu_bias_ns_f64 / mean_raw_cpu) * 100.0);
    }
    println!();

    // ---- Step 5: Parent-child from Measurements ----
    // Create parent Guard, create+drop 100 child Guards inside it, drop parent.
    // Drain buffer: 101 measurements (100 children pushed first, parent last).
    // Compute parent self_time = parent.wall_time - sum(child.wall_time).
    // Then corrected self_time = self_time - 100 * guard_overhead.
    let parent_child_results = std::thread::spawn(|| {
        const N_CHILDREN: usize = 100;

        let mut parent = Guard::new_uninstrumented(1, 0, 0, true);
        parent.stamp();

        for i in 0..N_CHILDREN {
            let mut child = Guard::new_uninstrumented((i + 2) as u64, 1, 1, true);
            child.stamp();
            drop(black_box(child));
        }

        drop(black_box(parent));

        let measurements = drain_thread_buffer();
        assert_eq!(
            measurements.len(),
            N_CHILDREN + 1,
            "expected {} measurements (100 children + 1 parent)",
            N_CHILDREN + 1
        );

        // Children are pushed first (LIFO drop order for Guard),
        // parent is last. Find parent by span_id == 1.
        let parent_m = measurements.iter().find(|m| m.span_id == 1).unwrap();
        let parent_wall = parent_m.end_ns.saturating_sub(parent_m.start_ns);
        let parent_cpu = parent_m.cpu_end_ns.saturating_sub(parent_m.cpu_start_ns);

        let child_wall_sum: u64 = measurements
            .iter()
            .filter(|m| m.span_id != 1)
            .map(|m| m.end_ns.saturating_sub(m.start_ns))
            .sum();
        let child_cpu_sum: u64 = measurements
            .iter()
            .filter(|m| m.span_id != 1)
            .map(|m| m.cpu_end_ns.saturating_sub(m.cpu_start_ns))
            .sum();

        (parent_wall, parent_cpu, child_wall_sum, child_cpu_sum)
    })
    .join()
    .expect("parent-child thread panicked");

    let (parent_wall, parent_cpu, child_wall_sum, child_cpu_sum) = parent_child_results;

    // Wall time analysis
    let self_time_wall = parent_wall as f64 - child_wall_sum as f64;
    let corrected_child_wall = child_wall_sum as f64 - (100.0 * tsc_bias_ns_f64);
    let corrected_self_wall = parent_wall as f64 - corrected_child_wall;

    println!("========================================");
    println!("  C. PARENT-CHILD -- WALL TIME (from Measurements)");
    println!("========================================");
    println!("  Children:              100");
    println!("  Parent wall_time_ns:   {} ns", parent_wall);
    println!("  Sum child wall_time:   {} ns", child_wall_sum);
    println!("  Self time (raw):       {:.2} ns", self_time_wall);
    println!("  Corrected children:    {:.2} ns  (- 100 * {:.0} ns TSC bias)", corrected_child_wall, tsc_bias_ns_f64);
    println!("  Self time (corrected): {:.2} ns", corrected_self_wall);
    if self_time_wall.abs() > 0.1 {
        let pct = ((corrected_self_wall - self_time_wall) / self_time_wall) * 100.0;
        println!("  Self time change:      {:+.1}%", pct);
    }
    println!();

    // CPU time analysis
    let self_time_cpu = parent_cpu as f64 - child_cpu_sum as f64;
    let corrected_child_cpu = child_cpu_sum as f64 - (100.0 * cpu_bias_ns_f64);
    let corrected_self_cpu = parent_cpu as f64 - corrected_child_cpu;

    println!("========================================");
    println!("  C. PARENT-CHILD -- CPU TIME (from Measurements)");
    println!("========================================");
    println!("  Children:              100");
    println!("  Parent cpu_time_ns:    {} ns", parent_cpu);
    println!("  Sum child cpu_time:    {} ns", child_cpu_sum);
    println!("  Self time (raw):       {:.2} ns", self_time_cpu);
    println!("  Corrected children:    {:.2} ns  (- 100 * {:.0} ns CPU bias)", corrected_child_cpu, cpu_bias_ns_f64);
    println!("  Self time (corrected): {:.2} ns", corrected_self_cpu);
    if self_time_cpu.abs() > 0.1 {
        let pct = ((corrected_self_cpu - self_time_cpu) / self_time_cpu) * 100.0;
        println!("  Self time change:      {:+.1}%", pct);
    }
    println!();

    // ---- Summary ----
    println!("========================================");
    println!("  SUMMARY");
    println!("========================================");
    println!("  TSC bias:              {:.2} ns", tsc_bias_ns_f64);
    println!("  CPU bias:              {:.2} ns", cpu_bias_ns_f64);
    println!("  Empty guard wall (raw/corrected):  {:.2} / {:.2} ns", mean_raw_wall, corrected_wall);
    println!("  Empty guard CPU  (raw/corrected):  {:.2} / {:.2} ns", mean_raw_cpu, corrected_cpu);
    println!("  Parent self wall (raw/corrected):  {:.2} / {:.2} ns", self_time_wall, corrected_self_wall);
    println!("  Parent self CPU  (raw/corrected):  {:.2} / {:.2} ns", self_time_cpu, corrected_self_cpu);
    println!("========================================\n");
}
