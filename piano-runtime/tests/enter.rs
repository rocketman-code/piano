use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::guard::enter;
use piano_runtime::session::ProfileSession;

#[test]
fn enter_inactive_is_noop() {
    std::thread::spawn(|| {
        let _g = enter(0);
    })
    .join()
    .unwrap();
}

#[test]
fn enter_produces_aggregate() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].calls, 1);
    })
    .join()
    .unwrap();
}

#[test]
fn nested_enter_computes_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g1 = enter(0);
            {
                let _g2 = enter(1);
            }
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 2);
    })
    .join()
    .unwrap();
}

#[test]
fn closure_works_without_capture() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        let handle = std::thread::spawn(|| {
            let _g = enter(0);
        });
        handle.join().unwrap();
        drain_thread_agg();
    })
    .join()
    .unwrap();
}

#[test]
fn spawn_inside_map_works() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    let _g = enter(0);
                    std::hint::black_box(42)
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        drain_thread_agg();
    })
    .join()
    .unwrap();
}
