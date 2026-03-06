#[cfg(feature = "channels-async")]
pub mod async_ch;
#[cfg(feature = "channels-crossbeam")]
pub mod crossbeam;
#[cfg(feature = "channels-futures")]
pub mod futures_ch;
pub mod std_mpsc;
#[cfg(feature = "channels-tokio")]
pub mod tokio_mpsc;

use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

use crate::sync_util::SyncOnceCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Bounded(usize),
    Unbounded,
}

impl core::fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ChannelKind::Bounded(cap) => write!(f, "bounded({cap})"),
            ChannelKind::Unbounded => write!(f, "unbounded"),
        }
    }
}

pub struct ChannelStats {
    pub label: &'static str,
    pub kind: ChannelKind,
    pub type_size: usize,
    sent: AtomicU64,
    received: AtomicU64,
    max_queued: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct ChannelSnapshot {
    pub label: &'static str,
    pub kind: ChannelKind,
    pub type_size: usize,
    pub sent: u64,
    pub received: u64,
    pub max_queued: u64,
}

impl ChannelStats {
    /// Record a send operation on this channel.
    ///
    /// `max_queued` is a best-effort upper bound: there is a benign TOCTOU race
    /// between the `fetch_add` on `sent` and the `load` on `received`, so the
    /// computed queue depth may momentarily exceed the true value when sends and
    /// receives overlap. This is acceptable for profiling purposes.
    pub fn record_send(&self) {
        let sent = self.sent.fetch_add(1, Relaxed) + 1;
        let recv = self.received.load(Relaxed);
        let queued = sent.saturating_sub(recv);
        self.max_queued.fetch_max(queued, Relaxed);
    }

    pub fn record_recv(&self) {
        self.received.fetch_add(1, Relaxed);
    }

    pub fn snapshot(&self) -> ChannelSnapshot {
        ChannelSnapshot {
            label: self.label,
            kind: self.kind,
            type_size: self.type_size,
            sent: self.sent.load(Relaxed),
            received: self.received.load(Relaxed),
            max_queued: self.max_queued.load(Relaxed),
        }
    }
}

static CHANNEL_REGISTRY: SyncOnceCell<Mutex<Vec<&'static ChannelStats>>> = SyncOnceCell::new();

fn channel_registry() -> &'static Mutex<Vec<&'static ChannelStats>> {
    CHANNEL_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

pub fn register_channel(
    label: &'static str,
    kind: ChannelKind,
    type_size: usize,
) -> &'static ChannelStats {
    let stats = Box::leak(Box::new(ChannelStats {
        label,
        kind,
        type_size,
        sent: AtomicU64::new(0),
        received: AtomicU64::new(0),
        max_queued: AtomicU64::new(0),
    }));
    channel_registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(stats);
    stats
}

pub fn collect_channel_stats() -> Vec<ChannelSnapshot> {
    channel_registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|s| s.snapshot())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_collect_channel_stats() {
        let stats = register_channel("test:1", ChannelKind::Bounded(10), 8);
        stats.record_send();
        stats.record_send();
        stats.record_recv();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.sent, 2);
        assert_eq!(snapshot.received, 1);
        assert_eq!(snapshot.max_queued, 2);
        assert_eq!(snapshot.type_size, 8);

        let all = collect_channel_stats();
        assert!(all.iter().any(|s| s.label == "test:1"));
    }

    #[test]
    fn max_queued_tracks_high_water_mark() {
        let stats = register_channel("test:hwm", ChannelKind::Unbounded, 16);
        for _ in 0..5 {
            stats.record_send();
        }
        for _ in 0..2 {
            stats.record_recv();
        }
        for _ in 0..1 {
            stats.record_send();
        }
        for _ in 0..3 {
            stats.record_recv();
        }

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.sent, 6);
        assert_eq!(snapshot.received, 5);
        assert_eq!(snapshot.max_queued, 5);
    }
}
