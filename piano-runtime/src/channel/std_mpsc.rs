use crate::channel::{register_channel, ChannelKind, ChannelStats};
use std::sync::mpsc;

pub struct PianoSender<T> {
    inner: mpsc::Sender<T>,
    stats: &'static ChannelStats,
}

impl<T> Clone for PianoSender<T> {
    fn clone(&self) -> Self {
        PianoSender {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> PianoSender<T> {
    pub fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
        let result = self.inner.send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }
}

pub struct PianoSyncSender<T> {
    inner: mpsc::SyncSender<T>,
    stats: &'static ChannelStats,
}

impl<T> Clone for PianoSyncSender<T> {
    fn clone(&self) -> Self {
        PianoSyncSender {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> PianoSyncSender<T> {
    pub fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
        let result = self.inner.send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn try_send(&self, value: T) -> Result<(), mpsc::TrySendError<T>> {
        let result = self.inner.try_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }
}

pub struct PianoReceiver<T> {
    inner: mpsc::Receiver<T>,
    stats: &'static ChannelStats,
}

impl<T> PianoReceiver<T> {
    pub fn recv(&self) -> Result<T, mpsc::RecvError> {
        let result = self.inner.recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Result<T, mpsc::RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn iter(&self) -> PianoIter<'_, T> {
        PianoIter { rx: self }
    }

    pub fn try_iter(&self) -> PianoTryIter<'_, T> {
        PianoTryIter { rx: self }
    }
}

pub struct PianoIter<'a, T> {
    rx: &'a PianoReceiver<T>,
}

impl<T> Iterator for PianoIter<'_, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.rx.recv().ok()
    }
}

pub struct PianoTryIter<'a, T> {
    rx: &'a PianoReceiver<T>,
}

impl<T> Iterator for PianoTryIter<'_, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.rx.try_recv().ok()
    }
}

impl<T> IntoIterator for PianoReceiver<T> {
    type Item = T;
    type IntoIter = PianoIntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        PianoIntoIter {
            inner: self.inner.into_iter(),
            stats: self.stats,
        }
    }
}

pub struct PianoIntoIter<T> {
    inner: mpsc::IntoIter<T>,
    stats: &'static ChannelStats,
}

impl<T> Iterator for PianoIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let item = self.inner.next();
        if item.is_some() {
            self.stats.record_recv();
        }
        item
    }
}

pub fn wrap_channel<T>(
    channel: (mpsc::Sender<T>, mpsc::Receiver<T>),
    label: &'static str,
) -> (PianoSender<T>, PianoReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Unbounded, std::mem::size_of::<T>());
    let (tx, rx) = channel;
    (
        PianoSender { inner: tx, stats },
        PianoReceiver { inner: rx, stats },
    )
}

pub fn wrap_sync_channel<T>(
    channel: (mpsc::SyncSender<T>, mpsc::Receiver<T>),
    label: &'static str,
    cap: usize,
) -> (PianoSyncSender<T>, PianoReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Bounded(cap), std::mem::size_of::<T>());
    let (tx, rx) = channel;
    (
        PianoSyncSender { inner: tx, stats },
        PianoReceiver { inner: rx, stats },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{collect_channel_stats, ChannelKind};

    #[test]
    fn proxy_sender_counts_sends() {
        let (tx, rx) = wrap_channel(std::sync::mpsc::channel::<u32>(), "test:std_ch");
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let _ = rx.recv().unwrap();

        let stats = collect_channel_stats();
        let ch = stats.iter().find(|s| s.label == "test:std_ch").unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.max_queued, 2);
        assert_eq!(ch.kind, ChannelKind::Unbounded);
    }

    #[test]
    fn proxy_sync_sender_counts_sends() {
        let (tx, rx) =
            wrap_sync_channel(std::sync::mpsc::sync_channel::<u32>(2), "test:std_sync", 2);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let _ = rx.recv().unwrap();

        let stats = collect_channel_stats();
        let ch = stats.iter().find(|s| s.label == "test:std_sync").unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.kind, ChannelKind::Bounded(2));
    }

    #[test]
    fn into_iter_counts_recv() {
        let (tx, rx) = wrap_channel(std::sync::mpsc::channel::<u32>(), "test:std_into_iter");
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        tx.send(30).unwrap();
        drop(tx);

        let collected: Vec<u32> = rx.into_iter().collect();
        assert_eq!(collected, vec![10, 20, 30]);

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:std_into_iter")
            .unwrap();
        assert_eq!(ch.received, 3);
    }

    #[test]
    fn proxy_sender_is_clone() {
        let (tx, rx) = wrap_channel(std::sync::mpsc::channel::<u32>(), "test:clone");
        let tx2 = tx.clone();
        tx.send(1).unwrap();
        tx2.send(2).unwrap();
        let _ = rx.recv().unwrap();
        let _ = rx.recv().unwrap();

        let stats = collect_channel_stats();
        let ch = stats.iter().find(|s| s.label == "test:clone").unwrap();
        assert_eq!(ch.sent, 2);
    }
}
