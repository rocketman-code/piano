use crate::channel::{register_channel, ChannelKind, ChannelStats};
use crossbeam_channel as cb;

pub struct PianoSender<T> {
    inner: cb::Sender<T>,
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
    pub fn send(&self, value: T) -> Result<(), cb::SendError<T>> {
        let result = self.inner.send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn try_send(&self, value: T) -> Result<(), cb::TrySendError<T>> {
        let result = self.inner.try_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn send_timeout(
        &self,
        value: T,
        timeout: std::time::Duration,
    ) -> Result<(), cb::SendTimeoutError<T>> {
        let result = self.inner.send_timeout(value, timeout);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }
}

pub struct PianoReceiver<T> {
    inner: cb::Receiver<T>,
    stats: &'static ChannelStats,
}

impl<T> Clone for PianoReceiver<T> {
    fn clone(&self) -> Self {
        PianoReceiver {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> PianoReceiver<T> {
    pub fn recv(&self) -> Result<T, cb::RecvError> {
        let result = self.inner.recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn try_recv(&self) -> Result<T, cb::TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Result<T, cb::RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn recv_deadline(&self, deadline: std::time::Instant) -> Result<T, cb::RecvTimeoutError> {
        let result = self.inner.recv_deadline(deadline);
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
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
    inner: cb::IntoIter<T>,
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

pub fn wrap_bounded<T>(
    channel: (cb::Sender<T>, cb::Receiver<T>),
    label: &'static str,
    cap: usize,
) -> (PianoSender<T>, PianoReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Bounded(cap), std::mem::size_of::<T>());
    let (tx, rx) = channel;
    (
        PianoSender { inner: tx, stats },
        PianoReceiver { inner: rx, stats },
    )
}

pub fn wrap_unbounded<T>(
    channel: (cb::Sender<T>, cb::Receiver<T>),
    label: &'static str,
) -> (PianoSender<T>, PianoReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Unbounded, std::mem::size_of::<T>());
    let (tx, rx) = channel;
    (
        PianoSender { inner: tx, stats },
        PianoReceiver { inner: rx, stats },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{collect_channel_stats, ChannelKind};

    #[test]
    fn proxy_crossbeam_bounded_counts() {
        let (tx, rx) = wrap_bounded(crossbeam_channel::bounded::<u32>(5), "test:cb_bounded", 5);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let _ = rx.recv().unwrap();

        let stats = collect_channel_stats();
        let ch = stats.iter().find(|s| s.label == "test:cb_bounded").unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.max_queued, 2);
        assert_eq!(ch.kind, ChannelKind::Bounded(5));
    }

    #[test]
    fn proxy_crossbeam_unbounded_counts() {
        let (tx, rx) = wrap_unbounded(crossbeam_channel::unbounded::<u32>(), "test:cb_unbounded");
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let _ = rx.recv().unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:cb_unbounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.kind, ChannelKind::Unbounded);
    }

    #[test]
    fn into_iter_counts_recv() {
        let (tx, rx) = wrap_unbounded(crossbeam_channel::unbounded::<u32>(), "test:cb_into_iter");
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        tx.send(30).unwrap();
        drop(tx);

        let collected: Vec<u32> = rx.into_iter().collect();
        assert_eq!(collected, vec![10, 20, 30]);

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:cb_into_iter")
            .unwrap();
        assert_eq!(ch.received, 3);
    }
}
