use crate::channel::{register_channel, ChannelKind, ChannelStats};
use async_channel as ac;

pub struct PianoSender<T> {
    inner: ac::Sender<T>,
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
    pub async fn send(&self, value: T) -> Result<(), ac::SendError<T>> {
        let result = self.inner.send(value).await;
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn try_send(&self, value: T) -> Result<(), ac::TrySendError<T>> {
        let result = self.inner.try_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn close(&self) -> bool {
        self.inner.close()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
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
    inner: ac::Receiver<T>,
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
    pub async fn recv(&self) -> Result<T, ac::RecvError> {
        let result = self.inner.recv().await;
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn try_recv(&self) -> Result<T, ac::TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn close(&self) -> bool {
        self.inner.close()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
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

pub fn wrap_bounded<T>(
    channel: (ac::Sender<T>, ac::Receiver<T>),
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
    channel: (ac::Sender<T>, ac::Receiver<T>),
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

    #[tokio::test]
    async fn proxy_async_bounded_counts() {
        let (tx, rx) = wrap_bounded(async_channel::bounded::<u32>(8), "test:async_bounded", 8);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        let _ = rx.recv().await.unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:async_bounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.max_queued, 2);
        assert_eq!(ch.kind, ChannelKind::Bounded(8));
    }

    #[tokio::test]
    async fn proxy_async_unbounded_counts() {
        let (tx, rx) = wrap_unbounded(async_channel::unbounded::<u32>(), "test:async_unbounded");
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        let _ = rx.recv().await.unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:async_unbounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.kind, ChannelKind::Unbounded);
    }
}
