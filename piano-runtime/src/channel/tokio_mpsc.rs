use crate::channel::{register_channel, ChannelKind, ChannelStats};
use tokio::sync::mpsc;

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
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        let result = self.inner.send(value).await;
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        let result = self.inner.try_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn blocking_send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        let result = self.inner.blocking_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn max_capacity(&self) -> usize {
        self.inner.max_capacity()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn closed(&self) {
        self.inner.closed().await
    }
}

pub struct PianoReceiver<T> {
    inner: mpsc::Receiver<T>,
    stats: &'static ChannelStats,
}

impl<T> PianoReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let result = self.inner.recv().await;
        if result.is_some() {
            self.stats.record_recv();
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn blocking_recv(&mut self) -> Option<T> {
        let result = self.inner.blocking_recv();
        if result.is_some() {
            self.stats.record_recv();
        }
        result
    }

    pub fn close(&mut self) {
        self.inner.close()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

pub struct PianoUnboundedSender<T> {
    inner: mpsc::UnboundedSender<T>,
    stats: &'static ChannelStats,
}

impl<T> Clone for PianoUnboundedSender<T> {
    fn clone(&self) -> Self {
        PianoUnboundedSender {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> PianoUnboundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        let result = self.inner.send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn closed(&self) {
        self.inner.closed().await
    }
}

pub struct PianoUnboundedReceiver<T> {
    inner: mpsc::UnboundedReceiver<T>,
    stats: &'static ChannelStats,
}

impl<T> PianoUnboundedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let result = self.inner.recv().await;
        if result.is_some() {
            self.stats.record_recv();
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    pub fn blocking_recv(&mut self) -> Option<T> {
        let result = self.inner.blocking_recv();
        if result.is_some() {
            self.stats.record_recv();
        }
        result
    }

    pub fn close(&mut self) {
        self.inner.close()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

pub fn wrap_bounded<T>(
    channel: (mpsc::Sender<T>, mpsc::Receiver<T>),
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
    channel: (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>),
    label: &'static str,
) -> (PianoUnboundedSender<T>, PianoUnboundedReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Unbounded, std::mem::size_of::<T>());
    let (tx, rx) = channel;
    (
        PianoUnboundedSender { inner: tx, stats },
        PianoUnboundedReceiver { inner: rx, stats },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{collect_channel_stats, ChannelKind};

    #[tokio::test]
    async fn proxy_tokio_bounded_counts() {
        let (tx, mut rx) = wrap_bounded(
            tokio::sync::mpsc::channel::<u32>(10),
            "test:tokio_bounded",
            10,
        );
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        let _ = rx.recv().await.unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:tokio_bounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.max_queued, 2);
        assert_eq!(ch.kind, ChannelKind::Bounded(10));
    }

    #[tokio::test]
    async fn proxy_tokio_unbounded_counts() {
        let (tx, mut rx) = wrap_unbounded(
            tokio::sync::mpsc::unbounded_channel::<u32>(),
            "test:tokio_unbounded",
        );
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let _ = rx.recv().await.unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:tokio_unbounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.kind, ChannelKind::Unbounded);
    }
}
