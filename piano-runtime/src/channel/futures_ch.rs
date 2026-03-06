use crate::channel::{register_channel, ChannelKind, ChannelStats};
use futures_channel::mpsc;

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
    pub fn try_send(&mut self, value: T) -> Result<(), mpsc::TrySendError<T>> {
        let result = self.inner.try_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn start_send(&mut self, value: T) -> Result<(), mpsc::SendError> {
        let result = self.inner.start_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn close_channel(&mut self) {
        self.inner.close_channel()
    }

    pub fn disconnect(&mut self) {
        self.inner.disconnect()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

pub struct PianoReceiver<T> {
    inner: mpsc::Receiver<T>,
    stats: &'static ChannelStats,
}

impl<T> PianoReceiver<T> {
    pub fn try_next(&mut self) -> Result<Option<T>, mpsc::TryRecvError> {
        let result = self.inner.try_next();
        if let Ok(Some(_)) = &result {
            self.stats.record_recv();
        }
        result
    }

    pub fn close(&mut self) {
        self.inner.close()
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
    pub fn unbounded_send(&self, value: T) -> Result<(), mpsc::TrySendError<T>> {
        let result = self.inner.unbounded_send(value);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    pub fn close_channel(&self) {
        self.inner.close_channel()
    }

    pub fn disconnect(&mut self) {
        self.inner.disconnect()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

pub struct PianoUnboundedReceiver<T> {
    inner: mpsc::UnboundedReceiver<T>,
    stats: &'static ChannelStats,
}

impl<T> PianoUnboundedReceiver<T> {
    pub fn try_next(&mut self) -> Result<Option<T>, mpsc::TryRecvError> {
        let result = self.inner.try_next();
        if let Ok(Some(_)) = &result {
            self.stats.record_recv();
        }
        result
    }

    pub fn close(&mut self) {
        self.inner.close()
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

    #[test]
    fn proxy_futures_bounded_counts() {
        let (mut tx, mut rx) = wrap_bounded(
            futures_channel::mpsc::channel::<u32>(8),
            "test:futures_bounded",
            8,
        );
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        let _ = rx.try_next().unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:futures_bounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.max_queued, 2);
        assert_eq!(ch.kind, ChannelKind::Bounded(8));
    }

    #[test]
    fn proxy_futures_unbounded_counts() {
        let (tx, mut rx) = wrap_unbounded(
            futures_channel::mpsc::unbounded::<u32>(),
            "test:futures_unbounded",
        );
        tx.unbounded_send(1).unwrap();
        tx.unbounded_send(2).unwrap();
        let _ = rx.try_next().unwrap();

        let stats = collect_channel_stats();
        let ch = stats
            .iter()
            .find(|s| s.label == "test:futures_unbounded")
            .unwrap();
        assert_eq!(ch.sent, 2);
        assert_eq!(ch.received, 1);
        assert_eq!(ch.kind, ChannelKind::Unbounded);
    }
}
