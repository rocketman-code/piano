//! Crossbeam channel proxy types with transparent `select!` compatibility.
//!
//! `ProxyReceiver` and `ProxySender` wrap crossbeam's `Receiver<T>` and
//! `Sender<T>` respectively, implementing `Deref` to the inner type so they
//! can be used directly in `crossbeam_channel::select!` macros. Inherent
//! methods shadow the `Deref`'d methods for direct calls, automatically
//! recording send/recv counts in the channel's `ChannelStats`.

use std::ops::Deref;
use std::time::{Duration, Instant};

use crossbeam_channel::{
    Receiver, RecvError, RecvTimeoutError, SendError, SendTimeoutError, Sender, TryRecvError,
    TrySendError,
};

use super::{register_channel, ChannelKind, ChannelStats};

// ---------------------------------------------------------------------------
// ProxyReceiver
// ---------------------------------------------------------------------------

/// A profiling proxy around crossbeam's `Receiver<T>`.
///
/// Implements `Deref<Target = Receiver<T>>` so it can be passed directly to
/// `crossbeam_channel::select!`. Inherent methods shadow the deref'd methods
/// for direct calls (e.g. `rx.recv()`), automatically recording receive counts.
pub struct ProxyReceiver<T> {
    inner: Receiver<T>,
    #[doc(hidden)]
    pub stats: &'static ChannelStats,
}

impl<T> Deref for ProxyReceiver<T> {
    type Target = Receiver<T>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Clone for ProxyReceiver<T> {
    fn clone(&self) -> Self {
        ProxyReceiver {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> ProxyReceiver<T> {
    /// Return a blocking iterator that records each receive.
    pub fn iter(&self) -> ProxyIter<'_, T> {
        ProxyIter { rx: self }
    }

    /// Return a non-blocking iterator that records each receive.
    pub fn try_iter(&self) -> ProxyTryIter<'_, T> {
        ProxyTryIter { rx: self }
    }

    /// Receive a message, recording the receive on success.
    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        let result = self.inner.recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    /// Try to receive a message without blocking, recording on success.
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    /// Receive with a timeout, recording on success.
    #[inline(always)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }

    /// Receive with a deadline, recording on success.
    #[inline(always)]
    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        let result = self.inner.recv_deadline(deadline);
        if result.is_ok() {
            self.stats.record_recv();
        }
        result
    }
}

// ---------------------------------------------------------------------------
// ProxySender
// ---------------------------------------------------------------------------

/// A profiling proxy around crossbeam's `Sender<T>`.
///
/// Implements `Deref<Target = Sender<T>>` so it can be passed directly to
/// `crossbeam_channel::select!`. Inherent methods shadow the deref'd methods
/// for direct calls (e.g. `tx.send(msg)`), automatically recording send counts.
pub struct ProxySender<T> {
    inner: Sender<T>,
    #[doc(hidden)]
    pub stats: &'static ChannelStats,
}

impl<T> Deref for ProxySender<T> {
    type Target = Sender<T>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Clone for ProxySender<T> {
    fn clone(&self) -> Self {
        ProxySender {
            inner: self.inner.clone(),
            stats: self.stats,
        }
    }
}

impl<T> ProxySender<T> {
    /// Send a message, recording the send on success.
    #[inline(always)]
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let result = self.inner.send(msg);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    /// Try to send a message without blocking, recording on success.
    #[inline(always)]
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        let result = self.inner.try_send(msg);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    /// Send with a timeout, recording on success.
    #[inline(always)]
    pub fn send_timeout(&self, msg: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        let result = self.inner.send_timeout(msg, timeout);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }

    /// Send with a deadline, recording on success.
    #[inline(always)]
    pub fn send_deadline(&self, msg: T, deadline: Instant) -> Result<(), SendTimeoutError<T>> {
        let result = self.inner.send_deadline(msg, deadline);
        if result.is_ok() {
            self.stats.record_send();
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Wrap functions
// ---------------------------------------------------------------------------

/// Create a bounded crossbeam channel wrapped in profiling proxies.
///
/// The `label` identifies this channel in profiling output. Registers the
/// channel in the global registry with `ChannelKind::Bounded(cap)`.
pub fn wrap_bounded<T>(cap: usize, label: &'static str) -> (ProxySender<T>, ProxyReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Bounded(cap), std::mem::size_of::<T>());
    let (tx, rx) = crossbeam_channel::bounded(cap);
    (
        ProxySender { inner: tx, stats },
        ProxyReceiver { inner: rx, stats },
    )
}

/// Create an unbounded crossbeam channel wrapped in profiling proxies.
///
/// The `label` identifies this channel in profiling output. Registers the
/// channel in the global registry with `ChannelKind::Unbounded`.
pub fn wrap_unbounded<T>(label: &'static str) -> (ProxySender<T>, ProxyReceiver<T>) {
    let stats = register_channel(label, ChannelKind::Unbounded, std::mem::size_of::<T>());
    let (tx, rx) = crossbeam_channel::unbounded();
    (
        ProxySender { inner: tx, stats },
        ProxyReceiver { inner: rx, stats },
    )
}

// ---------------------------------------------------------------------------
// Iterators
// ---------------------------------------------------------------------------

/// Blocking iterator over a `ProxyReceiver` that records each receive.
pub struct ProxyIter<'a, T> {
    rx: &'a ProxyReceiver<T>,
}

impl<T> Iterator for ProxyIter<'_, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let result = self.rx.inner.recv().ok();
        if result.is_some() {
            self.rx.stats.record_recv();
        }
        result
    }
}

impl<T> std::iter::FusedIterator for ProxyIter<'_, T> {}

/// Non-blocking iterator over a `ProxyReceiver` that records each receive.
pub struct ProxyTryIter<'a, T> {
    rx: &'a ProxyReceiver<T>,
}

impl<T> Iterator for ProxyTryIter<'_, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let result = self.rx.inner.try_recv().ok();
        if result.is_some() {
            self.rx.stats.record_recv();
        }
        result
    }
}

/// Owning iterator over a `ProxyReceiver` that records each receive.
pub struct ProxyIntoIter<T> {
    inner: crossbeam_channel::IntoIter<T>,
    stats: &'static ChannelStats,
}

impl<T> Iterator for ProxyIntoIter<T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let result = self.inner.next();
        if result.is_some() {
            self.stats.record_recv();
        }
        result
    }
}

impl<T> std::iter::FusedIterator for ProxyIntoIter<T> {}

impl<'a, T> IntoIterator for &'a ProxyReceiver<T> {
    type Item = T;
    type IntoIter = ProxyIter<'a, T>;

    fn into_iter(self) -> ProxyIter<'a, T> {
        self.iter()
    }
}

impl<T> IntoIterator for ProxyReceiver<T> {
    type Item = T;
    type IntoIter = ProxyIntoIter<T>;

    fn into_iter(self) -> ProxyIntoIter<T> {
        ProxyIntoIter {
            stats: self.stats,
            inner: self.inner.into_iter(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Short timeout for recv operations in tests. When a send mutation
    // replaces the method body with Ok(()), the channel stays empty and
    // recv_timeout fails fast instead of blocking the test runner for 120s.
    const TEST_TIMEOUT: Duration = Duration::from_millis(500);

    #[test]
    fn proxy_recv_counts_via_inherent_method() {
        let (tx, rx) = wrap_bounded::<i32>(16, "proxy_recv_test");
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        rx.recv_timeout(TEST_TIMEOUT).unwrap();

        let snap = rx.stats.snapshot();
        assert_eq!(snap.sent, 2);
        assert_eq!(snap.received, 1);
    }

    #[test]
    fn proxy_works_with_select_macro() {
        let (tx1, rx1) = wrap_bounded::<i32>(16, "select_ch1");
        let (_tx2, rx2) = wrap_bounded::<i32>(16, "select_ch2");

        tx1.send(42).unwrap();

        // select! uses Deref to access the inner Receiver, bypassing our
        // inherent recv(). The arm body injects the stats recording manually
        // -- this is the pattern Task 4 (rewriter) will generate.
        crossbeam_channel::select! {
            recv(rx1) -> msg => {
                rx1.stats.record_recv_if_ok(&msg);
                assert_eq!(msg.unwrap(), 42);
            }
            recv(rx2) -> msg => {
                rx2.stats.record_recv_if_ok(&msg);
                let _ = msg;
            }
            default(TEST_TIMEOUT) => panic!("select recv timed out"),
        }

        let snap1 = rx1.stats.snapshot();
        assert_eq!(snap1.received, 1);
    }

    #[test]
    fn proxy_clone_shares_stats() {
        let (tx, rx) = wrap_bounded::<i32>(16, "clone_test");
        let tx2 = tx.clone();

        tx.send(1).unwrap();
        tx2.send(2).unwrap();
        rx.recv_timeout(TEST_TIMEOUT).unwrap();

        let rx2 = rx.clone();
        rx2.recv_timeout(TEST_TIMEOUT).unwrap();

        let snap = tx.stats.snapshot();
        assert_eq!(snap.sent, 2, "both senders share the same stats");
        assert_eq!(snap.received, 2, "both receivers share the same stats");
    }

    #[test]
    fn proxy_unbounded_works() {
        let (tx, rx) = wrap_unbounded::<String>("unbounded_test");
        tx.send("hello".into()).unwrap();
        let msg = rx.recv_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(msg, "hello");

        let snap = rx.stats.snapshot();
        assert_eq!(snap.sent, 1);
        assert_eq!(snap.received, 1);
        assert_eq!(snap.kind, ChannelKind::Unbounded);
    }

    #[test]
    fn proxy_try_recv_counts() {
        let (tx, rx) = wrap_bounded::<i32>(16, "try_recv_test");
        tx.send(99).unwrap();

        // Successful try_recv should count
        let val = rx.try_recv().unwrap();
        assert_eq!(val, 99);

        // Failed try_recv on empty channel should NOT count
        let _ = rx.try_recv();

        let snap = rx.stats.snapshot();
        assert_eq!(snap.received, 1, "only successful try_recv should count");
    }

    #[test]
    fn deref_exposes_inner_for_passthrough() {
        let (tx, rx) = wrap_bounded::<i32>(16, "deref_test");

        // These methods come from Deref to crossbeam's Receiver/Sender
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);

        tx.send(1).unwrap();
        assert_eq!(rx.len(), 1);
        assert!(!rx.is_empty());

        // capacity() is on the inner Receiver via Deref
        assert_eq!(rx.capacity(), Some(16));
    }

    #[test]
    fn select_with_send_arm() {
        let (tx, rx) = wrap_bounded::<i32>(1, "select_send_test");

        crossbeam_channel::select! {
            send(tx, 42) -> res => {
                tx.stats.record_send_if_ok(&res);
                res.unwrap();
            }
        }

        let snap = tx.stats.snapshot();
        assert_eq!(snap.sent, 1);

        let val = rx.recv_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn proxy_iter_counts_each_recv() {
        let (tx, rx) = wrap_bounded::<i32>(10, "test:cb_iter");
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        drop(tx); // close channel so iter terminates

        let collected: Vec<i32> = rx.iter().collect();
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);

        let snap = rx.stats.snapshot();
        assert_eq!(snap.received, 5);
    }

    #[test]
    fn proxy_try_iter_counts_each_recv() {
        let (tx, rx) = wrap_bounded::<i32>(10, "test:cb_try_iter");
        for i in 0..3 {
            tx.send(i).unwrap();
        }

        let collected: Vec<i32> = rx.try_iter().collect();
        assert_eq!(collected, vec![0, 1, 2]);

        let snap = rx.stats.snapshot();
        assert_eq!(snap.received, 3);
    }

    #[test]
    fn proxy_into_iter_counts_each_recv() {
        let (tx, rx) = wrap_bounded::<i32>(10, "test:cb_into_iter");
        for i in 0..4 {
            tx.send(i).unwrap();
        }
        drop(tx);

        let stats = rx.stats;
        let collected: Vec<i32> = rx.into_iter().collect();
        assert_eq!(collected, vec![0, 1, 2, 3]);

        let snap = stats.snapshot();
        assert_eq!(snap.received, 4);
    }

    #[test]
    fn proxy_try_send_delivers_message() {
        let (tx, rx) = wrap_bounded::<i32>(16, "try_send_test");
        tx.try_send(42).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 42);
    }

    #[test]
    fn proxy_send_timeout_delivers_message() {
        let (tx, rx) = wrap_bounded::<i32>(16, "send_timeout_test");
        tx.send_timeout(7, Duration::from_secs(1)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 7);
    }

    #[test]
    fn proxy_send_deadline_delivers_message() {
        let (tx, rx) = wrap_bounded::<i32>(16, "send_deadline_test");
        tx.send_deadline(99, Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(rx.try_recv().unwrap(), 99);
    }

    #[test]
    fn mixed_direct_and_select_full_coverage() {
        let (tx, rx) = wrap_bounded::<i32>(16, "mixed_test");

        // 3 direct sends
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();

        // 2 direct recvs
        rx.recv_timeout(TEST_TIMEOUT).unwrap();
        rx.recv_timeout(TEST_TIMEOUT).unwrap();

        // 1 select! recv with arm body injection
        crossbeam_channel::select! {
            recv(rx) -> msg => {
                rx.stats.record_recv_if_ok(&msg);
                assert_eq!(msg.unwrap(), 3);
            }
            default(TEST_TIMEOUT) => panic!("select recv timed out"),
        }

        let snap = rx.stats.snapshot();
        assert_eq!(snap.sent, 3);
        assert_eq!(snap.received, 3);
    }
}
