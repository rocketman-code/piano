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

// Tests are in piano-runtime/tests/channel_crossbeam.rs (enforced by no_test_backdoors gate).
