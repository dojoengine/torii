use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_util::{Stream, StreamExt};
use once_cell::sync::Lazy;
use slab::Slab;

use crate::types::Update;

static SUBSCRIBERS: Lazy<DashMap<TypeId, Box<dyn Any + Send + Sync>>> = Lazy::new(Default::default);

#[derive(Debug)]
pub struct Senders<U: std::fmt::Debug + Clone + Send + Sync + 'static>(
    pub Slab<UnboundedSender<U>>,
);

struct BrokerStream<U: std::fmt::Debug + Clone + Send + Sync + 'static>(
    usize,
    UnboundedReceiver<U>,
);

fn with_senders<U, F, R>(f: F) -> R
where
    U: std::fmt::Debug + Clone + Send + Sync + 'static,
    F: FnOnce(&mut Senders<U>) -> R,
{
    let mut senders = SUBSCRIBERS
        .entry(TypeId::of::<Senders<U>>())
        .or_insert_with(|| Box::new(Senders::<U>(Default::default())));
    f(senders.downcast_mut::<Senders<U>>().unwrap())
}

impl<U: std::fmt::Debug + Clone + Send + Sync + 'static> Drop for BrokerStream<U> {
    fn drop(&mut self) {
        with_senders::<U, _, _>(|senders| senders.0.remove(self.0));
    }
}

impl<U: std::fmt::Debug + Clone + Send + Sync + 'static> Stream for BrokerStream<U> {
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.1.poll_next_unpin(cx)
    }
}

#[derive(Debug)]
/// A simple broker based on memory that works with Update<T> types
pub struct MemoryBroker<U>(PhantomData<U>)
where
    U: std::fmt::Debug + Clone + Send + Sync + 'static;

impl<T> MemoryBroker<Update<T>>
where
    T: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    /// Publish an update message that all subscription streams can receive.
    pub fn publish(msg: Update<T>) {
        with_senders::<Update<T>, _, _>(|senders| {
            for (_, sender) in senders.0.iter_mut() {
                sender.start_send(msg.clone()).ok();
            }
        });
    }

    /// Subscribe to all updates, regardless if they're optimistic or not.
    pub fn subscribe_all() -> impl Stream<Item = Update<T>> {
        with_senders::<Update<T>, _, _>(|senders| {
            let (tx, rx) = mpsc::unbounded();
            let id = senders.0.insert(tx);
            BrokerStream(id, rx)
        })
    }

    /// Subscribe to non-optimistic update messages
    pub fn subscribe() -> impl Stream<Item = T> {
        Self::subscribe_all()
            .filter_map(|u| {
                futures_util::future::ready(if u.is_optimistic() {
                    None
                } else {
                    Some(u.into_inner())
                })
            })
    }

    /// Subscribe to only optimistic update messages and returns a `Stream`.
    pub fn subscribe_optimistic() -> impl Stream<Item = T> {
        Self::subscribe_all()
            .filter_map(|u| {
                futures_util::future::ready(if u.is_optimistic() {
                    Some(u.into_inner())
                } else {
                    None
                })
            })
    }

    /// Execute the given function with the _subscribers_ of the specified subscription type.
    pub fn with_subscribers<F, R>(f: F) -> R
    where
        F: FnOnce(&mut Senders<Update<T>>) -> R,
    {
        with_senders(f)
    }
}
