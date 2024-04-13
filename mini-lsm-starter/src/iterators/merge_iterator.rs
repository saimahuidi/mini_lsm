#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters_heap = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                iters_heap.push(HeapWrapper(index, iter));
            }
        }
        let mut current = None;
        if !iters_heap.is_empty() {
            current = iters_heap.pop();
        }
        Self {
            iters: iters_heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match &self.current {
            Some(wrapper) => wrapper.1.key(),
            None => KeySlice::default(),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(wrapper) => wrapper.1.value(),
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let key_old = self.key().to_key_vec();
        let iter_current = self.current.take();
        if let Some(mut iter) = iter_current {
            iter.1.next()?;
            if iter.1.is_valid() {
                self.iters.push(iter);
            }
        }
        while let Some(mut iter_new) = self.iters.peek_mut() {
            let result_cmp = iter_new.1.key().to_key_vec().cmp(&key_old);
            if result_cmp == cmp::Ordering::Greater {
                self.current = Some(PeekMut::pop(iter_new));
            } else {
                assert_eq!(result_cmp, cmp::Ordering::Equal);
                let result = iter_new.1.next();
                if let e @ Err(_) = result {
                    PeekMut::pop(iter_new);
                    return e;
                }
                if !iter_new.1.is_valid() {
                    PeekMut::pop(iter_new);
                }
            }
            if self.current.is_some() {
                break;
            }
        }
        Ok(())
    }
}
