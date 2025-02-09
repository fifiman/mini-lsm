// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::iter;

use anyhow::Result;
use nom::AndThen;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
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
        // Remove all invalid iterators
        let mut iters: BinaryHeap<HeapWrapper<I>> = BinaryHeap::from_iter(
            iters
                .into_iter()
                .filter(|iter| iter.is_valid())
                .enumerate()
                .map(|(i, iter)| HeapWrapper(i, iter)),
        );

        // Handle no iterators or all invalid iterators
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let current = iters.pop();

        return Self { iters, current };
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // Remove all iterators that have the same key as the current iterator,
        // which has the highest priority for this key.

        let current = self.current.as_mut().unwrap();

        while let Some(mut next_iter) = self.iters.peek_mut() {
            // Skip
            if current.1.key() == next_iter.1.key() {
                if let e @ Err(_) = next_iter.1.next() {
                    PeekMut::pop(next_iter);
                    return e;
                }

                if !next_iter.1.is_valid() {
                    PeekMut::pop(next_iter);
                }
            }
            // Break, might swap later
            else {
                break;
            }
        }

        current.1.next()?;

        if (current.1.is_valid()) {
            // Might still be the top
            if let Some(mut topHeapIter) = self.iters.peek_mut() {
                if *current < *topHeapIter {
                    std::mem::swap(&mut *topHeapIter, current);
                }
            }
        } else {
            self.current = self.iters.pop();
        }
        Ok(())

        // What if heap is empty and we didnt execue anything in while
    }
}
