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

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    pub fn get_key_at_idx(block: &Block, idx: usize) -> KeyVec {
        let key_len_start = block.offsets[idx] as usize;
        let key_len = (&block.data[key_len_start..]).get_u16() as usize;

        let key_data_start = key_len_start + SIZEOF_U16;
        let key_data = (&block.data[key_data_start..key_data_start + key_len]);

        let mut key = KeyVec::new();
        key.append(key_data);
        key
    }

    pub fn get_data_value_range_at_idx(block: &Block, idx: usize) -> (usize, usize) {
        let key_len_start = block.offsets[idx] as usize;
        let key_len = (&block.data[key_len_start..]).get_u16() as usize;

        let data_len_start = key_len_start + SIZEOF_U16 + key_len;
        let data_len = (&block.data[data_len_start..]).get_u16() as usize;

        (
            data_len_start + SIZEOF_U16,
            data_len_start + SIZEOF_U16 + data_len,
        )
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter.first_key = iter.key.clone();

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::create_and_seek_to_first(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        if (!self.is_valid()) {
            panic!("Cannot return key since iterator is no longer valid");
        }

        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if (!self.is_valid()) {
            panic!("Cannot return key since iterator is no longer valid");
        }

        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key = Self::get_key_at_idx(&self.block, 0);
        self.value_range = Self::get_data_value_range_at_idx(&self.block, 0);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if (self.is_valid()) {
            self.idx += 1;
            if (self.idx < self.block.offsets.len()) {
                self.key = Self::get_key_at_idx(&self.block, self.idx);
                self.value_range = Self::get_data_value_range_at_idx(&self.block, self.idx);
            } else {
                self.key = KeyVec::new();
            }
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();

        while (self.is_valid() && self.key.as_key_slice().lt(&key)) {
            self.next();
        }
    }
}
