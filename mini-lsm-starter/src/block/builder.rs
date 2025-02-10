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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn current_size(&self) -> usize {
        return self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16;
    }

    pub fn additional_size_of_write(key: &KeySlice, value: &[u8]) -> usize {
        SIZEOF_U16 // Key Size
        + key.len() // Key Data
        + SIZEOF_U16 // Data size
        + value.len() // Data data 
        + SIZEOF_U16 // Additional offset
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // Check if we have enough space, always allow adding first time
        if (self.is_empty()
            || self.current_size() + Self::additional_size_of_write(&key, value) <= self.block_size)
        {
            self.offsets.push(self.data.len() as u16);

            // Data
            // Key size
            self.data.put_u16(key.len() as u16);

            // Key data
            self.data.put(&key.raw_ref()[..]);

            // Data size
            self.data.put_u16(value.len() as u16);

            // Data data
            self.data.put(value);

            if (self.first_key.is_empty()) {
                self.first_key = key.to_key_vec();
            }

            return true;
        } else {
            return false;
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        return self.offsets.is_empty();
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if (self.is_empty()) {
            panic!("Block is empty -- cannot finalize")
        }

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
