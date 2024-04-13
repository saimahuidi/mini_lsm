#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::mem;

use crate::key::{KeySlice, KeyVec};

use super::Block;

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
    /// return the current size the the block
    fn size(&self) -> usize {
        mem::size_of::<u8>() * self.data.len() + mem::size_of::<u16>() * (self.offsets.len() + 1)
    }
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.first_key.is_empty()
            && self.size()
                + mem::size_of::<u8>() * (key.len() + value.len())
                + mem::size_of::<u16>()
                > self.block_size
        {
            return false;
        }
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        self.offsets.push(self.data.len().try_into().unwrap());
        let mut kv_new = Vec::new();
        let key_len: u16 = key.len().try_into().unwrap();
        let val_len: u16 = value.len().try_into().unwrap();
        kv_new.extend_from_slice(&key_len.to_le_bytes());
        kv_new.extend_from_slice(key.raw_ref());
        kv_new.extend_from_slice(&val_len.to_le_bytes());
        kv_new.extend_from_slice(value);
        self.data.append(&mut kv_new);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
