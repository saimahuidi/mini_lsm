#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Ok(Self {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap()),
            blk_idx: 0,
            table,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0).unwrap());
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let id_block = table.find_block_idx(key);
        let iter_block =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(id_block).unwrap(), key);
        let mut ret = Self {
            table,
            blk_iter: iter_block,
            blk_idx: id_block,
        };
        if !ret.is_valid() {
            ret.next()?
        }
        Ok(ret)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let id_block = self.table.find_block_idx(key);
        let iter_block = BlockIterator::create_and_seek_to_key(
            self.table.read_block_cached(id_block).unwrap(),
            key,
        );
        self.blk_idx = id_block;
        self.blk_iter = iter_block;
        if !self.is_valid() {
            self.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if self.blk_iter.is_valid() {
            self.blk_iter.next();
        }
        if !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() - 1 {
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(self.blk_idx).unwrap(),
            );
        }
        Ok(())
    }
}
