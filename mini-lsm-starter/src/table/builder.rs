#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // special case1: the first kv of the sst
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }
        // special case2: the first kv of the block
        if self.builder.is_empty() {
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
            });
        }
        // common case
        let success = self.builder.add(key, value);
        // special case3: the builder is full
        if !success {
            let builder_old =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let block = builder_old.build();
            // add the data block
            self.data.append(&mut block.encode_vec());
            return self.add(key, value);
        }
        // update the last key of the builder
        self.last_key = key.raw_ref().to_vec();
        // update meta
        self.meta.last_mut().unwrap().last_key =
            KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            let block = self.builder.build();
            // add the data block
            self.data.append(&mut block.encode_vec());
        }
        let offset_meta = self.data.len();
        BlockMeta::encode_block_meta(self.meta.as_slice(), &mut self.data);
        let file = super::FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: offset_meta,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                self.first_key.as_slice(),
            )),
            last_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(self.last_key.as_slice())),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
