#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use builder::SsTableBuilder;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let offset = buf.len() as u32;
        // num of meta
        let num_meta = block_meta.len();
        buf.extend_from_slice(num_meta.to_le_bytes().as_slice());
        for meta in block_meta {
            // offset of the data block
            buf.extend_from_slice(meta.offset.to_le_bytes().as_slice());
            let len_first_key = meta.first_key.len() as u16;
            let len_last_key = meta.last_key.len() as u16;
            buf.extend_from_slice(len_first_key.to_le_bytes().as_slice());
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.extend_from_slice(len_last_key.to_le_bytes().as_slice());
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
        // offset of meta block
        buf.extend_from_slice(offset.to_le_bytes().as_slice());
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let num_meta = buf.get_u64_le();
        let mut metas = Vec::new();
        for i in 0..num_meta {
            let offset = buf.get_u64_le() as usize;
            let len_first_key = buf.get_u16_le();
            let mut first_key = Vec::new();
            for i in 0..len_first_key {
                first_key.push(buf.get_u8());
            }
            let len_last_key = buf.get_u16_le();
            let mut last_key = Vec::new();
            for i in 0..len_last_key {
                last_key.push(buf.get_u8());
            }
            metas.push(BlockMeta {
                offset: offset as usize,
                first_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                    first_key.as_slice(),
                )),
                last_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(last_key.as_slice())),
            });
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let offset_meta = file
            .read(
                file.size() - std::mem::size_of::<u32>() as u64,
                std::mem::size_of::<u32>() as u64,
            )
            .unwrap()
            .as_slice()
            .read_u32::<LittleEndian>()
            .unwrap() as u64;
        let metas = BlockMeta::decode_block_meta(
            file.read(
                offset_meta,
                file.size() - offset_meta - std::mem::size_of::<u32>() as u64,
            )
            .unwrap()
            .as_slice(),
        );
        Ok(Self {
            file,
            block_meta_offset: offset_meta as usize,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                metas.first().unwrap().first_key.raw_ref(),
            )),
            last_key: KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                metas.last().unwrap().last_key.raw_ref(),
            )),
            block_meta: metas,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(self.block_meta.len() > block_idx);
        let offset = self.block_meta[block_idx].offset;
        let len;
        if block_idx < self.block_meta.len() - 1 {
            len = self.block_meta[block_idx + 1].offset - offset;
        } else {
            assert_eq!(block_idx, self.block_meta.len() - 1);
            len = self.block_meta_offset - offset;
        }
        let block = Block::decode(
            self.file
                .read(offset as u64, len as u64)
                .unwrap()
                .as_slice(),
        );
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_cache.is_none() {
            return self.read_block(block_idx);
        }
        let cache = self.block_cache.as_ref().unwrap();
        cache
            .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
            .map_err(|e| anyhow!("{}", e))
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let res = self
            .block_meta
            .binary_search_by(|meta| meta.first_key.cmp(&(key.to_key_vec().into_key_bytes())));
        match res {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    idx
                } else {
                    idx - 1
                }
            }
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
