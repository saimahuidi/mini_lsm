#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use bytes::Bytes;
pub use iterator::BlockIterator;

use crate::key::{KeySlice, KeyVec};

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut content = self.data.clone();
        for offset in &self.offsets {
            content.append(&mut offset.to_le_bytes().to_vec());
        }
        let num_of_kvs: u16 = self.offsets.len().try_into().unwrap();
        content.append(&mut num_of_kvs.to_le_bytes().to_vec());
        Bytes::copy_from_slice(content.as_slice())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() >= 2);
        let len_content = data.len();
        let mut content = std::io::Cursor::new(data);
        content.set_position((len_content - 2).try_into().unwrap());
        let num_of_kvs = content.read_u16::<LittleEndian>().unwrap();
        let mut data_vec: Vec<u8> = Vec::new();
        let mut offsets_vec: Vec<u16> = Vec::new();
        content.set_position(0);
        for i in 0..num_of_kvs {
            let index_start: usize = content.position().try_into().unwrap();
            let len_key: u64 = content.read_u16::<LittleEndian>().unwrap().into();
            content.set_position(content.position() + len_key);
            let len_value: u64 = content.read_u16::<LittleEndian>().unwrap().into();
            content.set_position(content.position() + len_value);
            let index_end: usize = content.position().try_into().unwrap();
            data_vec.extend_from_slice(&data[index_start..index_end]);
        }
        for i in 0..num_of_kvs {
            let offset = content.read_u16::<LittleEndian>().unwrap();
            offsets_vec.push(offset);
        }
        Self {
            data: data_vec,
            offsets: offsets_vec,
        }
    }

    pub(crate) fn key(&self, idx: usize) -> KeyVec {
        let offset: usize = self.offsets[idx].into();
        self.key_with_offset(offset)
    }

    fn key_with_offset(&self, offset: usize) -> KeyVec {
        let len_key: usize = LittleEndian::read_u16(&self.data[offset..(offset + 2)]).into();
        KeyVec::from_vec(self.data[(offset + 2)..(offset + 2 + len_key)].to_vec())
    }

    pub(crate) fn value_range(&self, idx: usize) -> (usize, usize) {
        let offset: usize = self.offsets[idx].into();
        let len_key: usize = LittleEndian::read_u16(&self.data[offset..offset + 2]).into();
        let len_val: usize =
            LittleEndian::read_u16(&self.data[(offset + 2 + len_key)..(offset + 2 + len_key + 2)])
                .into();
        (offset + 2 + len_key + 2, offset + 2 + len_key + 2 + len_val)
    }

    pub(crate) fn idx(&self, key: KeySlice) -> usize {
        let key_cmp = key.to_key_vec();
        let mut ret;
        let fn_cmp = |offset: &u16| {
            let key_current = self.key_with_offset((*offset).into());
            key_current.cmp(&key_cmp)
        };
        match self.offsets.binary_search_by(fn_cmp) {
            Ok(idx) => ret = idx,
            Err(idx) => ret = idx,
        }
        if ret == self.offsets.len() {
            ret -= 1;
        }
        ret
    }
}
