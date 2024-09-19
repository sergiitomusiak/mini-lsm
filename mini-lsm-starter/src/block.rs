#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut result = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2);
        result.extend_from_slice(&self.data);
        for value in self.offsets.iter() {
            result.extend_from_slice(&value.to_be_bytes());
        }
        let num_entries = self.offsets.len() as u32;
        result.extend_from_slice(&num_entries.to_be_bytes());
        result.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_entries = u32_from_offset(data, data.len() - 4) as usize;
        let data = &data[..data.len() - 4];
        let mid = data.len() - num_entries * 2;
        let (data, offset_bytes) = data.split_at(mid);
        let mut offsets = Vec::with_capacity(num_entries);
        for offset in offset_bytes.chunks(2) {
            offsets.push(u16_from_offset(offset, 0));
        }
        Self {
            data: Vec::from(data),
            offsets,
        }
    }
}

pub(crate) fn u16_from_offset(data: &[u8], offset: usize) -> u16 {
    let value: [u8; 2] = [data[offset], data[offset + 1]];
    u16::from_be_bytes(value)
}

pub(crate) fn u32_from_offset(data: &[u8], offset: usize) -> u32 {
    let value: [u8; 4] = [
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ];
    u32::from_be_bytes(value)
}
