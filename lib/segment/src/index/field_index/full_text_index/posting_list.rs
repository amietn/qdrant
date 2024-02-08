use bitpacking::BitPacker;
use common::types::PointOffsetType;

#[derive(Clone, Debug, Default)]
pub struct PostingList {
    list: Vec<PointOffsetType>,
}

impl PostingList {
    pub fn new(idx: PointOffsetType) -> Self {
        Self { list: vec![idx] }
    }

    pub fn size(&self) -> usize {
        self.list.capacity() * std::mem::size_of::<PointOffsetType>()
            + std::mem::size_of::<Vec<PointOffsetType>>()
    }

    pub fn insert(&mut self, idx: PointOffsetType) {
        if let Err(insertion_idx) = self.list.binary_search(&idx) {
            // Yes, this is O(n) but:
            // 1. That would give us maximal search performance with minimal memory usage
            // 2. Documents are inserted mostly sequentially, especially in large segments
            // 3. Vector indexing is more expensive anyway
            self.list.insert(insertion_idx, idx);
        }
    }

    pub fn remove(&mut self, idx: PointOffsetType) {
        if let Ok(removal_idx) = self.list.binary_search(&idx) {
            self.list.remove(removal_idx);
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        self.list.binary_search(val).is_ok()
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.list.iter().copied()
    }
}

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingList {
    len: u32,
    data: Box<[u8]>,
    chunks: Box<[CompressedPostingChunk]>,
}

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingChunk {
    initial: PointOffsetType,
    offset: u32,
}

impl CompressedPostingList {
    pub fn new(mut posting_list: PostingList) -> Self {
        if posting_list.list.is_empty() {
            return Self::default();
        }

        let bitpacker = bitpacking::BitPacker4x::new();
        posting_list.list.sort_unstable();
        let len = posting_list.len() as u32;

        let last = *posting_list.list.last().unwrap();
        while posting_list.list.len() % bitpacking::BitPacker4x::BLOCK_LEN != 0 {
            posting_list.list.push(last);
        }

        // calculate chunks count
        let chunks_count = posting_list
            .len()
            .div_ceil(bitpacking::BitPacker4x::BLOCK_LEN);
        // fill chunks data
        let mut chunks = Vec::with_capacity(chunks_count);
        let mut data_size = 0;
        for chunk_data in posting_list
            .list
            .chunks_exact(bitpacking::BitPacker4x::BLOCK_LEN)
        {
            let initial = chunk_data[0];
            let chunk_bits: u8 = bitpacker.num_bits_sorted(initial, chunk_data);
            let chunk_size = bitpacking::BitPacker4x::compressed_block_size(chunk_bits);
            chunks.push(CompressedPostingChunk {
                initial,
                offset: data_size as u32,
            });
            data_size += chunk_size;
        }

        let mut data = vec![0u8; data_size];
        for (chunk_index, chunk_data) in posting_list
            .list
            .chunks_exact(bitpacking::BitPacker4x::BLOCK_LEN)
            .enumerate()
        {
            let chunk = &chunks[chunk_index];
            let chunk_size = Self::get_chunk_size(&chunks, &data, chunk_index);
            let chunk_bits = (chunk_size * 8) / bitpacking::BitPacker4x::BLOCK_LEN;
            bitpacker.compress_sorted(
                chunk.initial,
                chunk_data,
                &mut data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                chunk_bits as u8,
            );

            // debug decompress check
            // todo: remove
            let mut decompressed = vec![0u32; bitpacking::BitPacker4x::BLOCK_LEN];
            bitpacker.decompress_sorted(
                chunk.initial,
                &data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                &mut decompressed,
                chunk_bits as u8,
            );
            if decompressed != chunk_data {
                panic!("decompressed != chunk");
            }
        }

        Self {
            len,
            data: data.into_boxed_slice(),
            chunks: chunks.into_boxed_slice(),
        }
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<u32>()
            + std::mem::size_of::<Box<[CompressedPostingChunk]>>() * self.chunks.len()
            + self.data.len()
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        let bitpacker = bitpacking::BitPacker4x::new();
        (0..self.chunks.len())
            .flat_map(move |chunk_index| {
                let chunk = &self.chunks[chunk_index];
                let chunk_size = Self::get_chunk_size(&self.chunks, &self.data, chunk_index);
                let chunk_bits = (chunk_size * 8) / bitpacking::BitPacker4x::BLOCK_LEN;
                let mut decompressed = vec![0u32; bitpacking::BitPacker4x::BLOCK_LEN];
                bitpacker.decompress_sorted(
                    chunk.initial,
                    &self.data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                    &mut decompressed,
                    chunk_bits as u8,
                );
                decompressed.into_iter()
            })
            .take(self.len as usize)
            .any(|doc_id| doc_id == *val)
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        let bitpacker = bitpacking::BitPacker4x::new();
        (0..self.chunks.len())
            .flat_map(move |chunk_index| {
                let chunk = &self.chunks[chunk_index];
                let chunk_size = Self::get_chunk_size(&self.chunks, &self.data, chunk_index);
                let chunk_bits = (chunk_size * 8) / bitpacking::BitPacker4x::BLOCK_LEN;
                let mut decompressed = vec![0u32; bitpacking::BitPacker4x::BLOCK_LEN];
                bitpacker.decompress_sorted(
                    chunk.initial,
                    &self.data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                    &mut decompressed,
                    chunk_bits as u8,
                );
                decompressed.into_iter()
            })
            .take(self.len as usize)
    }

    fn get_chunk_size(
        chunks: &[CompressedPostingChunk],
        data: &[u8],
        chunk_index: usize,
    ) -> usize {
        assert!(chunk_index < chunks.len());
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression() {
        let mut chunk: Vec<u32> = Vec::new();
        for i in 0..bitpacking::BitPacker4x::BLOCK_LEN as u32 {
            chunk.push(1000 + 2 * i);
        }
        let initial = 1000;

        let bitpacker = bitpacking::BitPacker4x::new();
        let chunk_bits: u8 = bitpacker.num_bits_sorted(initial, &chunk);
        let chunk_size = bitpacking::BitPacker4x::compressed_block_size(chunk_bits);
        println!("BLOCK_SIZE: {}", bitpacking::BitPacker4x::BLOCK_LEN * 4);
        println!("chunk_bits: {}", chunk_bits);
        println!("chunk_size: {}", chunk_size);

        let mut compressed_chunk = vec![0u8; chunk_size].into_boxed_slice();
        let compressed_size =
            bitpacker.compress_sorted(initial, &chunk, &mut compressed_chunk, chunk_bits);
        println!("compressed_size: {}", compressed_size);

        let mut decompressed = vec![0u32; bitpacking::BitPacker4x::BLOCK_LEN];
        bitpacker.decompress_sorted(initial, &compressed_chunk, &mut decompressed, chunk_bits);
        assert_eq!(decompressed, chunk);
    }
}
