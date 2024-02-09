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
    len: usize,
    last_doc_id: PointOffsetType,
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
        let len = posting_list.len();
        let last_doc_id = *posting_list.list.last().unwrap();

        let bitpacker = bitpacking::BitPacker4x::new();
        posting_list.list.sort_unstable();

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
            last_doc_id,
            data: data.into_boxed_slice(),
            chunks: chunks.into_boxed_slice(),
        }
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        if !self.is_in_postings_range(*val) {
            return false;
        }

        // find the chunk that may contain the value and check if the value is in the chunk
        let chunk_index = self.find_chunk(val, None);
        if let Some(chunk_index) = chunk_index {
            if self.chunks[chunk_index].initial == *val {
                return true;
            }

            let mut decompressed = [0u32; bitpacking::BitPacker4x::BLOCK_LEN];
            self.decompress_chunk(
                &bitpacking::BitPacker4x::new(),
                chunk_index,
                &mut decompressed,
            );
            decompressed.binary_search(val).is_ok()
        } else {
            false
        }
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<u32>()
            + std::mem::size_of::<Box<[CompressedPostingChunk]>>() * self.chunks.len()
            + self.data.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        let bitpacker = bitpacking::BitPacker4x::new();
        (0..self.chunks.len())
            .flat_map(move |chunk_index| {
                let mut decompressed = [0u32; bitpacking::BitPacker4x::BLOCK_LEN];
                self.decompress_chunk(&bitpacker, chunk_index, &mut decompressed);
                decompressed.into_iter()
            })
            .take(self.len)
    }

    fn get_chunk_size(chunks: &[CompressedPostingChunk], data: &[u8], chunk_index: usize) -> usize {
        assert!(chunk_index < chunks.len());
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
    }

    fn find_chunk(&self, doc_id: &PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        let start_chunk = if let Some(idx) = start_chunk { idx } else { 0 };
        match self.chunks[start_chunk..].binary_search_by(|chunk| chunk.initial.cmp(doc_id)) {
            // doc_id is the initial value of the chunk with index idx
            Ok(idx) => Some(idx),
            // chunk idx has larger initial value than doc_id
            // so we need the previous chunk
            Err(idx) => {
                if idx > 0 {
                    Some(idx - 1)
                } else {
                    None
                }
            }
        }
    }

    fn is_in_postings_range(&self, val: PointOffsetType) -> bool {
        self.chunks.len() > 0 && val >= self.chunks[0].initial && val <= self.last_doc_id
    }

    fn decompress_chunk(
        &self,
        bitpacker: &bitpacking::BitPacker4x,
        chunk_index: usize,
        decompressed: &mut [PointOffsetType],
    ) {
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(&self.chunks, &self.data, chunk_index);
        let chunk_bits = (chunk_size * 8) / bitpacking::BitPacker4x::BLOCK_LEN;
        bitpacker.decompress_sorted(
            chunk.initial,
            &self.data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed,
            chunk_bits as u8,
        );
    }
}

pub struct CompressedPostingVisitor<'a> {
    bitpacker: bitpacking::BitPacker4x,
    postings: &'a CompressedPostingList,
    decompressed_chunk: [PointOffsetType; bitpacking::BitPacker4x::BLOCK_LEN],
    decompressed_chunk_idx: Option<usize>,
    #[cfg(test)]
    last_checked: Option<PointOffsetType>,
}

impl<'a> CompressedPostingVisitor<'a> {
    pub fn new(postings: &'a CompressedPostingList) -> CompressedPostingVisitor<'a> {
        CompressedPostingVisitor {
            bitpacker: bitpacking::BitPacker4x::new(),
            postings,
            decompressed_chunk: [0; bitpacking::BitPacker4x::BLOCK_LEN],
            decompressed_chunk_idx: None,
            #[cfg(test)]
            last_checked: None,
        }
    }

    pub fn contains(&mut self, val: &PointOffsetType) -> bool {
        #[cfg(test)]
        {
            // check if the checked values are in the increasing order
            if let Some(last_checked) = self.last_checked {
                assert!(*val > last_checked);
            }
            self.last_checked = Some(*val);
        }

        if !self.postings.is_in_postings_range(*val) {
            return false;
        }

        // check if current decompressed chunks range contains the value
        if self.decompressed_chunk_idx.is_some() {
            // check if value is in decompressed chunk range
            // check for max value in the chunk only because we already checked for min value while decompression
            let last_decompressed =
                &self.decompressed_chunk[bitpacking::BitPacker4x::BLOCK_LEN - 1];
            match val.cmp(last_decompressed) {
                std::cmp::Ordering::Less => {
                    // value is less than the last decompressed value
                    return self.decompressed_chunk.binary_search(val).is_ok();
                }
                std::cmp::Ordering::Equal => {
                    // value is equal to the last decompressed value
                    return true;
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        // decompressed chunk is not in the range, so we need to decompress another chunk
        // first, check if there is a chunk that contains the value
        let chunk_index = match self.postings.find_chunk(val, self.decompressed_chunk_idx) {
            Some(idx) => idx,
            None => return false,
        };
        // if the value is the initial value of the chunk, we don't need to decompress the chunk
        if self.postings.chunks[chunk_index].initial == *val {
            return true;
        }

        // second, decompress the chunk and check if the value is in the decompressed chunk
        self.postings
            .decompress_chunk(&self.bitpacker, chunk_index, &mut self.decompressed_chunk);
        self.decompressed_chunk_idx = Some(chunk_index);

        // check if the value is in the decompressed chunk
        self.decompressed_chunk.binary_search(val).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_posting_visitor() {
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
