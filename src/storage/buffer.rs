use crate::types::DbError;
use crate::storage::block::Block;
use crate::storage::column::ColumnStore;
use crate::schema::metadata::BlockInfo;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom};

pub struct BufferManager {
    cache: HashMap<(String, u64), Block>,
    lru: VecDeque<(String, u64)>,
    capacity: usize,
}

impl BufferManager {
    pub fn new(capacity: usize) -> BufferManager {
        BufferManager {
            cache: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    pub fn get_block(&mut self, column: &ColumnStore, block_info: &BlockInfo) -> Result<Block, DbError> {
        let key = (column.column.name.clone(), block_info.offset);
        if let Some(block) = self.cache.get(&key) {
            self.lru.retain(|k| k != &key);
            self.lru.push_back(key.clone());
            return Ok(block.clone());
        }
        let block = column.read_block(block_info)?;
        if self.cache.len() >= self.capacity {
            self.evict()?;
        }
        self.cache.insert(key.clone(), block.clone());
        self.lru.push_back(key);
        Ok(block)
    }

    pub fn evict(&mut self) -> Result<(), DbError> {
        if let Some(key) = self.lru.pop_front() {
            self.cache.remove(&key);
        }
        Ok(())
    }
}

// impl ColumnStore {
//     pub fn read_block(&self, block_info: &BlockInfo) -> Result<Block, DbError> {
//         let mut file = self.file.try_clone()?;
//         file.seek(SeekFrom::Start(block_info.offset))?;
//         let size = block_info.serialized_size.unwrap_or(block_info.row_count * 8);
//         let mut data = vec![0u8; size];
//         file.read_exact(&mut data)?;
//         Block::deserialize(&data, &self.column.data_type, block_info.compression.clone())
//     }
// }