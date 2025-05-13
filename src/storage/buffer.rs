use crate::types::DbError;
use std::collections::HashMap;

pub struct BufferManager {
    buffers: HashMap<String, Vec<u8>>,
    max_size: usize,
}

impl BufferManager {
    pub fn new(max_size: usize) -> Self {
        BufferManager {
            buffers: HashMap::new(),
            max_size,
        }
    }

    pub fn get_buffer(&self, key: &str) -> Result<&Vec<u8>, DbError> {
        self.buffers
            .get(key)
            .ok_or_else(|| DbError::InvalidData(format!("Buffer {} not found", key)))
    }

    pub fn put_buffer(&mut self, key: String, data: Vec<u8>) -> Result<(), DbError> {
        if data.len() > self.max_size {
            return Err(DbError::InvalidData(format!(
                "Buffer size {} exceeds maximum {}",
                data.len(),
                self.max_size
            )));
        }
        self.buffers.insert(key, data);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.buffers.clear();
    }
}