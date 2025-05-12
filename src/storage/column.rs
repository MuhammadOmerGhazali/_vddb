use crate::schema::{Column, metadata::BlockMetadata};
use crate::types::{Value, DbError, CompressionType};
use crate::query::Condition;
use crate::storage::block::Block;
use crate::storage::buffer::BufferManager;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

pub struct ColumnStore {
    pub column: Column,
    pub file: File,
    pub metadata: BlockMetadata,
}

impl ColumnStore {
    pub fn new(column: &Column, file_path: &str, data_dir: &str) -> Result<ColumnStore, DbError> {
        let path = Path::new(file_path);
        if !path.starts_with(data_dir) {
            return Err(DbError::InvalidData("File path must be within data directory".to_string()));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        let metadata = BlockMetadata::load(&column.name, column.data_type.clone(), data_dir)?;
        Ok(ColumnStore {
            column: column.clone(),
            file,
            metadata,
        })
    }

    pub fn append(&mut self, values: &[Value], compression: CompressionType) -> Result<(), DbError> {
        for value in values {
            if value.data_type() != self.column.data_type {
                return Err(DbError::TypeMismatch);
            }
        }
        let block = Block::new(values.to_vec(), compression)?;
        let offset = self.file.seek(SeekFrom::End(0))?;
        let data = block.serialize();
        let serialized_size = data.len();
        self.file.write_all(&data)?;
        self.file.flush().map_err(|e| DbError::Io(e))?;
        let (min, max) = block.min_max();
        self.metadata.add_block(min, max, offset, values.len(), compression, serialized_size)?;
        Ok(())
    }

    pub fn read(&self, condition: Option<&Condition>, buffer: &mut BufferManager) -> Result<Vec<Value>, DbError> {
        let blocks = self.metadata.get_blocks(condition);
        let mut values = Vec::new();
        for block_info in blocks {
            let block = buffer.get_block(self, block_info)?;
            values.extend(block.values);
        }
        Ok(values)
    }
}