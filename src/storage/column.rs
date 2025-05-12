use crate::schema::metadata::{BlockMetadata, BlockInfo};
use crate::storage::block::Block;
use crate::storage::buffer::BufferManager;
use crate::storage::compression::compress;
use crate::types::{CompressionType, DbError, Value};
use crate::schema::Column;
use crate::query::Condition;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub struct ColumnStore {
    pub column: Column,
    pub metadata: BlockMetadata,
    pub file: File,
    pub path: String,
}

impl ColumnStore {
    pub fn new(column: &Column, path: &str, data_dir: &str) -> Result<ColumnStore, DbError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let metadata = BlockMetadata::load(&column.name, column.data_type.clone(), data_dir)?;
        Ok(ColumnStore {
            column: column.clone(),
            metadata,
            file,
            path: path.to_string(),
        })
    }

    pub fn append(&mut self, values: &[Value], compression: CompressionType) -> Result<(), DbError> {
        for value in values {
            if value.data_type() != self.column.data_type {
                return Err(DbError::TypeMismatch);
            }
        }
        let block = Block::new(values.to_vec(), compression.clone())?;
        let min = values.iter().min_by(|a, b| a.cmp(b)).cloned().unwrap_or(Value::Int32(0));
        let max = values.iter().max_by(|a, b| a.cmp(b)).cloned().unwrap_or(Value::Int32(0));
        let serialized = compress(&block.values, compression.clone())?;
        let serialized_size = serialized.len();
        println!(
            "DEBUG: Appending block for column {} with compression {:?}, size {}, values {:?}",
            self.column.name, compression, serialized_size, values
        );
        self.metadata.add_block(
            min,
            max,
            self.file.seek(SeekFrom::End(0))?,
            values.len(),
            compression,
            serialized_size,
        )?;
        self.file.write_all(&serialized)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn read(&self, condition: Option<&Condition>, buffer: &mut BufferManager) -> Result<Vec<Value>, DbError> {
        let blocks = self.metadata.get_blocks(condition);
        let mut values = Vec::new();
        let mut seen_offsets = std::collections::HashSet::new();
        for block_info in blocks {
            if seen_offsets.insert(block_info.offset) {
                let block = self.read_block(block_info)?;
                println!(
                    "DEBUG: Reading block at offset {} for column {}: {:?}", 
                    block_info.offset, self.column.name, block.values
                );
                values.extend(block.values);
            }
        }
        Ok(values)
    }

    pub fn read_block(&self, block_info: &BlockInfo) -> Result<Block, DbError> {
        println!(
            "DEBUG: Reading block at offset {} for column {} with compression {:?}", 
            block_info.offset, self.column.name, block_info.compression
        );
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(block_info.offset))?;
        let size = block_info.serialized_size.unwrap_or(block_info.row_count * 8);
        println!("DEBUG: Reading {} bytes", size);
        let mut data = vec![0u8; size];
        file.read_exact(&mut data)?;
        println!("DEBUG: Raw bytes: {:?}", &data[..std::cmp::min(data.len(), 16)]); // Log first 16 bytes
        Block::deserialize(&data, &self.column.data_type, block_info.compression.clone())
    }
}