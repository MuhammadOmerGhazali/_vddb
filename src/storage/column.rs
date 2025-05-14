use crate::schema::metadata::{BlockMetadata, BlockInfo};
use crate::storage::block::Block;
use crate::storage::buffer::BufferManager;
use crate::storage::compression::compress;
use crate::types::{CompressionType, DbError, Value};
use crate::schema::Column;
use crate::query::Condition;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct ColumnStore {
    pub column: Column,
    pub metadata: BlockMetadata,
    pub data_dir: String,
    pub file_path: String, // Single file for this column
}

impl ColumnStore {
    pub fn new(column: &Column, data_dir: &str) -> Result<Self, DbError> {
        let file_path = format!("{}/columns/{}.dat", data_dir, column.name);
        let metadata = BlockMetadata::load(&column.name, column.data_type.clone(), data_dir)?;
        if !Path::new(&file_path).exists() {
            fs::create_dir_all(format!("{}/columns", data_dir))?;
            File::create(&file_path)?;
        }
        Ok(ColumnStore {
            column: column.clone(),
            metadata,
            data_dir: data_dir.to_string(),
            file_path,
        })
    }

    pub fn append(
        &mut self,
        values: &[Value],
        compression: CompressionType,
    ) -> Result<u64, DbError> {
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

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)?;
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(&serialized)?;
        file.flush()?;

        self.metadata.add_block(
            min,
            max,
            offset,
            values.len(),
            compression,
            serialized_size,
            &self.file_path,
        )?;
        Ok(offset)
    }

    pub fn read(&self, condition: Option<&Condition>, buffer: &mut BufferManager) -> Result<Vec<Value>, DbError> {
        let blocks = self.metadata.get_blocks(condition);
        let mut values = Vec::new();
        for block_info in blocks {
            match self.read_block(block_info, buffer) {
                Ok(block) => values.extend(block.values),
                Err(e) => {
                    log::warn!("Failed to read block at offset {}: {}", block_info.offset, e);
                    continue;
                }
            }
        }
        Ok(values)
    }

    pub fn read_block(&self, block_info: &BlockInfo, _buffer: &mut BufferManager) -> Result<Block, DbError> {
        let mut file = File::open(&self.file_path).map_err(|e| {
            DbError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to open column file {}: {}", self.file_path, e),
            ))
        })?;
        file.seek(SeekFrom::Start(block_info.offset))?;
        let size = block_info.serialized_size.ok_or_else(|| {
            DbError::InvalidData("Serialized size missing".to_string())
        })?;
        let mut data = vec![0u8; size];
        file.read_exact(&mut data)?;
        Block::deserialize(&data, &self.column.data_type, block_info.compression.clone())
    }

    pub fn clear(&mut self) -> Result<(), DbError> {
        self.metadata.blocks.clear();
        self.metadata.save()?;

        // Truncate the column file
        File::create(&self.file_path)?;
        Ok(())
    }
}