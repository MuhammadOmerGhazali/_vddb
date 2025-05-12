use crate::types::{DataType, Value, CompressionType, DbError};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockInfo {
    pub offset: u64,
    pub row_count: usize,
    pub null_count: usize,
    pub min: Value,
    pub max: Value,
    pub compression: CompressionType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockMetadata {
    pub column_name: String,
    pub data_type: DataType,
    pub blocks: Vec<BlockInfo>,
    pub metadata_path: String,
}
impl BlockMetadata {
    pub fn new_metadata(column_name: &str, data_type: DataType, data_dir: &str ) -> BlockMetadata {
        let metadata_path = format!("{}/metadata/{}.json", data_dir, column_name);
        BlockMetadata {
            column_name: column_name.to_string(),
            data_type,
            blocks: Vec::new(),
            metadata_path,
        }
    }
    pub fn add_block(
        &mut self,
        min: Value,
        max: Value,
        offset: u64,
        row_count: usize,
        null_count:usize,
        compression: CompressionType,
    ) -> Result<(), DbError> {
        if min.data_type() != self.data_type || max.data_type() != self.data_type {
            return Err(DbError::TypeMismatch);
        }
        self.blocks.push(BlockInfo {
            offset,
            row_count,
            null_count,
            min,
            max,
            compression,
        });
        self.save()?;
        Ok(())
    }
    pub fn save(&self) -> Result<(), DbError> {
        let parent = Path::new(&self.metadata_path).parent()
            .ok_or_else(|| DbError::InvalidData("Invalid metadata path".to_string()))?;
        fs::create_dir_all(parent)?;
        let json = serde_json::to_string_pretty(&self)?;
        fs::write(&self.metadata_path, json)?;
        Ok(())
    }
    pub fn load(column_name: &str, data_type: DataType, data_dir: &str) -> Result<BlockMetadata, DbError> {
        let metadata_path = format!("{}/metadata/{}.json", data_dir, column_name);
        if !Path::new(&metadata_path).exists() {
            return Ok(BlockMetadata::new_metadata(column_name, data_type, data_dir));
        }
        let json = fs::read_to_string(&metadata_path)?;
        let deserialized: BlockMetadata = serde_json::from_str(&json)?;
        Ok(BlockMetadata {
            column_name: column_name.to_string(),
            data_type,
            blocks: deserialized.blocks,
            metadata_path,
        })
    }
}

//========================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, CompressionType, DataType};
    use std::fs;

    fn cleanup_test_metadata(path: &str) {
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_add_and_save_block() {
        let dir = "test_data";
        cleanup_test_metadata(dir);

        let mut meta = BlockMetadata::new_metadata("age", DataType::Int32, dir);
        let block_result = meta.add_block(
            Value::Int32(1),
            Value::Int32(100),
            0,
            1000,
            0,
            CompressionType::None,
        );

        assert!(block_result.is_ok());
        assert_eq!(meta.blocks.len(), 1);
        assert!(Path::new(&meta.metadata_path).exists());

        cleanup_test_metadata(dir);
    }

    #[test]
    fn test_type_mismatch() {
        let dir = "test_data";
        cleanup_test_metadata(dir);

        let mut meta = BlockMetadata::new_metadata("score", DataType::Float32, dir);
        let result = meta.add_block(
            Value::Int32(1),
            Value::Int32(10),
            0,
            50,
            0,
            CompressionType::None,
        );

        assert!(matches!(result, Err(DbError::TypeMismatch)));

        cleanup_test_metadata(dir);
    }

    #[test]
    fn test_load_metadata() {
        let dir = "test_data";
        cleanup_test_metadata(dir);

        let mut meta = BlockMetadata::new_metadata("hp", DataType::Int32, dir);
        meta.add_block(
            Value::Int32(10),
            Value::Int32(100),
            0,
            50,
            0,
            CompressionType::None,
        ).unwrap();

        let loaded = BlockMetadata::load("hp", DataType::Int32, dir).unwrap();
        assert_eq!(loaded.blocks.len(), 1);
        assert_eq!(loaded.blocks[0].min, Value::Int32(10));

        cleanup_test_metadata(dir);
    }
}
