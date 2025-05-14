use crate::types::{CompressionType, DataType, DbError, Value};
use crate::query::Condition;
use crate::query::evaluator::evaluate_condition_block;
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockInfo {
    pub min: Value,
    pub max: Value,
    pub offset: u64,
    pub row_count: usize,
    pub compression: CompressionType,
    pub serialized_size: Option<usize>,
    pub segment_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub column_name: String,
    pub data_type: DataType,
    pub blocks: Vec<BlockInfo>,
    pub data_dir: String, // Added to store data_dir
}

impl BlockMetadata {
    pub fn new(column_name: &str, data_type: DataType, data_dir: &str) -> Self {
        BlockMetadata {
            column_name: column_name.to_string(),
            data_type,
            blocks: Vec::new(),
            data_dir: data_dir.to_string(),
        }
    }

    pub fn load(column_name: &str, data_type: DataType, data_dir: &str) -> Result<Self, DbError> {
        let metadata_path = format!("{}/metadata/{}.json", data_dir, column_name);
        if Path::new(&metadata_path).exists() {
            let contents = fs::read_to_string(&metadata_path)
                .map_err(|e| DbError::IoError(e))?;
            let metadata: BlockMetadata = serde_json::from_str(&contents)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;
            Ok(metadata)
        } else {
            Ok(Self::new(column_name, data_type, data_dir))
        }
    }

    pub fn add_block(
        &mut self,
        min: Value,
        max: Value,
        offset: u64,
        row_count: usize,
        compression: CompressionType,
        serialized_size: usize,
        segment_path: &str,
    ) -> Result<(), DbError> {
        self.blocks.push(BlockInfo {
            min,
            max,
            offset,
            row_count,
            compression,
            serialized_size: Some(serialized_size),
            segment_path: Some(segment_path.to_string()),
        });
        self.save()?;
        Ok(())
    }

    pub fn save(&self) -> Result<(), DbError> {
        fs::create_dir_all(format!("{}/metadata", self.data_dir))?;
        let metadata_path = format!("{}/metadata/{}.json", self.data_dir, self.column_name);
        let contents = serde_json::to_string_pretty(self)
            .map_err(|e| DbError::SerializationError(e.to_string()))?;
        fs::write(&metadata_path, contents)
            .map_err(|e| DbError::IoError(e))?;
        Ok(())
    }

    pub fn get_blocks(&self, condition: Option<&Condition>) -> Vec<&BlockInfo> {
        self.blocks
            .iter()
            .filter(|block| {
                condition
                    .as_ref()
                    .map(|cond| evaluate_condition_block(cond, &self.column_name, block))
                    .unwrap_or(true)
            })
            .collect()
    }
}