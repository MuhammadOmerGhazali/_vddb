use crate::query::Condition;
use crate::types::{CompressionType, DataType, DbError, Value};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockInfo {
    pub offset: u64,
    pub row_count: usize,
    pub min: Value,
    pub max: Value,
    pub compression: CompressionType,
    #[serde(default)]
    pub serialized_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockMetadata {
    pub column_name: String,
    pub data_type: DataType,
    pub blocks: Vec<BlockInfo>,
    pub metadata_path: String,
    #[serde(default)]
    pub index_enabled: bool,
}

impl BlockMetadata {
    pub fn new_metadata(column_name: &str, data_type: DataType, data_dir: &str) -> BlockMetadata {
        let metadata_path = format!("{}/metadata/{}.json", data_dir, column_name);
        BlockMetadata {
            column_name: column_name.to_string(),
            data_type,
            blocks: Vec::new(),
            metadata_path,
            index_enabled: false,
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
    ) -> Result<(), DbError> {
        if min.data_type() != self.data_type || max.data_type() != self.data_type {
            return Err(DbError::TypeMismatch);
        }
        println!("DEBUG: Adding block for {}: min={:?}, max={:?}, offset={}", self.column_name, min, max, offset);
        self.blocks.push(BlockInfo {
            offset,
            row_count,
            min,
            max,
            compression,
            serialized_size: Some(serialized_size),
        });
        self.save()?;
        Ok(())
    }

    pub fn enable_index(&mut self) -> Result<(), DbError> {
        self.index_enabled = true;
        self.save()?;
        Ok(())
    }

    pub fn save(&self) -> Result<(), DbError> {
        let parent = Path::new(&self.metadata_path)
            .parent()
            .or_else(|| Some(Path::new("")))
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
            index_enabled: deserialized.index_enabled,
        })
    }

    pub fn get_blocks(&self, condition: Option<&Condition>) -> Vec<&BlockInfo> {
        if let Some(cond) = condition {
            fn matches_condition(block: &BlockInfo, condition: &Condition, column_name: &str) -> bool {
                match condition {
                    Condition::GreaterThan(col, value) if col == column_name => match (&block.min, &block.max, value) {
                        (Value::Int32(_min), Value::Int32(max), Value::Int32(v)) => *max > *v,
                        (Value::Float32(_min), Value::Float32(max), Value::Float32(v)) => *max > *v,
                        (Value::String(_min), Value::String(max), Value::String(v)) => max > v,
                        _ => false,
                    },
                    Condition::LessThan(col, value) if col == column_name => match (&block.min, &block.max, value) {
                        (Value::Int32(min), Value::Int32(_max), Value::Int32(v)) => *min < *v,
                        (Value::Float32(min), Value::Float32(_max), Value::Float32(v)) => *min < *v,
                        (Value::String(min), Value::String(_max), Value::String(v)) => min < v,
                        _ => false,
                    },
                    Condition::Equal(col, value) if col == column_name => match (&block.min, &block.max, value) {
                        (Value::Int32(min), Value::Int32(max), Value::Int32(v)) => *min <= *v && *v <= *max,
                        (Value::Float32(min), Value::Float32(max), Value::Float32(v)) => *min <= *v && *v <= *max,
                        (Value::String(min), Value::String(max), Value::String(v)) => min <= v && v <= max,
                        _ => false,
                    },
                    Condition::And(left, right) => {
                        matches_condition(block, left, column_name) && matches_condition(block, right, column_name)
                    }
                    Condition::Or(left, right) => {
                        matches_condition(block, left, column_name) || matches_condition(block, right, column_name)
                    }
                    _ => true, // Return all blocks if condition references another column
                }
            }
            let blocks: Vec<&BlockInfo> = self.blocks
                .iter()
                .filter(|block| {
                    let result = matches_condition(block, cond, &self.column_name);
                    println!("DEBUG: Block offset {} for {} with condition {:?}: {}", block.offset, self.column_name, cond, result);
                    result
                })
                .collect();
            println!("DEBUG: Selected blocks for {} with condition {:?}: {:?}", self.column_name, cond, blocks.iter().map(|b| b.offset).collect::<Vec<_>>());
            blocks
        } else {
            self.blocks.iter().collect()
        }
    }
}