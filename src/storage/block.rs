use crate::types::{CompressionType, DataType, DbError, Value};
use crate::storage::compression::{compress, decompress};

#[derive(Debug, Clone)]
pub struct Block {
    pub values: Vec<Value>,
    pub compression: CompressionType,
}

impl Block {
    pub fn new(values: Vec<Value>, compression: CompressionType) -> Result<Self, DbError> {
        if values.is_empty() {
            return Err(DbError::InvalidData("Block cannot be empty".to_string()));
        }
        let data_type = values[0].data_type();
        for value in &values {
            if value.data_type() != data_type {
                return Err(DbError::TypeMismatch);
            }
        }
        Ok(Block {
            values,
            compression,
        })
    }

    pub fn serialize(&self) -> Result<Vec<u8>, DbError> {
        compress(&self.values, self.compression.clone())
    }

    pub fn deserialize(data: &[u8], data_type: &DataType, compression: CompressionType) -> Result<Self, DbError> {
        if data.is_empty() {
            return Err(DbError::SerializationError("Empty block data".to_string()));
        }
        let expected_size = estimate_block_size(data_type, compression.clone());
        if data.len() < expected_size {
            return Err(DbError::SerializationError(format!(
                "Insufficient data: expected at least {} bytes, got {}",
                expected_size, data.len()
            )));
        }
        let values = decompress(data, compression.clone(), data_type)?;
        if values.is_empty() {
            return Err(DbError::SerializationError("No values deserialized".to_string()));
        }
        Ok(Block {
            values,
            compression,
        })
    }
}

fn estimate_block_size(data_type: &DataType, compression: CompressionType) -> usize {
    match (data_type, compression) {
        (DataType::Int32, CompressionType::Rle) => 5, // 1 byte run length + 4 bytes value
        (DataType::Float32, CompressionType::Rle) => 5,
        (DataType::String, CompressionType::Rle) => 9, // 1 byte run length + 8 bytes length + min 1 byte string
        (DataType::Int32, CompressionType::None) => 4,
        (DataType::Float32, CompressionType::None) => 4,
        (DataType::String, CompressionType::None) => 9,
        (DataType::String, CompressionType::Dictionary) => 8, // At least one ID
        _ => 1, // Fallback for invalid combinations
    }
}