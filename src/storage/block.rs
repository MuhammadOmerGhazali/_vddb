use crate::types::{Value, DbError, DataType, CompressionType};
use crate::storage::compression::{compress, compress_dictionary, decompress, decompress_dictionary, decompress_rle};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct Block {
    pub values: Vec<Value>,
    pub compression: CompressionType,
}

impl Block {
    pub fn new(values: Vec<Value>, compression: CompressionType) -> Result<Block, DbError> {
        if values.is_empty() {
            return Err(DbError::InvalidData("Block cannot be empty".to_string()));
        }
        Ok(Block { values, compression })
    }

    pub fn min_max(&self) -> (Value, Value) {
        let mut min = self.values[0].clone();
        let mut max = self.values[0].clone();
        for value in &self.values[1..] {
            match (value, &min, &max) {
                (Value::Int32(v), Value::Int32(m), Value::Int32(mx)) => {
                    if v < m { min = Value::Int32(*v); }
                    if v > mx { max = Value::Int32(*v); }
                }
                (Value::Float32(v), Value::Float32(m), Value::Float32(mx)) => {
                    if v < m { min = Value::Float32(*v); }
                    if v > mx { max = Value::Float32(*v); }
                }
                (Value::String(v), Value::String(m), Value::String(mx)) => {
                    if v < m { min = Value::String(v.clone()); }
                    if v > mx { max = Value::String(v.clone()); }
                }
                _ => {}
            }
        }
        (min, max)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, DbError> {
        compress(&self.values, self.compression.clone())
    }

    pub fn deserialize(bytes: &[u8], data_type: &DataType, compression: CompressionType) -> Result<Block, DbError> {
        println!("DEBUG: Deserializing block with compression {:?}", compression);
        println!("DEBUG: First bytes: {:?}", &bytes[..std::cmp::min(bytes.len(), 16)]);
        let values = decompress(bytes, data_type, 0, compression.clone())?;
        println!("DEBUG: Deserialized values: {:?}", values);
        Ok(Block { values, compression })
    }
}