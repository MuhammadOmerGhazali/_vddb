use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Type mismatch")]
    TypeMismatch,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub enum Value {
    Int32(i32),
    Float32(OrderedFloat<f32>),
    String(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int32,
    Float32,
    String,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Rle, //Run-Length Encoding
    Dictionary,
    // Delta,
}

pub type TransactionId = u64;

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int32(_) => DataType::Int32,
            Value::Float32(_) => DataType::Float32,
            Value::String(_) => DataType::String,
        }
    }
    pub fn serialize(&self) -> Vec<u8>
    {
        let mut buf = Vec::new();
        match self {
            Value::Int32(v) => buf.write_i32::<LittleEndian>(*v).unwrap(),
            Value::Float32(v) => buf.write_f32::<LittleEndian>(**v).unwrap(),
            Value::String(v) => {
                let bytes = v.as_bytes();
                buf.write_u32::<LittleEndian>(bytes.len() as u32).unwrap();
                buf.extend_from_slice(bytes);
            }
        }
        buf
    }
    pub fn deserialize(data_type:&DataType,bytes:&[u8])-> Result<Value , DbError>
    {
        let mut cursor =Cursor::new(bytes);
        match data_type {
            DataType::Int32 => Ok(Value::Int32(cursor.read_i32::<LittleEndian>()?)),
            DataType::Float32 => Ok(Value::Float32(ordered_float::OrderedFloat(cursor.read_f32::<LittleEndian>()?))),
            DataType::String => {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut string_bytes = vec![0u8;len];
                cursor.read_exact(&mut string_bytes)?;
                Ok(Value::String(String::from_utf8(string_bytes).map_err(|e| DbError::InvalidData(e.to_string()))?))
            }
        }
    }
}
