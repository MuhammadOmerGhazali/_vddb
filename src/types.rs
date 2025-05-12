use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int32(i32),
    Float32(f32),
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
    Rle,                                    //Run-Length Encoding
    Dictionary,
    Delta,
}

pub type TransactionId = u64;

impl Value {
    pub fn data_type(&self) -> DataType
    {
        match self
        {
            Value::Int32(_) => DataType::Int32,
            Value::Float32(_) => DataType::Float32,
            Value::String(_) => DataType::String,
        }
    }
}