use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Int32,
    Float32,
    String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord ,Hash)]
pub enum Value {
    Int32(i32),
    Float32(OrderedFloat<f32>),
    String(String),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int32(_) => DataType::Int32,
            Value::Float32(_) => DataType::Float32,
            Value::String(_) => DataType::String,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Value::Int32(i) => i.to_le_bytes().to_vec(),
            Value::Float32(f) => f.0.to_le_bytes().to_vec(),
            Value::String(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as u32;
                let mut result = len.to_le_bytes().to_vec();
                result.extend(bytes);
                result
            }
        }
    }

    pub fn deserialize(data_type: &DataType, bytes: &[u8]) -> Result<Value, DbError> {
        match data_type {
            DataType::Int32 => {
                if bytes.len() >= 4 {
                    let mut array = [0u8; 4];
                    array.copy_from_slice(&bytes[..4]);
                    Ok(Value::Int32(i32::from_le_bytes(array)))
                } else {
                    Err(DbError::SerializationError("Insufficient bytes for Int32".to_string()))
                }
            }
            DataType::Float32 => {
                if bytes.len() >= 4 {
                    let mut array = [0u8; 4];
                    array.copy_from_slice(&bytes[..4]);
                    Ok(Value::Float32(OrderedFloat(f32::from_le_bytes(array))))
                } else {
                    Err(DbError::SerializationError("Insufficient bytes for Float32".to_string()))
                }
            }
            DataType::String => {
                if bytes.len() >= 4 {
                    let mut len_array = [0u8; 4];
                    len_array.copy_from_slice(&bytes[..4]);
                    let len = u32::from_le_bytes(len_array) as usize;
                    if bytes.len() >= 4 + len {
                        let s = String::from_utf8(bytes[4..4 + len].to_vec())
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        Ok(Value::String(s))
                    } else {
                        Err(DbError::SerializationError("Insufficient bytes for String".to_string()))
                    }
                } else {
                    Err(DbError::SerializationError("Insufficient bytes for String length".to_string()))
                }
            }
        }
    }

    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Int32(_) => 4,
            Value::Float32(_) => 4,
            Value::String(s) => 4 + s.as_bytes().len(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Rle,
    Dictionary,
}

#[derive(Debug)]
pub enum DbError {
    IoError(std::io::Error),
    SerializationError(String),
    TypeMismatch,
    InvalidData(String),
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> DbError {
        DbError::IoError(err)
    }
}
impl std::error::Error for DbError {}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::IoError(e) => write!(f, "IO Error: {}", e),
            DbError::SerializationError(s) => write!(f, "Serialization Error: {}", s),
            DbError::TypeMismatch => write!(f, "Type Mismatch"),
            DbError::InvalidData(s) => write!(f, "Invalid Data: {}", s),
        }
    }
}

impl From<serde_json::Error> for DbError {
    fn from(err: serde_json::Error) -> DbError {
        DbError::SerializationError(err.to_string())
    }
}