use crate::types::{Value, DbError, DataType, CompressionType};
use crate::storage::compression::{compress_rle, decompress_rle, compress_dictionary, decompress_dictionary};
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

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u8(match self.compression {
            CompressionType::None => 0,
            CompressionType::Rle => 1,
            CompressionType::Dictionary => 2,
        }).unwrap();
        match self.compression {
            CompressionType::None => {
                buf.write_u32::<LittleEndian>(self.values.len() as u32).unwrap();
                for value in &self.values {
                    buf.extend_from_slice(&value.serialize());
                }
            }
            CompressionType::Rle => {
                let compressed = compress_rle(&self.values);
                buf.write_u32::<LittleEndian>(compressed.len() as u32).unwrap();
                for (value, count) in compressed {
                    buf.extend_from_slice(&value.serialize());
                    buf.write_u32::<LittleEndian>(count).unwrap();
                }
            }
            CompressionType::Dictionary => {
                let (indices, dict) = compress_dictionary(&self.values);
                buf.write_u32::<LittleEndian>(dict.len() as u32).unwrap();
                for (id, value) in dict {
                    buf.write_u32::<LittleEndian>(id).unwrap();
                    buf.extend_from_slice(&value.serialize());
                }
                buf.write_u32::<LittleEndian>(indices.len() as u32).unwrap();
                for index in indices {
                    buf.write_u32::<LittleEndian>(index).unwrap();
                }
            }
        }
        buf
    }

    pub fn deserialize(bytes: &[u8], data_type: &DataType, compression: CompressionType) -> Result<Block, DbError> {
        let mut cursor = Cursor::new(bytes);
        let compression_byte = cursor.read_u8()?;
        if compression_byte != match compression {
            CompressionType::None => 0,
            CompressionType::Rle => 1,
            CompressionType::Dictionary => 2,
        } {
            return Err(DbError::InvalidData("Compression type mismatch".to_string()));
        }
        match compression {
            CompressionType::None => {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut values = Vec::with_capacity(len);
                let mut pos = cursor.position() as usize;
                for _ in 0..len {
                    let value = Value::deserialize(data_type, &bytes[pos..])?;
                    values.push(value);
                    let value_size = match data_type {
                        DataType::Int32 => 4,
                        DataType::Float32 => 4,
                        DataType::String => {
                            let mut temp_cursor = Cursor::new(&bytes[pos..]);
                            let len = temp_cursor.read_u32::<LittleEndian>()? as usize;
                            len + 4
                        }
                    };
                    pos += value_size;
                    cursor.set_position(pos as u64);
                }
                Ok(Block { values, compression })
            }
            CompressionType::Rle => {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut compressed = Vec::new();
                let mut pos = cursor.position() as usize;
                for _ in 0..len {
                    let value = Value::deserialize(data_type, &bytes[pos..])?;
                    let value_size = match data_type {
                        DataType::Int32 => 4,
                        DataType::Float32 => 4,
                        DataType::String => {
                            let mut temp_cursor = Cursor::new(&bytes[pos..]);
                            let len = temp_cursor.read_u32::<LittleEndian>()? as usize;
                            len + 4
                        }
                    };
                    pos += value_size;
                    let mut temp_cursor = Cursor::new(&bytes[pos..]);
                    let count = temp_cursor.read_u32::<LittleEndian>()?;
                    pos += 4;
                    compressed.push((value, count));
                    cursor.set_position(pos as u64);
                }
                let values = decompress_rle(&compressed);
                Ok(Block { values, compression })
            }
            CompressionType::Dictionary => {
                let dict_len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut dict = std::collections::HashMap::new();
                let mut pos = cursor.position() as usize;
                for _ in 0..dict_len {
                    let mut temp_cursor = Cursor::new(&bytes[pos..]);
                    let id = temp_cursor.read_u32::<LittleEndian>()?;
                    pos += 4;
                    let value = Value::deserialize(data_type, &bytes[pos..])?;
                    let value_size = match data_type {
                        DataType::Int32 => 4,
                        DataType::Float32 => 4,
                        DataType::String => {
                            let mut temp_cursor = Cursor::new(&bytes[pos..]);
                            let len = temp_cursor.read_u32::<LittleEndian>()? as usize;
                            len + 4
                        }
                    };
                    pos += value_size;
                    dict.insert(id, value);
                    cursor.set_position(pos as u64);
                }
                let indices_len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut indices = Vec::with_capacity(indices_len);
                for _ in 0..indices_len {
                    let id = cursor.read_u32::<LittleEndian>()?;
                    indices.push(id);
                }
                let values = decompress_dictionary(&indices, &dict);
                Ok(Block { values, compression })
            }
        }
    }
}