use crate::types::{CompressionType, DbError, Value, DataType};
use std::collections::HashMap;
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::io::{Read, Cursor};

pub fn compress(values: &[Value], compression: CompressionType) -> Result<Vec<u8>, DbError> {
    match compression {
        CompressionType::None => {
            let mut buffer = Vec::new();
            for value in values {
                match value {
                    Value::Int32(i) => buffer.write_i32::<LittleEndian>(*i)?,
                    Value::Float32(f) => buffer.write_f32::<LittleEndian>(f.0)?,
                    Value::String(s) => {
                        buffer.write_u64::<LittleEndian>(s.len() as u64)?;
                        buffer.extend_from_slice(s.as_bytes());
                    }
                }
            }
            Ok(buffer)
        }
        CompressionType::Rle => {
            if values.is_empty() {
                return Ok(Vec::new());
            }
            let mut buffer = Vec::new();
            let mut current = &values[0];
            let mut count = 1;
            for value in values.iter().skip(1) {
                if value == current {
                    count += 1;
                } else {
                    write_rle_value(&mut buffer, current, count)?;
                    current = value;
                    count = 1;
                }
            }
            write_rle_value(&mut buffer, current, count)?;
            Ok(buffer)
        }
        CompressionType::Dictionary => {
            let mut dictionary: HashMap<&String, u64> = HashMap::new();
            let mut next_id = 0;
            let mut buffer = Vec::new();
            buffer.write_u64::<LittleEndian>(values.len() as u64)?;
            for value in values {
                if let Value::String(s) = value {
                    let id = *dictionary.entry(s).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    });
                    buffer.write_u64::<LittleEndian>(id)?;
                } else {
                    return Err(DbError::InvalidData("Dictionary compression only for strings".to_string()));
                }
            }
            buffer.write_u64::<LittleEndian>(dictionary.len() as u64)?;
            for (s, id) in dictionary.iter() {
                buffer.write_u64::<LittleEndian>(*id)?;
                buffer.write_u64::<LittleEndian>(s.len() as u64)?;
                buffer.extend_from_slice(s.as_bytes());
            }
            Ok(buffer)
        }
    }
}

fn write_rle_value(buffer: &mut Vec<u8>, value: &Value, count: usize) -> Result<(), DbError> {
    if count > 255 {
        return Err(DbError::InvalidData("RLE run length exceeds 255".to_string()));
    }
    buffer.write_u8(count as u8)?;
    match value {
        Value::Int32(i) => buffer.write_i32::<LittleEndian>(*i)?,
        Value::Float32(f) => buffer.write_f32::<LittleEndian>(f.0)?,
        Value::String(s) => {
            buffer.write_u64::<LittleEndian>(s.len() as u64)?;
            buffer.extend_from_slice(s.as_bytes());
        }
    }
    Ok(())
}

pub fn decompress(data: &[u8], compression: CompressionType, data_type: &DataType) -> Result<Vec<Value>, DbError> {
    match compression {
        CompressionType::None => {
            let mut values = Vec::new();
            let mut cursor = Cursor::new(data);
            while cursor.position() < data.len() as u64 {
                match data_type {
                    DataType::Int32 => {
                        let value = cursor.read_i32::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        values.push(Value::Int32(value));
                    }
                    DataType::Float32 => {
                        let value = cursor.read_f32::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        values.push(Value::Float32(ordered_float::OrderedFloat(value)));
                    }
                    DataType::String => {
                        let len = cursor.read_u64::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))? as usize;
                        let mut string_data = vec![0u8; len];
                        cursor.read_exact(&mut string_data)?;
                        let s = String::from_utf8(string_data)
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        values.push(Value::String(s));
                    }
                }
            }
            Ok(values)
        }
        CompressionType::Rle => {
            let mut values = Vec::new();
            let mut cursor = Cursor::new(data);
            while cursor.position() < data.len() as u64 {
                let count = cursor.read_u8()
                    .map_err(|e| DbError::SerializationError(e.to_string()))? as usize;
                if count == 0 {
                    return Err(DbError::SerializationError("Invalid RLE run length".to_string()));
                }
                match data_type {
                    DataType::Int32 => {
                        let value = cursor.read_i32::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        for _ in 0..count {
                            values.push(Value::Int32(value));
                        }
                    }
                    DataType::Float32 => {
                        let value = cursor.read_f32::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        for _ in 0..count {
                            values.push(Value::Float32(ordered_float::OrderedFloat(value)));
                        }
                    }
                    DataType::String => {
                        let len = cursor.read_u64::<LittleEndian>()
                            .map_err(|e| DbError::SerializationError(e.to_string()))? as usize;
                        let mut string_data = vec![0u8; len];
                        cursor.read_exact(&mut string_data)?;
                        let s = String::from_utf8(string_data)
                            .map_err(|e| DbError::SerializationError(e.to_string()))?;
                        for _ in 0..count {
                            values.push(Value::String(s.clone()));
                        }
                    }
                }
            }
            Ok(values)
        }
        CompressionType::Dictionary => {
            let mut cursor = Cursor::new(data);
            let value_count = cursor.read_u64::<LittleEndian>()
                .map_err(|e| DbError::SerializationError(format!("Failed to read value count: {}", e)))? as usize;
            if value_count == 0 {
                return Ok(Vec::new());
            }
            let mut ids = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                let id = cursor.read_u64::<LittleEndian>()
                    .map_err(|e| DbError::SerializationError(format!("Failed to read ID: {}", e)))?;
                ids.push(id);
            }
            let dict_size = cursor.read_u64::<LittleEndian>()
                .map_err(|e| DbError::SerializationError(format!("Failed to read dict size: {}", e)))? as usize;
            let mut dictionary = HashMap::with_capacity(dict_size);
            for _ in 0..dict_size {
                let id = cursor.read_u64::<LittleEndian>()
                    .map_err(|e| DbError::SerializationError(format!("Failed to read dict ID: {}", e)))?;
                let len = cursor.read_u64::<LittleEndian>()
                    .map_err(|e| DbError::SerializationError(format!("Failed to read string len: {}", e)))? as usize;
                let mut string_data = vec![0u8; len];
                cursor.read_exact(&mut string_data)
                    .map_err(|e| DbError::SerializationError(format!("Failed to read string data: {}", e)))?;
                let s = String::from_utf8(string_data)
                    .map_err(|e| DbError::SerializationError(e.to_string()))?;
                dictionary.insert(id, s);
            }
            let mut values = Vec::with_capacity(value_count);
            for id in ids {
                let s = dictionary.get(&id).ok_or_else(|| {
                    DbError::SerializationError(format!("Invalid dictionary ID: {}", id))
                })?.clone();
                values.push(Value::String(s));
            }
            Ok(values)
        }
    }
}

pub fn estimate_compressed_size(values: &[Value], compression: CompressionType) -> usize {
    match compression {
        CompressionType::None => values.iter().map(|v| match v {
            Value::Int32(_) => 4,
            Value::Float32(_) => 4,
            Value::String(s) => 8 + s.len(),
        }).sum(),
        CompressionType::Rle => {
            if values.is_empty() {
                return 0;
            }
            let mut size = 0;
            let mut current = &values[0];
            let mut _count = 1;
            for value in values.iter().skip(1) {
                if value != current {
                    size += 1 + match current {
                        Value::Int32(_) => 4,
                        Value::Float32(_) => 4,
                        Value::String(s) => 8 + s.len(),
                    };
                    current = value;
                    _count = 1;
                } else {
                    _count += 1;
                }
            }
            size + 1 + match current {
                Value::Int32(_) => 4,
                Value::Float32(_) => 4,
                Value::String(s) => 8 + s.len(),
            }
        }
        CompressionType::Dictionary => {
            let mut dictionary: HashMap<&String, u64> = HashMap::new();
            let mut next_id = 0;
            for value in values {
                if let Value::String(s) = value {
                    dictionary.entry(s).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    });
                }
            }
            8 + (values.len() * 8) + dictionary.iter().map(|(s, _)| 8 + 8 + s.len()).sum::<usize>()
        }
    }
}