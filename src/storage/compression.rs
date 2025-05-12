use crate::types::{Value, CompressionType, DbError, DataType};
use std::collections::HashMap;
use bincode;

pub fn compress_rle(values: &[Value]) -> Vec<(Value, u32)> {
    let mut result = Vec::new();
    if values.is_empty() {
        return result;
    }
    let mut current = values[0].clone();
    let mut count = 1;
    for value in values.iter().skip(1) {
        if *value == current {
            count += 1;
        } else {
            result.push((current, count));
            current = value.clone();
            count = 1;
        }
    }
    result.push((current, count));
    result
}

pub fn decompress_rle(compressed: &[(Value, u32)]) -> Vec<Value> {
    let mut result = Vec::new();
    for (value, count) in compressed {
        result.extend(std::iter::repeat(value.clone()).take(*count as usize));
    }
    result
}

pub fn compress_dictionary(values: &[Value]) -> (Vec<u32>, HashMap<u32, Value>) {
    let mut forward = HashMap::new();
    let mut indices = Vec::new();
    let mut next_id = 0;

    for value in values {
        let id = *forward.entry(value.clone()).or_insert_with(|| {
            let id = next_id;
            next_id += 1;
            id
        });
        indices.push(id);
    }

    let reverse: HashMap<u32, Value> = forward.into_iter().map(|(k, v)| (v, k)).collect();
    (indices, reverse)
}

pub fn decompress_dictionary(indices: &[u32], dict: &HashMap<u32, Value>) -> Vec<Value> {
    indices.iter().map(|id| dict[id].clone()).collect()
}

pub fn compress(values: &[Value], compression: CompressionType) -> Result<Vec<u8>, DbError> {
    println!("DEBUG: Compressing with {:?}", compression);
    let data = match compression {
        CompressionType::None => {
            values.iter().flat_map(|v| v.serialize()).collect::<Vec<u8>>()
        }
        CompressionType::Rle => {
            let compressed = compress_rle(values);
            bincode::serialize(&compressed)
                .map_err(|e| DbError::SerializationError(e.to_string()))?
        }
        CompressionType::Dictionary => {
            let (indices, dict) = compress_dictionary(values);
            bincode::serialize(&(&indices, &dict))
                .map_err(|e| DbError::SerializationError(e.to_string()))?
        }
    };
    println!("DEBUG: Compressed size: {}, first bytes: {:?}", data.len(), &data[..std::cmp::min(data.len(), 16)]);
    Ok(data)
}

pub fn decompress(serialized: &[u8], data_type: &DataType, _row_count: usize, compression: CompressionType) -> Result<Vec<Value>, DbError> {
    println!("DEBUG: Decompressing with {:?}", compression);
    match compression {
        CompressionType::None => {
            let mut values = Vec::new();
            let mut cursor = 0;
            while cursor < serialized.len() {
                let value = Value::deserialize(data_type, &serialized[cursor..])?;
                cursor += value.serialized_size();
                values.push(value);
            }
            println!("DEBUG: Decompressed (None) values: {:?}", values);
            Ok(values)
        }
        CompressionType::Rle => {
            let compressed: Vec<(Value, u32)> = bincode::deserialize(serialized)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;
            let values = decompress_rle(&compressed);
            println!("DEBUG: Decompressed (Rle) values: {:?}", values);
            Ok(values)
        }
        CompressionType::Dictionary => {
            let (indices, dict): (Vec<u32>, HashMap<u32, Value>) = bincode::deserialize(serialized)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;
            let values = decompress_dictionary(&indices, &dict);
            println!("DEBUG: Decompressed (Dictionary) values: {:?}", values);
            Ok(values)
        }
    }
}