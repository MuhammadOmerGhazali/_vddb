use crate::types::Value;
use std::collections::HashMap;

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
    let mut forward = HashMap::new(); // Value -> ID
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

    // Invert the map: ID -> Value
    let reverse: HashMap<u32, Value> = forward.into_iter().map(|(k, v)| (v, k)).collect();

    (indices, reverse)
}

pub fn decompress_dictionary(indices: &[u32], dict: &HashMap<u32, Value>) -> Vec<Value> {
    indices.iter().map(|id| dict[id].clone()).collect()
}
