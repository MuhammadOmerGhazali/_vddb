use crate::query::Condition;
use crate::schema::metadata::BlockInfo;
use crate::types::{DbError, Value};

pub fn evaluate_condition_block(condition: &Condition, column_name: &str, block: &BlockInfo) -> bool {
    match condition {
        Condition::GreaterThan(col, val) if col == column_name => {
            match (&block.max, val) {
                (Value::Int32(max), Value::Int32(v)) => max > v,
                (Value::Float32(max), Value::Float32(v)) => max > v,
                (Value::String(max), Value::String(v)) => max > v,
                _ => false,
            }
        }
        Condition::LessThan(col, val) if col == column_name => {
            match (&block.min, val) {
                (Value::Int32(min), Value::Int32(v)) => min < v,
                (Value::Float32(min), Value::Float32(v)) => min < v,
                (Value::String(min), Value::String(v)) => min < v,
                _ => false,
            }
        }
        Condition::Equal(col, val) if col == column_name => {
            match (&block.min, &block.max, val) {
                (Value::Int32(min), Value::Int32(max), Value::Int32(v)) => min <= v && v <= max,
                (Value::Float32(min), Value::Float32(max), Value::Float32(v)) => min <= v && v <= max,
                (Value::String(min), Value::String(max), Value::String(v)) => min <= v && v <= max,
                _ => false,
            }
        }
        Condition::And(left, right) => {
            evaluate_condition_block(left, column_name, block)
                && evaluate_condition_block(right, column_name, block)
        }
        Condition::Or(left, right) => {
            evaluate_condition_block(left, column_name, block)
                || evaluate_condition_block(right, column_name, block)
        }
        _ => true,
    }
}

pub fn evaluate_condition_row(
    condition: &Condition,
    column_values: &std::collections::HashMap<String, Vec<Value>>,
    row_index: usize,
) -> Result<bool, DbError> {
    match condition {
        Condition::Equal(col, val) => {
            let values = column_values
                .get(col)
                .ok_or_else(|| DbError::QueryError(format!("Column {} not found", col)))?;
            Ok(values.get(row_index).map_or(false, |v| v == val))
        }
        Condition::GreaterThan(col, val) => {
            let values = column_values
                .get(col)
                .ok_or_else(|| DbError::QueryError(format!("Column {} not found", col)))?;
            Ok(values.get(row_index).map_or(false, |v| match (v, val) {
                (Value::Int32(a), Value::Int32(b)) => a > b,
                (Value::Float32(a), Value::Float32(b)) => a > b,
                (Value::String(a), Value::String(b)) => a > b,
                _ => false,
            }))
        }
        Condition::LessThan(col, val) => {
            let values = column_values
                .get(col)
                .ok_or_else(|| DbError::QueryError(format!("Column {} not found", col)))?;
            Ok(values.get(row_index).map_or(false, |v| match (v, val) {
                (Value::Int32(a), Value::Int32(b)) => a < b,
                (Value::Float32(a), Value::Float32(b)) => a < b,
                (Value::String(a), Value::String(b)) => a < b,
                _ => false,
            }))
        }
        Condition::And(left, right) => Ok(evaluate_condition_row(left, column_values, row_index)?
            && evaluate_condition_row(right, column_values, row_index)?),
        Condition::Or(left, right) => Ok(evaluate_condition_row(left, column_values, row_index)?
            || evaluate_condition_row(right, column_values, row_index)?),
    }
}