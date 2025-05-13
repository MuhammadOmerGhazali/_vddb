use crate::query::{Aggregation, Condition, Query};
use crate::types::{DataType, DbError, Value};
use ordered_float::OrderedFloat;

pub fn parse_query(input: &str) -> Result<Query, DbError> {
    let input = input.trim();
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return Err(DbError::QueryError("Empty command".to_string()));
    }

    match parts[0].to_uppercase().as_str() {
        "CREATE" => parse_create_table(input),
        "INSERT" => parse_insert(input),
        "SELECT" => parse_select(input),
        "JOIN" => parse_join(input),
        _ => Err(DbError::QueryError(format!("Unknown command: {}", parts[0]))),
    }
}

fn parse_create_table(input: &str) -> Result<Query, DbError> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 4 || parts[1].to_uppercase() != "TABLE" {
        return Err(DbError::QueryError("Invalid CREATE TABLE syntax".to_string()));
    }
    let table = parts[2].to_string();
    let col_defs_start = input
        .find('(')
        .ok_or_else(|| DbError::QueryError("Missing column definitions".to_string()))?;
    let col_defs_end = input
        .rfind(')')
        .ok_or_else(|| DbError::QueryError("Missing closing parenthesis".to_string()))?;
    let col_defs = input[col_defs_start + 1..col_defs_end]
        .split(',')
        .map(|s| s.trim())
        .collect::<Vec<_>>();
    let mut columns = Vec::new();
    for col_def in col_defs {
        let col_parts = col_def.split_whitespace().collect::<Vec<_>>();
        if col_parts.len() != 2 {
            return Err(DbError::QueryError("Invalid column definition".to_string()));
        }
        let data_type = match col_parts[1].to_uppercase().as_str() {
            "INT" => DataType::Int32,
            "FLOAT" => DataType::Float32,
            "STRING" => DataType::String,
            _ => return Err(DbError::QueryError(format!("Invalid data type: {}", col_parts[1]))),
        };
        columns.push((col_parts[0].to_string(), data_type));
    }
    Ok(Query::CreateTable { table, columns })
}

fn parse_insert(input: &str) -> Result<Query, DbError> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 4 || parts[1].to_uppercase() != "INTO" || parts[3].to_uppercase() != "VALUES" {
        return Err(DbError::QueryError("Invalid INSERT syntax".to_string()));
    }
    let table = parts[2].to_string();
    let values_start = input
        .find("VALUES")
        .ok_or_else(|| DbError::QueryError("Missing VALUES clause".to_string()))?
        + 6;
    let values_str = input[values_start..].trim();
    let values = values_str[1..values_str.len() - 1]
        .split(',')
        .map(|s| s.trim())
        .map(|s| {
            if s.starts_with('"') && s.ends_with('"') {
                Ok(Value::String(s[1..s.len() - 1].to_string()))
            } else if s.contains('.') {
                s.parse::<f32>()
                    .map(|f| Value::Float32(OrderedFloat(f)))
                    .map_err(|_| DbError::QueryError(format!("Invalid float value: {}", s)))
            } else {
                s.parse::<i32>()
                    .map(|i| Value::Int32(i))
                    .map_err(|_| DbError::QueryError(format!("Invalid integer value: {}", s)))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Query::Insert { table, values })
}

fn parse_select(input: &str) -> Result<Query, DbError> {
    let columns_end = input
        .find("FROM")
        .ok_or_else(|| DbError::QueryError("Missing FROM clause".to_string()))?;
    let columns_str = input[6..columns_end].trim();
    let columns = columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let from_end = input.find("WHERE").unwrap_or(input.len());
    let table = input[columns_end + 4..from_end].trim().to_string();
    let condition = if from_end < input.len() {
        Some(parse_condition(&input[from_end + 5..].trim())?)
    } else {
        None
    };

    if columns.iter().any(|c| {
        c.to_uppercase().contains("COUNT")
            || c.to_uppercase().contains("SUM")
            || c.to_uppercase().contains("AVG")
            || c.to_uppercase().contains("MIN")
            || c.to_uppercase().contains("MAX")
    }) {
        let aggregations = columns
            .iter()
            .map(|c| {
                let c_upper = c.to_uppercase();
                if c_upper.starts_with("COUNT") {
                    Aggregation::Count
                } else if c_upper.starts_with("SUM") {
                    Aggregation::Sum(c[4..c.len() - 1].to_string())
                } else if c_upper.starts_with("AVG") {
                    Aggregation::Avg(c[4..c.len() - 1].to_string())
                } else if c_upper.starts_with("MIN") {
                    Aggregation::Min(c[4..c.len() - 1].to_string())
                } else if c_upper.starts_with("MAX") {
                    Aggregation::Max(c[4..c.len() - 1].to_string())
                } else {
                    Aggregation::Count // Fallback
                }
            })
            .collect();
        Ok(Query::SelectAggregate {
            table,
            aggregations,
            condition,
        })
    } else {
        Ok(Query::Select {
            table,
            columns,
            condition,
        })
    }
}

fn parse_join(input: &str) -> Result<Query, DbError> {
    let parts = input.split("ON").collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(DbError::QueryError("Invalid JOIN syntax".to_string()));
    }
    let select_part = parts[0].trim();
    let on_part = parts[1].trim();
    let select_parts = select_part.split_whitespace().collect::<Vec<_>>();
    if select_parts.len() < 6 {
        return Err(DbError::QueryError("Invalid JOIN syntax".to_string()));
    }
    let left_table = select_parts[1].to_string();
    let right_table = select_parts[3].to_string();
    let columns_start = input
        .find("SELECT")
        .ok_or_else(|| DbError::QueryError("Missing SELECT clause in JOIN".to_string()))?
        + 6;
    let columns_end = input.find("FROM").unwrap();
    let columns = input[columns_start..columns_end]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let on_parts = on_part.split('=').map(|s| s.trim()).collect::<Vec<_>>();
    if on_parts.len() != 2 {
        return Err(DbError::QueryError("Invalid ON clause".to_string()));
    }
    let left_column = on_parts[0].to_string();
    let right_column = on_parts[1].to_string();
    let condition = if let Some(where_pos) = input.find("WHERE") {
        Some(parse_condition(&input[where_pos + 5..].trim())?)
    } else {
        None
    };
    Ok(Query::Join {
        left_table,
        right_table,
        left_column,
        right_column,
        columns,
        condition,
    })
}

fn parse_condition(input: &str) -> Result<Condition, DbError> {
    // Handle AND/OR by splitting on keywords
    let input = input.trim();
    if input.contains(" AND ") {
        let parts = input.split(" AND ").collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err(DbError::QueryError("Invalid AND condition syntax".to_string()));
        }
        let left = parse_condition(parts[0])?;
        let right = parse_condition(parts[1])?;
        return Ok(Condition::And(Box::new(left), Box::new(right)));
    }
    if input.contains(" OR ") {
        let parts = input.split(" OR ").collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err(DbError::QueryError("Invalid OR condition syntax".to_string()));
        }
        let left = parse_condition(parts[0])?;
        let right = parse_condition(parts[1])?;
        return Ok(Condition::Or(Box::new(left), Box::new(right)));
    }

    // Parse simple condition (e.g., "ID > 1")
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(DbError::QueryError(format!(
            "Invalid condition syntax: expected 3 parts, got {}",
            parts.len()
        )));
    }
    let column = parts[0].to_string();
    let operator = parts[1];
    let value = if parts[2].starts_with('"') && parts[2].ends_with('"') {
        Value::String(parts[2][1..parts[2].len() - 1].to_string())
    } else if parts[2].contains('.') {
        Value::Float32(OrderedFloat(parts[2].parse::<f32>().map_err(|_| {
            DbError::QueryError(format!("Invalid float value: {}", parts[2]))
        })?))
    } else {
        Value::Int32(parts[2].parse::<i32>().map_err(|_| {
            DbError::QueryError(format!("Invalid integer value: {}", parts[2]))
        })?)
    };

    match operator {
        "=" => Ok(Condition::Equal(column, value)),
        ">" => Ok(Condition::GreaterThan(column, value)),
        "<" => Ok(Condition::LessThan(column, value)),
        _ => Err(DbError::QueryError(format!("Invalid operator: {}", operator))),
    }
}