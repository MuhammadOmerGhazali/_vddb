use crate::query::{Aggregation, Condition, Query};
use crate::schema::Table;
use crate::storage::StorageManager;
use crate::types::{DbError, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct QueryEngine {
    storage: Arc<Mutex<StorageManager>>,
}

impl QueryEngine {
    pub fn new(storage: Arc<Mutex<StorageManager>>) -> Self {
        QueryEngine { storage }
    }

    pub fn execute(&mut self, query: Query) -> Result<Vec<Vec<Value>>, DbError> {
        match query {
            Query::Select {
                table,
                columns,
                condition,
            } => self.execute_select(&table, &columns, condition),
            Query::SelectAggregate {
                table,
                aggregations,
                condition,
            } => self.execute_aggregate(&table, &aggregations, condition),
            Query::Join {
                left_table,
                right_table,
                left_column,
                right_column,
                columns,
                condition,
            } => self.execute_join(
                &left_table,
                &right_table,
                &left_column,
                &right_column,
                &columns,
                condition,
            ),
            Query::Insert { table, values } => {
                self.storage.lock().unwrap().insert_row(&table, values)?;
                Ok(vec![])
            }
            Query::CreateTable { table, columns } => {
                let table_def = Table {
                    name: table.clone(),
                    columns: columns
                        .into_iter()
                        .map(|(name, data_type)| crate::schema::Column { name, data_type })
                        .collect(),
                    row_count: 0,
                };
                self.storage.lock().unwrap().create_table(&table_def)?;
                Ok(vec![])
            }
        }
    }

    fn execute_select(
        &mut self,
        table: &str,
        columns: &[String],
        condition: Option<Condition>,
    ) -> Result<Vec<Vec<Value>>, DbError> {
        let _table_def = self
            .storage
            .lock()
            .unwrap()
            .schema()
            .get_table(table)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table)))?;

        let mut column_values = HashMap::new();
        for col in columns {
            let values = self.storage.lock().unwrap().read_column(table, col, condition.as_ref())?;
            column_values.insert(col.clone(), values);
        }

        let row_count = column_values
            .values()
            .next()
            .map_or(0, |v| v.len());
        let mut result = Vec::new();
        for i in 0..row_count {
            let row = columns
                .iter()
                .map(|col| column_values.get(col).unwrap()[i].clone())
                .collect();
            result.push(row);
        }
        Ok(result)
    }

    fn execute_aggregate(
        &mut self,
        table: &str,
        aggregations: &[Aggregation],
        condition: Option<Condition>,
    ) -> Result<Vec<Vec<Value>>, DbError> {
        let values = self.storage.lock().unwrap().read_column(
            table,
            &aggregations
                .first()
                .map(|agg| match agg {
                    Aggregation::Count => "ID".to_string(),
                    Aggregation::Sum(col) | Aggregation::Avg(col) | Aggregation::Min(col) | Aggregation::Max(col) => col.clone(),
                })
                .unwrap_or("ID".to_string()),
            condition.as_ref(),
        )?;

        let mut results = Vec::new();
        for agg in aggregations {
            let result = match agg {
                Aggregation::Count => Value::Int32(values.len() as i32),
                Aggregation::Sum(_) => values.iter().fold(Value::Float32(ordered_float::OrderedFloat(0.0)), |acc, v| {
                    match (acc.clone(), v) {
                        (Value::Float32(a), Value::Float32(b)) => Value::Float32(a + b),
                        _ => acc,
                    }
                }),
                Aggregation::Avg(_) => {
                    let sum = values.iter().fold(Value::Float32(ordered_float::OrderedFloat(0.0)), |acc, v| {
                        match (acc.clone(), v) {
                            (Value::Float32(a), Value::Float32(b)) => Value::Float32(a + b),
                            _ => acc,
                        }
                    });
                    match sum {
                        Value::Float32(s) if values.len() > 0 => {
                            Value::Float32(ordered_float::OrderedFloat(s.0 / values.len() as f32))
                        }
                        _ => Value::Float32(ordered_float::OrderedFloat(0.0)),
                    }
                }
                Aggregation::Min(_) => values
                    .iter()
                    .min_by(|a, b| a.cmp(b))
                    .cloned()
                    .unwrap_or(Value::Int32(0)),
                Aggregation::Max(_) => values
                    .iter()
                    .max_by(|a, b| a.cmp(b))
                    .cloned()
                    .unwrap_or(Value::Int32(0)),
            };
            results.push(result);
        }
        Ok(vec![results])
    }

    fn execute_join(
        &mut self,
        left_table: &str,
        right_table: &str,
        left_column: &str,
        right_column: &str,
        columns: &[String],
        condition: Option<Condition>,
    ) -> Result<Vec<Vec<Value>>, DbError> {
        let left_values = self
            .storage
            .lock()
            .unwrap()
            .read_column(left_table, left_column, condition.as_ref())?;
        let right_values = self
            .storage
            .lock()
            .unwrap()
            .read_column(right_table, right_column, condition.as_ref())?;

        let mut result = Vec::new();
        let mut column_values = HashMap::new();
        for col in columns {
            let (table, col_name) = if col.contains('.') {
                let parts = col.split('.').collect::<Vec<_>>();
                (parts[0], parts[1])
            } else {
                (left_table, col.as_str())
            };
            column_values.insert(
                col.clone(),
                self.storage.lock().unwrap().read_column(table, col_name, condition.as_ref())?,
            );
        }

        for (i, left_val) in left_values.iter().enumerate() {
            for (j, right_val) in right_values.iter().enumerate() {
                if left_val == right_val {
                    let row = columns
                        .iter()
                        .map(|col| column_values.get(col).unwrap()[if col.starts_with(right_table) { j } else { i }].clone())
                        .collect();
                    result.push(row);
                }
            }
        }
        Ok(result)
    }
}