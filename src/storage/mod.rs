use crate::query::Condition;
use crate::schema::{Schema, Table};
use crate::types::{CompressionType, DbError, Value, DataType};
use std::collections::HashMap;
use std::fs;

pub mod block;
pub mod buffer;
pub mod column;
pub mod compression;

use buffer::BufferManager;
use column::ColumnStore;

pub struct StorageEngine {
    data_dir: String,
    pub columns: HashMap<String, HashMap<String, ColumnStore>>,
    buffer: BufferManager,
    pub schema: Schema,
    next_tx_id: u64,
}

#[derive(Clone)]
pub enum Operation {
    InsertRow { table_name: String, row: Vec<Value> },
    CreateTable { table: Table },
}

pub struct Transaction {
    id: u64,
    pub operations: Vec<Operation>,
}

impl StorageEngine {
    pub fn new(data_dir: &str, schema: Schema) -> Result<StorageEngine, DbError> {
        fs::create_dir_all(format!("{}/columns", data_dir))?;
        Ok(StorageEngine {
            data_dir: data_dir.to_string(),
            columns: HashMap::new(),
            buffer: BufferManager::new(100),
            schema,
            next_tx_id: 1,
        })
    }

    pub fn begin_transaction(&mut self) -> Transaction {
        let tx = Transaction {
            id: self.next_tx_id,
            operations: Vec::new(),
        };
        self.next_tx_id += 1;
        tx
    }

    pub fn commit_transaction(&mut self, tx: Transaction) -> Result<(), DbError> {
        for op in tx.operations {
            match op {
                Operation::InsertRow { table_name, row } => {
                    self.insert_row(&table_name, row)?;
                }
                Operation::CreateTable { table } => {
                    self.create_table(&table)?;
                }
            }
        }
        Ok(())
    }

    pub fn create_table(&mut self, table: &Table) -> Result<(), DbError> {
        let mut table_cols = HashMap::new();
        for col in &table.columns {
            let path = format!("{}/columns/{}_{}.dat", self.data_dir, table.name, col.name);
            table_cols.insert(
                col.name.clone(),
                ColumnStore::new(col, &path, &self.data_dir)?,
            );
        }
        self.columns.insert(table.name.clone(), table_cols);
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Vec<Value>) -> Result<(), DbError> {
        self.schema.validate_row(table_name, &row)?;
        let table_cols = self
            .columns
            .get_mut(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;
        if row.len() != table_cols.len() {
            return Err(DbError::InvalidData(format!(
                "Expected {} columns, got {}",
                table_cols.len(),
                row.len()
            )));
        }
        for (value, col_name) in row
            .into_iter()
            .zip(table_cols.keys().cloned().collect::<Vec<_>>())
        {
            let col = table_cols.get_mut(&col_name).unwrap();
            let compression = match value.data_type() {
                DataType::String => CompressionType::Dictionary,
                _ => CompressionType::Rle,
            };
            col.append(&[value], compression)?;
        }
        if let Some(table) = self.schema.get_table(table_name) {
            let mut table = table.clone();
            table.increment_row_count();
            self.schema.tables.insert(table_name.to_string(), table);
            self.schema.save()?;
        }
        Ok(())
    }

    pub fn read_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        condition: Option<&Condition>,
    ) -> Result<Vec<Value>, DbError> {
        let table_cols = self
            .columns
            .get(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;
        let col = table_cols
            .get(column_name)
            .ok_or_else(|| DbError::InvalidData(format!("Column {} not found", column_name)))?;
        let values = col.read(condition, &mut self.buffer)?;

        if let Some(cond) = condition {
            match cond {
                Condition::And(_, _) | Condition::Or(_, _) => {
                    let filtered = values
                        .into_iter()
                        .filter(|v| self.evaluate_condition(cond, column_name, v))
                        .collect();
                    Ok(filtered)
                }
                _ => Ok(values), // Simple conditions are handled by get_blocks
            }
        } else {
            Ok(values)
        }
    }

    fn evaluate_condition(&self, condition: &Condition, column_name: &str, value: &Value) -> bool {
        match condition {
            Condition::GreaterThan(col, val) if col == column_name => match (value, val) {
                (Value::Int32(v), Value::Int32(u)) => v > u,
                (Value::Float32(v), Value::Float32(u)) => v > u,
                (Value::String(v), Value::String(u)) => v > u,
                _ => false,
            },
            Condition::Equal(col, val) if col == column_name => value == val,
            Condition::LessThan(col, val) if col == column_name => match (value, val) {
                (Value::Int32(v), Value::Int32(u)) => v < u,
                (Value::Float32(v), Value::Float32(u)) => v < u,
                (Value::String(v), Value::String(u)) => v < u,
                _ => false,
            },
            Condition::And(left, right) => {
                self.evaluate_condition(left, column_name, value)
                    && self.evaluate_condition(right, column_name, value)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, column_name, value)
                    || self.evaluate_condition(right, column_name, value)
            }
            _ => true,
        }
    }
}