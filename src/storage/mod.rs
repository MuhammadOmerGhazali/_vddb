use crate::query::Condition;
use crate::schema::{Schema, Table};
use crate::types::{CompressionType, DataType, DbError, Value};
use std::collections::{HashMap, HashSet};
use std::fs;

pub mod block;
pub mod buffer;
pub mod column;
pub mod compression;
pub mod index;

use buffer::BufferManager;
use column::ColumnStore;
use index::Index;

pub struct StorageEngine {
    data_dir: String,
    pub columns: HashMap<String, HashMap<String, ColumnStore>>,
    pub indexes: HashMap<String, HashMap<String, Index>>,
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
    #[allow(dead_code)]
    id: u64,
    pub operations: Vec<Operation>,
}

impl StorageEngine {
    pub fn new(data_dir: &str, schema: Schema) -> Result<StorageEngine, DbError> {
        fs::create_dir_all(format!("{}/columns", data_dir))?;
        fs::create_dir_all(format!("{}/indexes", data_dir))?;
        Ok(StorageEngine {
            data_dir: data_dir.to_string(),
            columns: HashMap::new(),
            indexes: HashMap::new(),
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
        let mut table_indexes = HashMap::new();
        for col in &table.columns {
            // Define paths for data and metadata files
            let path = format!("{}/columns/{}_{}.dat", self.data_dir, table.name, col.name);
            let metadata_path = format!("{}/metadata/{}_{}.json", self.data_dir, table.name, col.name);
            
            // Delete existing files to reset the column state
            let _ = fs::remove_file(&path);        // Ignore error if file doesn't exist
            let _ = fs::remove_file(&metadata_path); // Ignore error if file doesn't exist
            
            // Create a new ColumnStore with fresh files
            table_cols.insert(
                col.name.clone(),
                ColumnStore::new(col, &path, &self.data_dir)?,
            );
            
            // Add indexes for specific columns
            if col.name == "ID" || col.name == "Name" {
                let index_path = format!("{}/indexes/{}_{}.idx", self.data_dir, table.name, col.name);
                table_indexes.insert(
                    col.name.clone(),
                    Index::new(&index_path, col.data_type.clone())?,
                );
            }
        }
        self.columns.insert(table.name.clone(), table_cols);
        self.indexes.insert(table.name.clone(), table_indexes);
        // Update schema with table name and columns
        self.schema.add_table(table.name.as_str(), table.columns.clone())?;
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Vec<Value>) -> Result<(), DbError> {
        self.schema.validate_row(table_name, &row)?;
        let table_cols = self
            .columns
            .get_mut(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;
        let table_indexes = self
            .indexes
            .get_mut(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;
        let table_def = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;

        if row.len() != table_def.columns.len() {
            return Err(DbError::InvalidData(format!(
                "Expected {} columns, got {}",
                table_def.columns.len(),
                row.len()
            )));
        }

        let row_offset = self.schema.get_table(table_name).unwrap().row_count as u64;
        for (value, col) in row.into_iter().zip(table_def.columns.iter()) {
            let col_name = &col.name;
            let col_store = table_cols.get_mut(col_name).unwrap();
            let compression = match value.data_type() {
                DataType::String => CompressionType::Dictionary,
                _ => CompressionType::Rle,
            };
            col_store.append(&[value.clone()], compression)?;
            if let Some(index) = table_indexes.get_mut(col_name) {
                index.append(&[value], row_offset)?;
            }
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
        // Collect all involved columns
        let mut involved_columns = HashSet::new();
        involved_columns.insert(column_name.to_string());
        if let Some(cond) = condition {
            involved_columns.extend(collect_condition_columns(cond));
        }

        // Fetch all column stores upfront
        let mut column_stores = HashMap::new();
        for col_name in &involved_columns {
            let col_store = self
                .columns
                .get(table_name)
                .and_then(|cols| cols.get(col_name))
                .ok_or_else(|| {
                    DbError::InvalidData(format!("Column {}.{} not found", table_name, col_name))
                })?
                .clone();
            column_stores.insert(col_name.clone(), col_store);
        }

        // Read all values for these columns
        let mut column_values = HashMap::new();
        for (col_name, col_store) in &column_stores {
            let values = col_store.read(None, &mut self.buffer)?;
            column_values.insert(col_name.clone(), values);
        }

        // Apply condition logic
        let target_values = column_values.get(column_name).unwrap().clone();
        let filtered_values = if let Some(cond) = condition {
            let mut mask = vec![true; target_values.len()];
            apply_condition_mask(cond, &column_values, &mut mask);
            target_values
                .into_iter()
                .enumerate()
                .filter(|(i, _)| mask[*i])
                .map(|(_, v)| v)
                .collect()
        } else {
            target_values
        };

        Ok(filtered_values)
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
            },
            Condition::Or(left, right) => {
                self.evaluate_condition(left, column_name, value)
                    || self.evaluate_condition(right, column_name, value)
            },
            _ => false,
        }
    }
}

fn collect_condition_columns(condition: &Condition) -> HashSet<String> {
    let mut columns = HashSet::new();
    match condition {
        Condition::Equal(col, _) | Condition::GreaterThan(col, _) | Condition::LessThan(col, _) => {
            columns.insert(col.clone());
        }
        Condition::And(left, right) | Condition::Or(left, right) => {
            columns.extend(collect_condition_columns(left));
            columns.extend(collect_condition_columns(right));
        }
    }
    columns
}

fn apply_condition_mask(
    cond: &Condition,
    column_values: &HashMap<String, Vec<Value>>,
    mask: &mut [bool],
) {
    match cond {
        Condition::Equal(col, val) => {
            let values = column_values.get(col).unwrap();
            for (i, v) in values.iter().enumerate() {
                mask[i] &= v == val;
            }
        }
        Condition::GreaterThan(col, val) => {
            let values = column_values.get(col).unwrap();
            for (i, v) in values.iter().enumerate() {
                mask[i] &= match (v, val) {
                    (Value::Int32(a), Value::Int32(b)) => a > b,
                    (Value::Float32(a), Value::Float32(b)) => a > b,
                    (Value::String(a), Value::String(b)) => a > b,
                    _ => false,
                };
            }
        }
        Condition::LessThan(col, val) => {
            let values = column_values.get(col).unwrap();
            for (i, v) in values.iter().enumerate() {
                mask[i] &= match (v, val) {
                    (Value::Int32(a), Value::Int32(b)) => a < b,
                    (Value::Float32(a), Value::Float32(b)) => a < b,
                    (Value::String(a), Value::String(b)) => a < b,
                    _ => false,
                };
            }
        }
        Condition::And(left, right) => {
            apply_condition_mask(left, column_values, mask);
            apply_condition_mask(right, column_values, mask);
        }
        Condition::Or(left, right) => {
            let mut left_mask = mask.to_vec();
            let mut right_mask = mask.to_vec();
            apply_condition_mask(left, column_values, &mut left_mask);
            apply_condition_mask(right, column_values, &mut right_mask);
            for (i, (l, r)) in left_mask.iter().zip(right_mask.iter()).enumerate() {
                mask[i] = *l || *r;
            }
        }
    }
}