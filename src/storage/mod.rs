use crate::schema::{Schema, Table};
use crate::storage::{
    buffer::BufferManager,
    column::ColumnStore,
    index::Index,
};
use crate::types::{CompressionType, DbError, Value};
use crate::{Condition, DataType};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub mod block;
pub mod buffer;
pub mod column;
pub mod compression;
pub mod index;

// Standalone function to flush pending rows
fn do_flush_pending_rows(
    pending_rows: &mut HashMap<String, HashMap<String, Vec<Value>>>,
    table_name: &str,
    table_cols: &mut HashMap<String, ColumnStore>,
    table_indexes: &mut HashMap<String, Index>,
    table_def: &Table,
) -> Result<(), DbError> {
    let table_pending = pending_rows.remove(table_name).unwrap_or_default();
    for col in &table_def.columns {
        let col_name = &col.name;
        let col_store = table_cols.get_mut(col_name).ok_or_else(|| {
            DbError::InvalidData(format!("Column {}.{} not found", table_name, col_name))
        })?;
        let values = table_pending.get(col_name).cloned().unwrap_or_default();
        if !values.is_empty() {
            let compression = match col.data_type {
                DataType::String => CompressionType::Dictionary,
                _ => CompressionType::Rle,
            };
            let offset = col_store.append(&values, compression)?;
            if let Some(index) = table_indexes.get_mut(col_name) {
                index.append(&values, offset)?;
            }
        }
    }
    Ok(())
}

pub struct StorageManager {
    data_dir: String,
    pub columns: HashMap<String, HashMap<String, ColumnStore>>,
    pub indexes: HashMap<String, HashMap<String, Index>>,
    pub buffer: BufferManager,
    schema: Schema,
    pending_rows: HashMap<String, HashMap<String, Vec<Value>>>,
    max_rows_per_segment: usize,
}

impl StorageManager {
    pub fn new(data_dir: &str, schema: Schema) -> Result<Self, DbError> {
        fs::create_dir_all(format!("{}/columns", data_dir))?;
        fs::create_dir_all(format!("{}/indexes", data_dir))?;
        fs::create_dir_all(format!("{}/metadata", data_dir))?;
        let mut columns = HashMap::new();
        let mut indexes = HashMap::new();
        for table in schema.tables() {
            let mut table_cols = HashMap::new();
            let mut table_indexes = HashMap::new();
            for col in &table.columns {
                table_cols.insert(
                    col.name.clone(),
                    ColumnStore::new(col, data_dir)?,
                );
                if col.name == "ID"{
                    let index_path = format!("{}/indexes/{}_{}.idx", data_dir, table.name, col.name);
                    table_indexes.insert(
                        col.name.clone(),
                        Index::new(&index_path, col.data_type.clone())?,
                    );
                }
            }
            columns.insert(table.name.clone(), table_cols);
            indexes.insert(table.name.clone(), table_indexes);
        }
        Ok(StorageManager {
            data_dir: data_dir.to_string(),
            columns,
            indexes,
            buffer: BufferManager::new(100_000_000),
            schema,
            pending_rows: HashMap::new(),
            max_rows_per_segment: 3, // Increased for batching
        })
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn create_table(&mut self, table: &Table) -> Result<(), DbError> {
        let mut table_cols = HashMap::new();
        let mut table_indexes = HashMap::new();
        for col in &table.columns {
            table_cols.insert(
                col.name.clone(),
                ColumnStore::new(col, &self.data_dir)?,
            );
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
        self.schema.add_table(&table.name, table.columns.clone())?;
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Vec<Value>) -> Result<(), DbError> {
        // Validate and get references
        let table_def = self.schema.get_table(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?.clone();
        self.schema.validate_row(table_name, &row)?;

        // Check for duplicate ID
        {
            let table_indexes = self.indexes.get_mut(table_name).ok_or_else(|| {
                DbError::InvalidData(format!("Table {} not found", table_name))
            })?;
            if let Some(id_index) = table_indexes.get("ID") {
                let id_value = &row[0];
                let existing = id_index.lookup(id_value)?;
                if !existing.is_empty() {
                    return Err(DbError::InvalidData(format!("Duplicate ID: {:?}", id_value)));
                }
            }
        }

        // Buffer the row
        let table_pending = self.pending_rows.entry(table_name.to_string()).or_insert_with(HashMap::new);
        for (value, col) in row.into_iter().zip(table_def.columns.iter()) {
            let col_name = &col.name;
            let col_values = table_pending.entry(col_name.clone()).or_insert_with(Vec::new);
            col_values.push(value);
        }

        // Flush if buffer is full
        if table_pending.values().next().map_or(0, |v| v.len()) >= self.max_rows_per_segment {
            let mut table_cols = self.columns.get_mut(table_name).ok_or_else(|| {
                DbError::InvalidData(format!("Table {} not found", table_name))
            })?;
            let mut table_indexes = self.indexes.get_mut(table_name).ok_or_else(|| {
                DbError::InvalidData(format!("Table {} not found", table_name))
            })?;
            do_flush_pending_rows(
                &mut self.pending_rows,
                table_name,
                &mut table_cols,
                &mut table_indexes,
                &table_def,
            )?;
        }

        // Increment row count
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
        let col_store = self
            .columns
            .get(table_name)
            .and_then(|cols| cols.get(column_name))
            .ok_or_else(|| {
                DbError::InvalidData(format!("Column {}.{} not found", table_name, column_name))
            })?;
        let mut values = col_store.read(condition, &mut self.buffer)?;

        // Append pending rows
        if let Some(table_pending) = self.pending_rows.get(table_name) {
            if let Some(pending_values) = table_pending.get(column_name) {
                values.extend(pending_values.iter().cloned());
            }
        }

        Ok(values)
    }

    pub fn delete_rows(&mut self, table_name: &str, condition: Option<&Condition>) -> Result<(), DbError> {
        let table_def = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table_name)))?;
        let columns = table_def.columns.clone();

        let mut column_values = HashMap::new();
        let mut min_row_count = usize::MAX;
        for col in &columns {
            let values = self.read_column(table_name, &col.name, None)?;
            min_row_count = min_row_count.min(values.len());
            column_values.insert(col.name.clone(), values);
        }

        let keep_indices = match condition {
            Some(cond) => {
                let cond_columns = crate::query::collect_condition_columns(cond);
                for col in cond_columns {
                    if !column_values.contains_key(&col) {
                        let values = self.read_column(table_name, &col, None)?;
                        min_row_count = min_row_count.min(values.len());
                        column_values.insert(col, values);
                    }
                }
                let mut indices = Vec::new();
                for i in 0..min_row_count {
                    if !crate::query::evaluator::evaluate_condition_row(cond, &column_values, i)? {
                        indices.push(i);
                    }
                }
                indices
            }
            None => {
                let table_cols = self.columns.get_mut(table_name).ok_or_else(|| {
                    DbError::InvalidData(format!("Table {} not found", table_name))
                })?;
                let table_indexes = self.indexes.get_mut(table_name).ok_or_else(|| {
                    DbError::InvalidData(format!("Table {} not found", table_name))
                })?;
                for col in &columns {
                    let col_store = table_cols.get_mut(&col.name).unwrap();
                    col_store.clear()?;
                    if let Some(index) = table_indexes.get_mut(&col.name) {
                        index.clear()?;
                    }
                }
                self.pending_rows.remove(table_name);
                if let Some(table) = self.schema.get_table(table_name) {
                    let mut table = table.clone();
                    table.row_count = 0;
                    self.schema.tables.insert(table_name.to_string(), table);
                    self.schema.save()?;
                }
                return Ok(());
            }
        };

        let table_cols = self.columns.get_mut(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;
        let table_indexes = self.indexes.get_mut(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;

        for col in &columns {
            let col_store = table_cols.get_mut(&col.name).unwrap();
            let values = column_values
                .get(&col.name)
                .cloned()
                .unwrap_or_else(|| col_store.read(None, &mut self.buffer).unwrap_or_default());
            let filtered_values: Vec<Value> = keep_indices
                .iter()
                .filter(|&&i| i < values.len())
                .map(|&i| values[i].clone())
                .collect();
            col_store.clear()?;
            if !filtered_values.is_empty() {
                let compression = match col.data_type {
                    DataType::String => CompressionType::Dictionary,
                    _ => CompressionType::Rle,
                };
                col_store.append(&filtered_values, compression)?;
            }
            if let Some(index) = table_indexes.get_mut(&col.name) {
                index.clear()?;
                if !filtered_values.is_empty() {
                    index.append(&filtered_values, 0)?;
                }
            }
        }
        self.pending_rows.remove(table_name);

        if let Some(table) = self.schema.get_table(table_name) {
            let mut table = table.clone();
            table.row_count = keep_indices.len() as u64;
            self.schema.tables.insert(table_name.to_string(), table);
            self.schema.save()?;
        }
        Ok(())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if !self.schema.tables.contains_key(table_name) {
            return Err(DbError::InvalidData(format!("Table {} not found", table_name)));
        }

        let table_cols = self.columns.remove(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;
        for (col_name, _) in table_cols {
            let file_path = format!("{}/columns/{}.dat", self.data_dir, col_name);
            if Path::new(&file_path).exists() {
                fs::remove_file(&file_path)?;
            }
        }

        let table_indexes = self.indexes.remove(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;
        for (col_name, _) in table_indexes {
            let index_path = format!("{}/indexes/{}_{}.idx", self.data_dir, table_name, col_name);
            if Path::new(&index_path).exists() {
                fs::remove_file(&index_path)?;
            }
        }

        let metadata_dir = format!("{}/metadata", self.data_dir);
        for col in self.schema.get_table(table_name).unwrap().columns.iter() {
            let metadata_path = format!("{}/{}.json", metadata_dir, col.name);
            if Path::new(&metadata_path).exists() {
                fs::remove_file(&metadata_path)?;
            }
        }

        self.pending_rows.remove(table_name);
        self.schema.tables.remove(table_name);
        self.schema.save()?;
        Ok(())
    }
}