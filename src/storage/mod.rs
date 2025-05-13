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

pub mod block;
pub mod buffer;
pub mod column;
pub mod compression;
pub mod index;

pub struct StorageManager {
    data_dir: String,
    columns: HashMap<String, HashMap<String, ColumnStore>>,
    indexes: HashMap<String, HashMap<String, Index>>,
    buffer: BufferManager,
    schema: Schema,
    next_segment_id: u64,
}

impl StorageManager {
    pub fn new(data_dir: &str, schema: Schema) -> Result<Self, DbError> {
        fs::create_dir_all(format!("{}/columns", data_dir))?;
        fs::create_dir_all(format!("{}/indexes", data_dir))?;
        let mut columns = HashMap::new();
        let mut indexes = HashMap::new();
        for table in schema.tables() {
            let mut table_cols = HashMap::new();
            let mut table_indexes = HashMap::new();
            for col in &table.columns {
                let segment_dir = format!("{}/columns/{}_{}", data_dir, table.name, col.name);
                fs::create_dir_all(&segment_dir)?;
                table_cols.insert(
                    col.name.clone(),
                    ColumnStore::new(col, &segment_dir, data_dir)?,
                );
                if col.name == "ID" || col.name == "Name" {
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
            buffer: BufferManager::new(100_000_000), // 100MB buffer
            schema,
            next_segment_id: 1,
        })
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn create_table(&mut self, table: &Table) -> Result<(), DbError> {
        let mut table_cols = HashMap::new();
        let mut table_indexes = HashMap::new();
        for col in &table.columns {
            let segment_dir = format!("{}/columns/{}_{}", self.data_dir, table.name, col.name);
            fs::create_dir_all(&segment_dir)?;
            table_cols.insert(
                col.name.clone(),
                ColumnStore::new(col, &segment_dir, &self.data_dir)?,
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
        let table_cols = self.columns.get_mut(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;
        let table_indexes = self.indexes.get_mut(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;
        let table_def = self.schema.get_table(table_name).ok_or_else(|| {
            DbError::InvalidData(format!("Table {} not found", table_name))
        })?;

        self.schema.validate_row(table_name, &row)?;

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        for (value, col) in row.into_iter().zip(table_def.columns.iter()) {
            let col_name = &col.name;
            let col_store = table_cols.get_mut(col_name).ok_or_else(|| {
                DbError::InvalidData(format!("Column {}.{} not found", table_name, col_name))
            })?;
            let compression = match value.data_type() {
                DataType::String => CompressionType::Dictionary,
                _ => CompressionType::Rle,
            };
            let segment_path = format!("{}/segment_{}.dat", col_store.segment_dir, segment_id);
            let offset = col_store.append_to_segment(&[value.clone()], compression, &segment_path)?;
            if let Some(index) = table_indexes.get_mut(col_name) {
                index.append(&[value], offset)?;
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
        let col_store = self
            .columns
            .get(table_name)
            .and_then(|cols| cols.get(column_name))
            .ok_or_else(|| {
                DbError::InvalidData(format!("Column {}.{} not found", table_name, column_name))
            })?;
        col_store.read(condition, &mut self.buffer)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}