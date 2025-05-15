use crate::types::{DataType, DbError, Value};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub mod metadata;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub row_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
    pub data_dir: String,
}

impl Schema {
    pub fn new_schema(data_dir: &str) -> Result<Self, DbError> {
        if Path::new(data_dir).exists() {
            Self::load(data_dir)
        } else {
            fs::create_dir_all(data_dir)?;
            Ok(Schema {
                tables: HashMap::new(),
                data_dir: data_dir.to_string(),
            })
        }
    }

    pub fn add_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::InvalidData(format!(
                "Table {} already exists",
                name
            )));
        }
        if columns.is_empty() {
            return Err(DbError::InvalidData(
                "Table must have at least one column".to_string(),
            ));
        }
        for col in &columns {
            if col.name.is_empty() {
                return Err(DbError::InvalidData(
                    "Column name cannot be empty".to_string(),
                ));
            }
        }
        self.tables.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                columns,
                row_count: 0,
            },
        );
        self.save()?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn validate_row(&self, table: &str, values: &[Value]) -> Result<(), DbError> {
        let table_def = self
            .get_table(table)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table)))?;

        if values.len() != table_def.columns.len() {
            return Err(DbError::InvalidData(format!(
                "Expected {} columns, got {}",
                table_def.columns.len(),
                values.len()
            )));
        }

        for (value, col) in values.iter().zip(table_def.columns.iter()) {
            if value.data_type() != col.data_type {
                return Err(DbError::TypeMismatch);
            }
        }

        Ok(())
    }

    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }

    pub fn save(&self) -> Result<(), DbError> {
        let path = format!("{}/schema.json", self.data_dir);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)?;
        file.lock_exclusive()?;
        let json = serde_json::to_string_pretty(&self.tables)
            .map_err(|e| DbError::SerializationError(e.to_string()))?;
        fs::write(&path, json)?;
        fs2::FileExt::unlock(&file)?;
        Ok(())
    }

    pub fn load(data_dir: &str) -> Result<Self, DbError> {
        let path = format!("{}/schema.json", data_dir);
        if !std::path::Path::new(&path).exists() {
            return Self::new_schema(data_dir);
        }
        let json = fs::read_to_string(&path)?;
        let tables: HashMap<String, Table> =
            serde_json::from_str(&json).map_err(|e| DbError::SerializationError(e.to_string()))?;
        Ok(Schema {
            tables,
            data_dir: data_dir.to_string(),
        })
    }
}

impl Table {
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn increment_row_count(&mut self) {
        self.row_count += 1;
    }
}
