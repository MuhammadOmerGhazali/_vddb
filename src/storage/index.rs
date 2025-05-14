use crate::types::{DataType, DbError, Value};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use bincode;

pub struct Index {
    path: String,
    data_type: DataType,
    map: BTreeMap<Value, Vec<u64>>,
}

impl Index {
    pub fn new(path: &str, data_type: DataType) -> Result<Self, DbError> {
        let mut index = Index {
            path: path.to_string(),
            data_type,
            map: BTreeMap::new(),
        };
        if std::path::Path::new(path).exists() {
            index.load()?;
        }
        Ok(index)
    }

    pub fn append(&mut self, values: &[Value], offset: u64) -> Result<(), DbError> {
        for value in values {
            if value.data_type() != self.data_type {
                return Err(DbError::TypeMismatch);
            }
            self.map
                .entry(value.clone())
                .or_insert_with(Vec::new)
                .push(offset);
        }
        self.save()?;
        Ok(())
    }

    pub fn lookup(&self, value: &Value) -> Result<Vec<u64>, DbError> {
        if value.data_type() != self.data_type {
            return Err(DbError::TypeMismatch);
        }
        Ok(self.map.get(value).cloned().unwrap_or_default())
    }

    pub fn range_lookup(&self, min: &Value, max: &Value) -> Result<Vec<u64>, DbError> {
        if min.data_type() != self.data_type || max.data_type() != self.data_type {
            return Err(DbError::TypeMismatch);
        }
        let mut offsets = Vec::new();
        for (_value, offs) in self.map.range(min..=max) {
            offsets.extend(offs);
        }
        Ok(offsets)
    }

    pub fn clear(&mut self) -> Result<(), DbError> {
        // Clear the in-memory map
        self.map.clear();

        // Truncate the index file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        file.flush()?;
        Ok(())
    }

    fn save(&self) -> Result<(), DbError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        let serialized = bincode::serialize(&self.map)
            .map_err(|e| DbError::SerializationError(e.to_string()))?;
        file.write_all(&serialized)?;
        file.flush()?;
        Ok(())
    }

    fn load(&mut self) -> Result<(), DbError> {
        let mut file = File::open(&self.path)?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        if !contents.is_empty() {
            self.map = bincode::deserialize(&contents)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;
        }
        Ok(())
    }
}