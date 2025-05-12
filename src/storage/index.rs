use crate::types::{Value, DbError, DataType};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::collections::HashMap;

pub struct Index {
    file: File,
    #[allow(dead_code)]
    path: String,
    data_type: DataType,
    cache: HashMap<Value, Vec<u64>>,
}

impl Index {
    pub fn new(path: &str, data_type: DataType) -> Result<Index, DbError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut index = Index {
            file,
            path: path.to_string(),
            data_type,
            cache: HashMap::new(),
        };
        index.load()?;
        Ok(index)
    }

    pub fn append(&mut self, values: &[Value], block_offset: u64) -> Result<(), DbError> {
        for value in values {
            if value.data_type() != self.data_type {
                return Err(DbError::TypeMismatch);
            }
            let offsets = self.cache.entry(value.clone()).or_insert_with(Vec::new);
            if !offsets.contains(&block_offset) {
                offsets.push(block_offset);
                println!("DEBUG: Index append for value {:?} at offset {}", value, block_offset);
            }
        }
        self.save()?;
        Ok(())
    }

    pub fn lookup(&self, value: &Value) -> Result<Vec<u64>, DbError> {
        if value.data_type() != self.data_type {
            return Err(DbError::TypeMismatch);
        }
        let offsets = self.cache.get(value).cloned().unwrap_or_default();
        println!("DEBUG: Index lookup for value {:?}: offsets {:?}", value, offsets);
        Ok(offsets)
    }

    fn save(&mut self) -> Result<(), DbError> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_u32::<LittleEndian>(self.cache.len() as u32)?;
        for (value, offsets) in &self.cache {
            let value_bytes = value.serialize();
            self.file.write_u32::<LittleEndian>(value_bytes.len() as u32)?;
            self.file.write_all(&value_bytes)?;
            self.file.write_u32::<LittleEndian>(offsets.len() as u32)?;
            for offset in offsets {
                self.file.write_u64::<LittleEndian>(*offset)?;
            }
        }
        self.file.flush().map_err(|e| DbError::IoError(e))?;
        Ok(())
    }

    fn load(&mut self) -> Result<(), DbError> {
        self.cache.clear();
        self.file.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![];
        self.file.read_to_end(&mut buffer)?;
        if buffer.is_empty() {
            return Ok(());
        }
        let mut cursor = std::io::Cursor::new(buffer);
        let num_entries = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;
        for _ in 0..num_entries {
            let value_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut value_bytes = vec![0u8; value_len];
            cursor.read_exact(&mut value_bytes)?;
            let value = Value::deserialize(&self.data_type, &value_bytes)?;
            let num_offsets = cursor.read_u32::<LittleEndian>()? as usize;
            let mut offsets = Vec::with_capacity(num_offsets);
            for _ in 0..num_offsets {
                offsets.push(cursor.read_u64::<LittleEndian>()?);
            }
            self.cache.insert(value, offsets);
        }
        Ok(())
    }
}