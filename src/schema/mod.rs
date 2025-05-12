use crate::types::{DbError, DataType, Value};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::Path};

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
    pub row_count : u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaterializedView {
    pub name: String,
    pub query: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    tables: HashMap<String, Table>,
    // views: HashMap<String, MaterializedView>,
    data_dir : String,
}

impl Schema {
    pub fn new_schema(data_dir : &str) -> Result<Schema,DbError>{
        fs::create_dir_all(data_dir)?;
        Ok(Schema{
            tables :HashMap::new(),
            // views: HashMap::new(),
            data_dir : data_dir.to_string(),
        })
    }
    pub fn add_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::InvalidData(format!("Table {} already exists", name)));
        }
        self.tables.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                columns,
                row_count : 0,
            },
        );
        Ok(())
    }
    pub fn create_table(&mut self,name :String ,columns :Vec<Column>) ->Result<(),DbError>{
        if self.tables.contains_key(&name){
            return Err(DbError::InvalidData(format!("Table {} already exists.",name)));
        }
        if columns.is_empty(){
            return Err(DbError::InvalidData(("Table must have at least one column").to_string()));
        }
        for col in &columns{
            if col.name.is_empty(){
                return Err(DbError::InvalidData(("Column name cannot be empty.").to_string()));
            }
        }
        self.tables.insert(name.clone(), Table { name, columns, row_count: 0});
        self.save()?;
        Ok(())
    }

    // pub fn add_materialized_view(&mut self, name: &str, query: &str, table: &str) -> Result<(), DbError> {
    //     if self.views.contains_key(name) {
    //         return Err(DbError::InvalidData(format!("View {} already exists", name)));
    //     }
    //     self.views.insert(
    //         name.to_string(),
    //         MaterializedView {
    //             name: name.to_string(),
    //             query: query.to_string(),
    //             table: table.to_string(),
    //         },
    //     );
    //     Ok(())
    // }
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    // pub fn get_view(&self, name: &str) -> Option<&MaterializedView> {
    //     self.views.get(name)
    // }
    pub fn validate_row(&self, table: &str, values: &[Value]) -> Result<(), DbError> {
        let table_def = self.get_table(table)
            .ok_or_else(|| DbError::InvalidData(format!("Table {} not found", table)))?;
        if values.len() != table_def.columns.len() {
            return Err(DbError::InvalidData("Mismatched column count".to_string()));
        }
        for (value, col) in values.iter().zip(table_def.columns.iter()) {
            match (value, &col.data_type) {
                (Value::Int32(_), DataType::Int32) |
                (Value::Float32(_), DataType::Float32) |
                (Value::String(_), DataType::String) => {}
                _ => return Err(DbError::InvalidData(format!("Type mismatch for column {}", col.name))),
            }
        }
        Ok(())
    }
    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }
    pub fn save(&self) ->Result<(),DbError>{
        let path =format!("{}/schema.json",self.data_dir);
        let json = serde_json::to_string_pretty(&self.tables)?;
        fs::write(&path, json)?;
        Ok(())
    }
    pub fn load(data_dir : &str) -> Result<Schema,DbError>{
        let path = format!("{}/schema.json", data_dir);
        if !Path::new(&path).exists(){
            return Ok(Schema::new_schema(data_dir)?);
        }
        let json = fs::read_to_string(&path)?;
        let tables:HashMap<String,Table> = serde_json::from_str(&json)?;
        // let views:HashMap<String, MaterializedView> = serde_json::from_str(&json)?;
        Ok(Schema { 
            tables,
            // views,
            data_dir: data_dir.to_string()
            })
    }
}



// ================================= ADD UNIT TESTS HERE LATER =====================================
