use vddb::types::{DataType, Value, CompressionType, DbError};
use vddb::schema::{Schema, Column, metadata::BlockMetadata};

fn main() -> Result<(), DbError> {
    // // Initialize schema
    let data_dir = "data";
    let mut schema = Schema::new_schema(data_dir)?;

    // Create table
    let columns = vec![
        Column { name: "ID".to_string(), data_type: DataType::Int32 },
        Column { name: "Name".to_string(), data_type: DataType::String },
        Column { name: "Salary".to_string(), data_type: DataType::Float32 },
    ];
    schema.create_table("Employees".to_string(), columns)?;
    println!("Created table: {:?}", schema.get_table("Employees"));

    // Validate a row
    let row = vec![
        Value::Int32(1),
        Value::String("Alice".to_string()),
        Value::Float32(1000.0),
    ];
    schema.validate_row("Employees", &row)?;
    println!("Row validated successfully");

    // Test persistence
    schema.save()?;
    let loaded_schema = Schema::load(data_dir)?;
    println!("Loaded schema: {:?}", loaded_schema.get_table("Employees"));

    // Test block metadata
    let mut id_metadata = BlockMetadata::new_metadata("ID", DataType::Int32, data_dir);
    id_metadata.add_block(
        Value::Int32(1),
        Value::Int32(100),
        0,
        100,
        0,
        CompressionType::None,
    )?;
    id_metadata.add_block(
        Value::Int32(101),
        Value::Int32(200),
        400,
        100,
        0,
        CompressionType::Rle,
    )?;
    println!("Added blocks: {:?}", id_metadata.blocks);

    // Test block filtering
    // let condition = columnar_db::query::Condition::GreaterThan("ID".to_string(), Value::Int32(150));
    // let filtered_blocks = id_metadata.get_blocks(Some(&condition));
    // println!("Filtered blocks: {:?}", filtered_blocks);

    // Test metadata persistence
    id_metadata.save()?;
    let loaded_metadata = BlockMetadata::load("ID", DataType::Int32, data_dir)?;
    println!("Loaded metadata: {:?}", loaded_metadata.blocks);

    Ok(())
}