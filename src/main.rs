use vddb::types::{DataType, Value, DbError};
use vddb::schema::{Schema, Column};
use vddb::storage::{StorageEngine, Operation};
use vddb::query::Condition;
use ordered_float::OrderedFloat;

fn main() -> Result<(), DbError> {
    // Initialize schema and storage
    let data_dir = "data";
    let schema = Schema::new_schema(data_dir)?;
    let mut storage = StorageEngine::new(data_dir, schema)?;

    // Create table
    let columns = vec![
        Column { name: "ID".to_string(), data_type: DataType::Int32 },
        Column { name: "Name".to_string(), data_type: DataType::String },
        Column { name: "Salary".to_string(), data_type: DataType::Float32 },
    ];
    let table = storage.schema.create_table("Employees".to_string(), columns)?;
    storage.create_table(&table)?;
    println!("Created table: {:?}", storage.schema.get_table("Employees"));

    // Test transaction: Insert multiple rows atomically
    let mut tx = storage.begin_transaction();
    tx.operations.push(Operation::InsertRow {
        table_name: "Employees".to_string(),
        row: vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::Float32(OrderedFloat(1000.0)),
        ],
    });
    tx.operations.push(Operation::InsertRow {
        table_name: "Employees".to_string(),
        row: vec![
            Value::Int32(2),
            Value::String("Bob".to_string()),
            Value::Float32(OrderedFloat(2000.0)),
        ],
    });
    tx.operations.push(Operation::InsertRow {
        table_name: "Employees".to_string(),
        row: vec![
            Value::Int32(3),
            Value::String("Charlie".to_string()),
            Value::Float32(OrderedFloat(1500.0)),
        ],
    });
    storage.commit_transaction(tx)?;
    println!("Inserted 3 rows via transaction");

    // Verify row count
    if let Some(table) = storage.schema.get_table("Employees") {
        println!("Table row count: {}", table.row_count);
    }

    // Test reading entire column (no condition)
    let all_names = storage.read_column("Employees", "Name", None)?;
    println!("All Names: {:?}", all_names);

    // Test reading with GreaterThan condition
    let condition = Condition::GreaterThan("ID".to_string(), Value::Int32(1));
    let filtered_salaries = storage.read_column("Employees", "Salary", Some(&condition))?;
    println!("Salaries where ID > 1: {:?}", filtered_salaries);

    // Test reading with Equal condition
    let condition = Condition::Equal("Name".to_string(), Value::String("Alice".to_string()));
    let alice_salary = storage.read_column("Employees", "Salary", Some(&condition))?;
    println!("Salary where Name = Alice: {:?}", alice_salary);

    // Test reading with combined condition (And)
    let condition = Condition::And(
        Box::new(Condition::GreaterThan("ID".to_string(), Value::Int32(1))),
        Box::new(Condition::LessThan("Salary".to_string(), Value::Float32(OrderedFloat(2000.0)))),
    );
    let combined_filter = storage.read_column("Employees", "Name", Some(&condition))?;
    println!("Names where ID > 1 AND Salary < 2000: {:?}", combined_filter);

    Ok(())
}