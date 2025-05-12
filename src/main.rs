use ordered_float::OrderedFloat;
use vddb::query::Condition;
use vddb::schema::{Column, Schema, Table};
use vddb::storage::{Operation, StorageEngine};
use vddb::types::{DataType, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = "data";
    let mut schema = Schema::new_schema(data_dir)?;
    let columns = vec![
        Column {
            name: "ID".to_string(),
            data_type: DataType::Int32,
        },
        Column {
            name: "Name".to_string(),
            data_type: DataType::String,
        },
        Column {
            name: "Salary".to_string(),
            data_type: DataType::Float32,
        },
    ];
    // Create the table object but don't call schema.create_table directly
    let table = Table {
        name: "Employees".to_string(),
        columns,
        row_count: 0,
    };
    println!("Created table: {:?}", &table);

    let mut engine = StorageEngine::new(data_dir, schema)?;
    let mut tx = engine.begin_transaction();
    tx.operations.push(Operation::CreateTable { table });
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
    tx.operations.push(Operation::InsertRow {
        table_name: "Employees".to_string(),
        row: vec![
            Value::Int32(4),
            Value::String("Alice".to_string()),
            Value::Float32(OrderedFloat(1200.0)),
        ],
    });
    engine.commit_transaction(tx)?;
    println!("Inserted 4 rows via transaction");

    let table = engine.schema.get_table("Employees").unwrap();
    println!("Table row count: {}", table.row_count);

    let names = engine.read_column("Employees", "Name", None)?;
    println!("All Names: {:?}", names);

    let salaries = engine.read_column(
        "Employees",
        "Salary",
        Some(&Condition::GreaterThan("ID".to_string(), Value::Int32(1))),
    )?;
    println!("Salaries where ID > 1: {:?}", salaries);

    let salary_alice = engine.read_column(
        "Employees",
        "Salary",
        Some(&Condition::Equal("Name".to_string(), Value::String("Alice".to_string()))),
    )?;
    println!("Salary where Name = Alice: {:?}", salary_alice);

    // Handle multi-column condition using row positions
    let ids = engine.read_column(
        "Employees",
        "ID",
        Some(&Condition::GreaterThan("ID".to_string(), Value::Int32(1))),
    )?;
    let salaries = engine.read_column(
        "Employees",
        "Salary",
        Some(&Condition::LessThan("Salary".to_string(), Value::Float32(OrderedFloat(2000.0)))),
    )?;
    let names_all = engine.read_column("Employees", "Name", None)?;
    let mut filtered_names = Vec::new();
    let mut seen_positions = std::collections::HashSet::new();
    for (i, id) in ids.iter().enumerate() {
        if let Value::Int32(id_val) = id {
            if *id_val > 1 {
                if let Some(salary) = salaries.get(i) {
                    if let Value::Float32(sal_val) = salary {
                        if *sal_val < OrderedFloat(2000.0) {
                            if let Some(name) = names_all.get(i) {
                                if seen_positions.insert(i) {
                                    filtered_names.push(name.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    println!("Names where ID > 1 AND Salary < 2000: {:?}", filtered_names);

    Ok(())
}