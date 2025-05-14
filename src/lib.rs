pub mod query;
pub mod repl;
pub mod schema;
pub mod storage;
pub mod transaction;
pub mod types;

pub use query::{Aggregation, Condition, Query};
pub use repl::Repl;
pub use schema::{Column, Schema, Table};
use std::sync::{Arc, Mutex};
pub use storage::StorageManager;
pub use transaction::{Transaction, TransactionManager};
pub use types::{CompressionType, DataType, DbError, Value};

pub fn create_database(
    data_dir: &str,
) -> Result<(Schema, Arc<Mutex<StorageManager>>, TransactionManager), DbError> {
    let schema = Schema::new_schema(data_dir)?;
    let storage = Arc::new(Mutex::new(StorageManager::new(data_dir, schema.clone())?));
    let tx_manager = TransactionManager::new(storage.clone())?;
    Ok((schema, storage, tx_manager))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;
    use rand::distributions::{Alphanumeric, DistString};
    use std::fs;

    fn setup_test_db(
        test_name: &str,
    ) -> Result<
        (
            String,
            Schema,
            Arc<Mutex<StorageManager>>,
            TransactionManager,
        ),
        DbError,
    > {
        let random_suffix = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
        let data_dir = format!("test_data_{}_{}", test_name, random_suffix);
        let (schema, storage, tx_manager) = create_database(&data_dir)?;
        Ok((data_dir, schema, storage, tx_manager))
    }

    fn cleanup_test_db(data_dir: &String) {
        if fs::metadata(data_dir).is_ok() {
            fs::remove_dir_all(data_dir).unwrap();
        }
    }

    #[test]
    fn test_wal_directory_creation() {
        let (data_dir, _schema, _storage, _tx_manager) = setup_test_db("wal_creation").unwrap();
        assert!(fs::metadata(format!("{}/wal", data_dir)).is_ok());
        assert!(fs::metadata(format!("{}/wal/wal.log", data_dir)).is_ok());
        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_create_table() {
        let (data_dir, _schema, storage, mut tx_manager) = setup_test_db("create_table").unwrap();
        let query = Query::CreateTable {
            table: "Employees".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Name".to_string(), DataType::String),
                ("Salary".to_string(), DataType::Float32),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(query);
        tx_manager.commit_transaction(tx).unwrap();

        let storage_guard = storage.lock().unwrap();
        let schema = storage_guard.schema();
        assert!(schema.get_table("Employees").is_some());
        let table = schema.get_table("Employees").unwrap();
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.columns[0].name, "ID");
        assert_eq!(table.columns[0].data_type, DataType::Int32);
        assert_eq!(table.columns[1].name, "Name");
        assert_eq!(table.columns[1].data_type, DataType::String);
        assert_eq!(table.columns[2].name, "Salary");
        assert_eq!(table.columns[2].data_type, DataType::Float32);

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_insert_and_select() {
        let (data_dir, _schema, _storage, mut tx_manager) = setup_test_db("insert_select").unwrap();
        let create_query = Query::CreateTable {
            table: "Employees".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Name".to_string(), DataType::String),
                ("Salary".to_string(), DataType::Float32),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        let insert_query = Query::Insert {
            table: "Employees".to_string(),
            values: vec![
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Float32(OrderedFloat(1000.0)),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        tx_manager.commit_transaction(tx).unwrap();

        let select_query = Query::Select {
            table: "Employees".to_string(),
            columns: vec!["Name".to_string(), "Salary".to_string()],
            condition: Some(Condition::Equal("ID".to_string(), Value::Int32(1))),
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query);
        let results = tx_manager.commit_transaction(tx).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec![
                Value::String("Alice".to_string()),
                Value::Float32(OrderedFloat(1000.0))
            ]
        );

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_aggregation() {
        let (data_dir, _schema, _storage, mut tx_manager) = setup_test_db("aggregation").unwrap();
        let create_query = Query::CreateTable {
            table: "Sales".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Amount".to_string(), DataType::Float32),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        let inserts = vec![
            Query::Insert {
                table: "Sales".to_string(),
                values: vec![Value::Int32(1), Value::Float32(OrderedFloat(100.0))],
            },
            Query::Insert {
                table: "Sales".to_string(),
                values: vec![Value::Int32(2), Value::Float32(OrderedFloat(200.0))],
            },
            Query::Insert {
                table: "Sales".to_string(),
                values: vec![Value::Int32(3), Value::Float32(OrderedFloat(300.0))],
            },
        ];
        let mut tx = tx_manager.begin_transaction();
        for insert in inserts {
            tx.add_query(insert);
        }
        tx_manager.commit_transaction(tx).unwrap();

        let agg_query = Query::SelectAggregate {
            table: "Sales".to_string(),
            aggregations: vec![
                Aggregation::Count,
                Aggregation::Sum("Amount".to_string()),
                Aggregation::Avg("Amount".to_string()),
                Aggregation::Min("Amount".to_string()),
                Aggregation::Max("Amount".to_string()),
            ],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(agg_query);
        let results = tx_manager.commit_transaction(tx).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec![
                Value::Int32(3),
                Value::Float32(OrderedFloat(600.0)),
                Value::Float32(OrderedFloat(200.0)),
                Value::Float32(OrderedFloat(100.0)),
                Value::Float32(OrderedFloat(300.0)),
            ]
        );

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_join() {
        let (data_dir, _schema, _storage, mut tx_manager) = setup_test_db("join").unwrap();
        let create_employees = Query::CreateTable {
            table: "Employees".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Name".to_string(), DataType::String),
            ],
        };
        let create_departments = Query::CreateTable {
            table: "Departments".to_string(),
            columns: vec![
                ("DeptID".to_string(), DataType::Int32),
                ("DeptName".to_string(), DataType::String),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_employees);
        tx.add_query(create_departments);
        tx_manager.commit_transaction(tx).unwrap();

        let insert_employees = vec![
            Query::Insert {
                table: "Employees".to_string(),
                values: vec![Value::Int32(1), Value::String("Alice".to_string())],
            },
            Query::Insert {
                table: "Employees".to_string(),
                values: vec![Value::Int32(2), Value::String("Bob".to_string())],
            },
        ];
        let insert_departments = vec![
            Query::Insert {
                table: "Departments".to_string(),
                values: vec![Value::Int32(1), Value::String("HR".to_string())],
            },
            Query::Insert {
                table: "Departments".to_string(),
                values: vec![Value::Int32(2), Value::String("IT".to_string())],
            },
        ];
        let mut tx = tx_manager.begin_transaction();
        for insert in insert_employees {
            tx.add_query(insert);
        }
        for insert in insert_departments {
            tx.add_query(insert);
        }
        tx_manager.commit_transaction(tx).unwrap();

        let join_query = Query::Join {
            left_table: "Employees".to_string(),
            right_table: "Departments".to_string(),
            left_column: "ID".to_string(),
            right_column: "DeptID".to_string(),
            columns: vec![
                "Employees.Name".to_string(),
                "Departments.DeptName".to_string(),
            ],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(join_query);
        let results = tx_manager.commit_transaction(tx).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec![
                vec![
                    Value::String("Alice".to_string()),
                    Value::String("HR".to_string())
                ],
                vec![
                    Value::String("Bob".to_string()),
                    Value::String("IT".to_string())
                ],
            ]
        );

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_transaction_commit_rollback() {
        let (data_dir, _schema, _storage, mut tx_manager) =
            setup_test_db("tx_commit_rollback").unwrap();
        let create_query = Query::CreateTable {
            table: "Test".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Value".to_string(), DataType::String),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        // Test commit
        let insert_query = Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(1), Value::String("Committed".to_string())],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        tx_manager.commit_transaction(tx).unwrap();

        let select_query = Query::Select {
            table: "Test".to_string(),
            columns: vec!["Value".to_string()],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query.clone());
        let results = tx_manager.commit_transaction(tx).unwrap();
        assert_eq!(results, vec![vec![Value::String("Committed".to_string())]]);

        // Test rollback
        let insert_query = Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(2), Value::String("RolledBack".to_string())],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        tx_manager.rollback_transaction(tx).unwrap();

        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query);
        let results = tx_manager.commit_transaction(tx).unwrap();
        assert_eq!(results, vec![vec![Value::String("Committed".to_string())]]);

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_error_handling() {
        let (data_dir, _schema, _storage, mut tx_manager) =
            setup_test_db("error_handling").unwrap();
        let create_query = Query::CreateTable {
            table: "Test".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Value".to_string(), DataType::String),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        // Test type mismatch
        let insert_query = Query::Insert {
            table: "Test".to_string(),
            values: vec![
                Value::String("Invalid".to_string()),
                Value::String("Test".to_string()),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        let result = tx_manager.commit_transaction(tx);
        assert!(matches!(result, Err(DbError::TypeMismatch)));

        // Test missing table
        let select_query = Query::Select {
            table: "NonExistent".to_string(),
            columns: vec!["ID".to_string()],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query);
        let result = tx_manager.commit_transaction(tx);
        assert!(matches!(
            result,
            Err(DbError::InvalidData(ref s)) if s.contains("Table NonExistent not found")
        ));

        // Test invalid column count
        let insert_query = Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(1)],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        let result = tx_manager.commit_transaction(tx);
        assert!(matches!(
            result,
            Err(DbError::InvalidData(ref s)) if s.contains("Expected 2 columns, got 1")
        ));

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_delete() {
        let (data_dir, _schema, _storage, mut tx_manager) = setup_test_db("delete").unwrap();
        let create_query = Query::CreateTable {
            table: "Employees".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Name".to_string(), DataType::String),
                ("Salary".to_string(), DataType::Float32),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        let inserts = vec![
            Query::Insert {
                table: "Employees".to_string(),
                values: vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::Float32(OrderedFloat(1000.0)),
                ],
            },
            Query::Insert {
                table: "Employees".to_string(),
                values: vec![
                    Value::Int32(2),
                    Value::String("Bob".to_string()),
                    Value::Float32(OrderedFloat(1500.0)),
                ],
            },
        ];
        let mut tx = tx_manager.begin_transaction();
        for insert in inserts {
            tx.add_query(insert);
        }
        tx_manager.commit_transaction(tx).unwrap();

        let delete_query = Query::Delete {
            table: "Employees".to_string(),
            condition: Some(Condition::Equal("ID".to_string(), Value::Int32(1))),
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(delete_query);
        tx_manager.commit_transaction(tx).unwrap();

        let select_query = Query::Select {
            table: "Employees".to_string(),
            columns: vec!["Name".to_string(), "Salary".to_string()],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query);
        let results = tx_manager.commit_transaction(tx).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec![
                Value::String("Bob".to_string()),
                Value::Float32(OrderedFloat(1500.0))
            ]
        );

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_drop_table() {
        let (data_dir, _schema, storage, mut tx_manager) = setup_test_db("drop_table").unwrap();
        let create_query = Query::CreateTable {
            table: "Test".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Value".to_string(), DataType::String),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        let insert_query = Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(1), Value::String("Test".to_string())],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(insert_query);
        tx_manager.commit_transaction(tx).unwrap();

        let drop_query = Query::DropTable {
            table: "Test".to_string(),
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(drop_query);
        tx_manager.commit_transaction(tx).unwrap();

        let storage_guard = storage.lock().unwrap();
        let schema = storage_guard.schema();
        assert!(schema.get_table("Test").is_none());
        assert!(!fs::metadata(format!("{}/columns/Test_ID", data_dir)).is_ok());
        assert!(!fs::metadata(format!("{}/indexes/Test_ID.idx", data_dir)).is_ok());

        cleanup_test_db(&data_dir);
    }

    #[test]
    fn test_transaction_queries() {
        let (data_dir, _schema, _storage, mut tx_manager) = setup_test_db("tx_queries").unwrap();

        // Create table
        let create_query = Query::CreateTable {
            table: "Test".to_string(),
            columns: vec![
                ("ID".to_string(), DataType::Int32),
                ("Value".to_string(), DataType::String),
            ],
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(create_query);
        tx_manager.commit_transaction(tx).unwrap();

        // Start transaction, insert, and commit
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(1), Value::String("Committed".to_string())],
        });
        tx_manager.commit_transaction(tx).unwrap();

        // Start transaction, insert, and rollback
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(Query::Insert {
            table: "Test".to_string(),
            values: vec![Value::Int32(2), Value::String("RolledBack".to_string())],
        });
        tx_manager.rollback_transaction(tx).unwrap();

        // Verify only the committed row is present
        let select_query = Query::Select {
            table: "Test".to_string(),
            columns: vec!["Value".to_string()],
            condition: None,
        };
        let mut tx = tx_manager.begin_transaction();
        tx.add_query(select_query);
        let results = tx_manager.commit_transaction(tx).unwrap();
        assert_eq!(results, vec![vec![Value::String("Committed".to_string())]]);

        cleanup_test_db(&data_dir);
    }
}
