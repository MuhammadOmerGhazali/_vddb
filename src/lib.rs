pub mod query;
pub mod schema;
pub mod storage;
pub mod transaction;
pub mod repl;
pub mod types;

pub use query::{Aggregation, Condition, Query};
pub use schema::{Column, Schema, Table};
pub use storage::StorageManager;
pub use transaction::{Transaction, TransactionManager};
pub use repl::Repl;
pub use types::{CompressionType, DataType, DbError, Value};
use std::sync::{Arc, Mutex};

/// Creates a new database instance with the given data directory.
/// Returns a tuple of (`Schema`, `Arc<Mutex<StorageManager>>`, `TransactionManager`) for use in initializing the database.
///
/// # Arguments
///
/// * `data_dir` - The directory where database files (schema, columns, indexes, metadata) are stored.
///
/// # Example
///
/// ```rust
/// use vddb::{create_database, Repl};
///
/// let data_dir = "data";
/// let (schema, storage, tx_manager) = create_database(data_dir).unwrap();
/// let mut repl = Repl::new(tx_manager);
/// // Use repl.run() or interact with storage directly
/// ```
pub fn create_database(data_dir: &str) -> Result<(Schema, Arc<Mutex<StorageManager>>, TransactionManager), DbError> {
    let schema = Schema::new_schema(data_dir)?;
    let storage = Arc::new(Mutex::new(StorageManager::new(data_dir, schema.clone())?));
    let tx_manager = TransactionManager::new(storage.clone());
    Ok((schema, storage, tx_manager))
}