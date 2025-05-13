use crate::query::{Query, planner::QueryEngine};
use crate::storage::StorageManager;
use crate::types::DbError;
use crate::Value;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Serialize, Deserialize)]
pub struct Transaction {
    id: u64,
    queries: Vec<Query>,
}

impl Transaction {
    pub fn add_query(&mut self, query: Query) {
        self.queries.push(query);
    }
}

pub struct TransactionManager {
    storage: Arc<Mutex<StorageManager>>,
    next_tx_id: u64,
    wal: File,
}

impl TransactionManager {
    pub fn new(storage: Arc<Mutex<StorageManager>>) -> Result<Self, DbError> {
        let data_dir = {
            let storage_guard = storage.lock().unwrap();
            storage_guard.data_dir().to_string()
        };
        let wal_dir = format!("{}/wal", data_dir);
        fs::create_dir_all(&wal_dir)?;
        let wal = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(format!("{}/wal.log", wal_dir))
            .map_err(|e| DbError::IoError(e))?;
        Ok(TransactionManager {
            storage,
            next_tx_id: 1,
            wal,
        })
    }

    pub fn begin_transaction(&mut self) -> Transaction {
        let tx = Transaction {
            id: self.next_tx_id,
            queries: Vec::new(),
        };
        self.next_tx_id += 1;
        tx
    }

    pub fn commit_transaction(&mut self, mut tx: Transaction) -> Result<Vec<Vec<Value>>, DbError> {
        let tx_data = bincode::serialize(&tx).map_err(|e| DbError::from(*e))?;
        self.wal.write_all(&tx_data)?;
        self.wal.flush()?;

        let mut results = Vec::new();
        let mut query_engine = QueryEngine::new(Arc::clone(&self.storage));
        for query in tx.queries.drain(..) {
            results.extend(query_engine.execute(query)?);
        }

        self.wal.set_len(0)?;
        self.wal.seek(SeekFrom::Start(0))?;
        Ok(results)
    }

    pub fn rollback_transaction(&mut self, _tx: Transaction) -> Result<(), DbError> {
        // For immutable database, rollback clears WAL and relies on not having committed changes
        self.wal.set_len(0)?;
        self.wal.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}