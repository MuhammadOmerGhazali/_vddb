use crate::query::parser::parse_query;
use crate::query::Query;
use crate::transaction::{Transaction, TransactionManager};
use crate::types::DbError;
use std::io::{self, Write};

pub struct Repl {
    tx_manager: TransactionManager,
    active_transaction: Option<Transaction>,
}

impl Repl {
    pub fn new(tx_manager: TransactionManager) -> Self {
        Repl {
            tx_manager,
            active_transaction: None,
        }
    }

    pub fn run(&mut self) -> Result<(), DbError> {
        println!("VDDB REPL (type EXIT to quit)");
        loop {
            print!("vddb> ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            if input.eq_ignore_ascii_case("EXIT") {
                if let Some(tx) = self.active_transaction.take() {
                    self.tx_manager.rollback_transaction(tx)?;
                    println!("Active transaction rolled back.");
                }
                break;
            }
            if input.is_empty() {
                continue;
            }

            match parse_query(input) {
                Ok(query) => {
                    match query {
                        Query::StartTransaction => {
                            if self.active_transaction.is_some() {
                                println!("Error: Transaction already active");
                                continue;
                            }
                            self.active_transaction = Some(self.tx_manager.begin_transaction());
                            println!("Transaction started.");
                        }
                        Query::Commit => {
                            if let Some(tx) = self.active_transaction.take() {
                                match self.tx_manager.commit_transaction(tx) {
                                    Ok(results) => {
                                        if !results.is_empty() {
                                            println!("{:?}", results);
                                        }
                                        println!("Transaction committed.");
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            } else {
                                println!("Error: No active transaction");
                            }
                        }
                        Query::Rollback => {
                            if let Some(tx) = self.active_transaction.take() {
                                self.tx_manager.rollback_transaction(tx)?;
                                println!("Transaction rolled back.");
                            } else {
                                println!("Error: No active transaction");
                            }
                        }
                        _ => {
                            if let Some(ref mut tx) = self.active_transaction {
                                tx.add_query(query);
                            } else {
                                let mut tx = self.tx_manager.begin_transaction();
                                tx.add_query(query);
                                match self.tx_manager.commit_transaction(tx) {
                                    Ok(results) => {
                                        if !results.is_empty() {
                                            println!("{:?}", results);
                                        }
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            }
                        }
                    }
                }
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(())
    }
}