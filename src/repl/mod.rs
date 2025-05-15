use crate::query::parser::parse_query;
use crate::query::Query;
use crate::transaction::{Transaction, TransactionManager};
use crate::types::{DbError, Value};
use prettytable::{format, row, Table};
use rustyline::{error::ReadlineError, Editor};
use std::fmt;

pub struct Repl {
    tx_manager: TransactionManager,
    active_transaction: Option<Transaction>,
}

// Implement Display for Value to match your enum variants
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(i) => write!(f, "{}", i),
            Value::Float32(fl) => write!(f, "{}", fl.0),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

impl Repl {
    pub fn new(tx_manager: TransactionManager) -> Self {
        Repl {
            tx_manager,
            active_transaction: None,
        }
    }

    pub fn run(&mut self) -> Result<(), DbError> {
        println!("VDDB REPL (type EXIT to quit, type HELP for help)");
        
        // Initialize rustyline editor with history
        let mut rl = Editor::<()>::new().map_err(|e| DbError::TransactionError(e.to_string()))?;
        if rl.load_history("vddb_history.txt").is_err() {
            println!("No previous history found");
        }

        loop {
            let readline = rl.readline("vddb> ");
            match readline {
                Ok(input) => {
                    let input = input.trim();
                    if input.eq_ignore_ascii_case("EXIT") {
                        if let Some(tx) = self.active_transaction.take() {
                            self.tx_manager.rollback_transaction(tx)?;
                            println!("Active transaction rolled back.");
                        }
                        rl.save_history("vddb_history.txt")
                            .map_err(|e| DbError::TransactionError(e.to_string()))?;
                        break;
                    }
                    
                    if input.eq_ignore_ascii_case("HELP") {
                        self.print_help();
                        continue;
                    }
                    
                    if input.is_empty() {
                        continue;
                    }

                    // Add to history
                    rl.add_history_entry(input);

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
                                                self.print_results(&results);
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
                                                self.print_results(&results);
                                            }
                                            Err(e) => println!("Error: {}", e),
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                },
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break
                },
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break
                },
                Err(err) => {
                    println!("Error: {:?}", err);
                    break
                }
            }
        }
        Ok(())
    }

    fn print_results(&self, results: &[Vec<Value>]) {
        if results.is_empty() {
            return;
        }

        let mut table = Table::new();
        // Set a compact format
        table.set_format(*format::consts::FORMAT_BOX_CHARS);

        // Add data rows
        for row in results {
            let mut table_row = vec![];
            for cell in row {
                table_row.push(cell.to_string());
            }
            table.add_row(table_row.into());
        }

        table.printstd();
    }

    fn print_help(&self) {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_BOX_CHARS);
        
        table.add_row(row![bFg => "Command", "Description"]);
        table.add_row(row!["START TRANSACTION", "Begin a new transaction"]);
        table.add_row(row!["COMMIT", "Commit the active transaction"]);
        table.add_row(row!["ROLLBACK", "Rollback the active transaction"]);
        table.add_row(row!["EXIT", "Exit the REPL"]);
        table.add_row(row!["HELP", "Show this help message"]);
        table.add_row(row!["", ""]);
        table.add_row(row![bFg => "SQL Commands"]);
        table.add_row(row!["SELECT ...", "Query data"]);
        table.add_row(row!["INSERT ...", "Insert data"]);
        table.add_row(row!["UPDATE ...", "Update data"]);
        table.add_row(row!["DELETE ...", "Delete data"]);
        table.add_row(row!["CREATE TABLE ...", "Create a new table"]);
        
        table.printstd();
    }
}