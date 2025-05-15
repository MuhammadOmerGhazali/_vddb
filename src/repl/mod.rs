use crate::query::parser::parse_query;
use crate::query::Query;
use crate::transaction::{Transaction, TransactionManager};
use crate::types::{DbError, Value};
use prettytable::{format, row, Table};
use rustyline::{error::ReadlineError, Editor};
use std::fmt;
use colored::*;

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
        println!("{}","VDDB REPL (type EXIT to quit, type HELP for help)".cyan().bold());
        
        // Initialize rustyline editor with history
        let mut rl = Editor::<()>::new().map_err(|e| DbError::TransactionError(e.to_string()))?;
        if rl.load_history("vddb_history.txt").is_err() {
            println!("No previous history found");
        }

        loop {
            let readline = rl.readline(&"vddb> ".blue().bold().to_string());
            match readline {
                Ok(input) => {
                    let input = input.trim();
                    if input.eq_ignore_ascii_case("EXIT") {
                        if let Some(tx) = self.active_transaction.take() {
                            self.tx_manager.rollback_transaction(tx)?;
                            println!("{}","Active transaction rolled back.".green());
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
                                        println!("{}","Error: Transaction already active".red());
                                        continue;
                                    }
                                    self.active_transaction = Some(self.tx_manager.begin_transaction());
                                    println!("{}", "Transaction started.".green());
                                }
                                Query::Commit => {
                                    if let Some(tx) = self.active_transaction.take() {
                                        match self.tx_manager.commit_transaction(tx) {
                                            Ok(results) => {
                                                self.print_results(&results);
                                                println!("{}", "Transaction committed.".green());
                                            }
                                            Err(e) => println!("{}: {}", "Error".red().bold(), e),
                                        }
                                    } else {
                                        println!("{}", "Error: No active transaction".red());
                                    }
                                }
                                Query::Rollback => {
                                    if let Some(tx) = self.active_transaction.take() {
                                        self.tx_manager.rollback_transaction(tx)?;
                                        println!("{}", "Transaction rolled back.".green());
                                    } else {
                                        println!("{}", "Error: No active transaction".red());
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
                                            Err(e) => println!("{}: {}", "Error".red().bold(), e),
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => println!("{}: {}", "Error".red().bold(), e),
                    }
                },
                Err(ReadlineError::Interrupted) => {
                    println!("{}", "CTRL-C".yellow());
                    break
                },
                Err(ReadlineError::Eof) => {
                    println!("{}", "CTRL-D".yellow());
                    break
                },
                Err(err) => {
                    println!("{}: {:?}", "Error".red().bold(), err);
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
        table.set_format(*format::consts::FORMAT_CLEAN);
        
        // Colored header
        table.add_row(row![bFg => "Command".cyan().bold(), "Description".cyan().bold()]);
        table.add_row(row!["START TRANSACTION".green(), "Begin a new transaction"]);
        table.add_row(row!["COMMIT".green(), "Commit the active transaction"]);
        table.add_row(row!["ROLLBACK".green(), "Rollback the active transaction"]);
        table.add_row(row!["EXIT".yellow(), "Exit the REPL"]);
        table.add_row(row!["HELP".yellow(), "Show this help message"]);
        table.add_row(row!["", ""]);
        table.add_row(row![bFg => "SQL Commands".cyan().bold(), "".cyan().bold()]);
        table.add_row(row!["SELECT ...".green(), "Query data"]);
        table.add_row(row!["INSERT ...".green(), "Insert data"]);
        table.add_row(row!["UPDATE ...".green(), "Update data"]);
        table.add_row(row!["DELETE ...".green(), "Delete data"]);
        table.add_row(row!["CREATE TABLE ...".green(), "Create a new table"]);
        table.add_row(row!["MAKE INDEX ON table (column)".green(), "Create an index on a column"]);
        table.add_row(row!["UNMAKE INDEX column ON table".green(), "Drop an index from a column"]);

        table.printstd();
    }
}