use crate::query::parser::parse_query;
use crate::transaction::TransactionManager;
use crate::types::DbError;
use std::io::{self, Write};

pub struct Repl {
    tx_manager: TransactionManager,
}

impl Repl {
    pub fn new(tx_manager: TransactionManager) -> Self {
        Repl { tx_manager }
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
                break;
            }
            match parse_query(input) {
                Ok(query) => {
                    let mut tx = self.tx_manager.begin_transaction();
                    tx.add_query(query);
                    match self.tx_manager.commit_transaction(tx) {
                        Ok(results) => {
                            for row in results {
                                println!("{:?}", row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                }
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(())
    }
}