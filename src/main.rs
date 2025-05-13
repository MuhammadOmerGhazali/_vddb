use vddb::{create_database, DbError, Repl};

fn main() -> Result<(), DbError> {
    let data_dir = "data";
    let (_schema, _storage, tx_manager) = create_database(data_dir)?;
    let mut repl = Repl::new(tx_manager);
    repl.run()?;
    Ok(())
}