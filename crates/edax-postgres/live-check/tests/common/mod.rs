//! What every live check shares: the server connection, a schema of reference tables, and the parameters a
//! statement binds.

use edax_postgres::{reference_ddl, statements, Statements, Tables};
use postgres::types::ToSql;
use postgres::{Client, NoTls};

/// A connection to the throwaway server `EDAX_POSTGRES_URL` names, `None` when it names none.
pub fn connect() -> Option<Client> {
    let url = std::env::var("EDAX_POSTGRES_URL").ok()?;
    Some(Client::connect(&url, NoTls).expect("EDAX_POSTGRES_URL connects"))
}

/// A fresh schema holding the reference tables `tables` names, as a host's migration tool creates them, and
/// every statement rendered for them.
pub fn provision(client: &mut Client, tables: &Tables) -> Statements {
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS \"{0}\" CASCADE; CREATE SCHEMA \"{0}\"",
            tables.schema()
        ))
        .unwrap();
    client.batch_execute(&reference_ddl(tables)).unwrap();
    statements(tables)
}

/// The kernel's parameters as the client binds them: text, or SQL `NULL`.
pub fn bind(params: &[Option<String>]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}
