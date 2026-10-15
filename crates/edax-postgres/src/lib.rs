//! edax-postgres — the Postgres store kernel.
//!
//! Two SDKs pointed at one Postgres database must agree on the tables, on the SQL each operation runs,
//! on how a message or a record is laid out in a row, on how a read-model query becomes SQL, on what a
//! server error means and on the ledger's fence. Those decisions live here, in Rust, so every SDK ships
//! only the client calls around them. No Postgres client library is involved: the SQL is text, the
//! parameters are text, and every message, record or instance a statement returns is one JSON text column.
//!
//! - [`config`] — the settings a store refuses, their defaults, the relay lease, and the `bigint` bound.
//! - [`error`] — the verdict on a server error, per operation, and on a client-side failure.
//! - [`schema`] — the table contract, the configurable table names, the catalog check, the reference
//!   DDL and the server check. edax provisions nothing: the host's migration tool owns the schema.
//! - [`statements`](mod@statements) — every statement, byte for byte, rendered for one schema, and the text
//!   that opens a transaction with the settings' timeouts.
//! - [`rows`] — the parameters each statement binds, the statement a call runs, and the rows a read decodes.
//! - [`documents`] — the stored text of a projection document, and exact number text.
//! - [`query`](mod@query) — the read-model query compiled to SQL, and the page from its rows.
//! - [`ledger`] — the claim owner, what a claim does with the row it read, its parameters, and the fence on
//!   each settling write.
//!
//! `testdata/golden/postgres_*.json` pin what is here, and `crates/edax-postgres/live-check` replays
//! them, and the filter and query vectors, against a real server.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod fold;
pub mod ingest;

pub mod config;
pub mod documents;
pub mod error;
pub mod ledger;
pub mod query;
pub mod rows;
pub mod schema;
pub mod statements;

pub use config::{check_config, check_lease, Settings};
pub use documents::stored_document;
pub use error::{client_verdict, verdict, ClientFailure, Op};
pub use ledger::{
    claim_read, complete_write, forget_write, new_owner, release_write, ClaimAction, ClaimRead, LedgerRow,
};
pub use query::{compile, decimal_sql, page, CompiledQuery};
pub use rows::{Call, Outbox};
pub use schema::{
    catalog_param, check_catalog, check_server, contract, reference_ddl, Compatibility, Logical, Tables, UniqueKey,
    CONTRACT_VERSION,
};
pub use statements::{begin, statements, Statements};
