//! Sql trait implementations for `rusqlite`.
//!
//! Requires the `rusqlite` feature to be enabled.
use crate::connection::{SqlConnection, SqlExecutor, SqlTransaction};
use rusqlite::{Connection, Transaction};
use std::ops::Deref;

impl SqlExecutor for Connection {
    type Error = rusqlite::Error;
    fn sql_query_values(&self, query: &str) -> Result<Vec<usize>, Self::Error> {
        let mut stmt = self.prepare(query)?;
        let rows = stmt.query_map((), |r| r.get(0))?;
        let mut table_ids = Vec::new();
        for row in rows {
            table_ids.push(row?);
        }
        Ok(table_ids)
    }

    fn sql_execute(&mut self, query: &str) -> Result<(), Self::Error> {
        Connection::execute(self, query, ())?;
        Ok(())
    }
}
impl SqlConnection for Connection {
    fn sql_transaction(&mut self) -> Result<impl SqlTransaction<Error = Self::Error>, Self::Error> {
        self.transaction()
    }
}

impl<'c> SqlExecutor for Transaction<'c> {
    type Error = rusqlite::Error;
    fn sql_query_values(&self, query: &str) -> Result<Vec<usize>, Self::Error> {
        self.deref().sql_query_values(query)
    }

    fn sql_execute(&mut self, query: &str) -> Result<(), Self::Error> {
        self.execute(query, ())?;
        Ok(())
    }
}

impl<'c> SqlTransaction for Transaction<'c> {
    fn sql_commit_transaction(self) -> Result<(), Self::Error> {
        Transaction::commit(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connection::test::TestObserver;
    use crate::connection::Connection as WatchedConnection;
    use crate::connection::SqlTransaction;
    use crate::watcher::Watcher;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[test]
    fn transaction_tracking() {
        let orig = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            orig(panic_info);
            std::process::exit(-1);
        }));

        let connection = Connection::open_in_memory().unwrap();

        let watcher = Watcher::new().unwrap();
        let mut connection = WatchedConnection::new(connection, Arc::clone(&watcher)).unwrap();
        connection
            .execute(
                "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
                (),
            )
            .unwrap();
        connection
            .execute("CREATE TABLE bar (v INTEGER UNIQUE)", ())
            .unwrap();

        let foo_table_set = BTreeSet::from_iter(["foo".to_string()]);
        let bar_table_set = BTreeSet::from_iter(["bar".to_string()]);
        let foo_bar_table_set = BTreeSet::from_iter(["foo".to_string(), "bar".to_string()]);

        // Synchronization to avoid merging of changes;
        let (observer, receiver) = TestObserver::new(
            foo_bar_table_set.clone().into_iter().collect(),
            [
                foo_table_set,
                bar_table_set.clone(),
                bar_table_set,
                foo_bar_table_set,
            ],
        );

        let _ = watcher.add_observer(Box::new(observer));

        do_tx(&mut connection, |tx| {
            tx.execute("INSERT INTO foo VALUES( null,10)", ()).unwrap();
        });
        receiver.recv().unwrap();
        do_tx(&mut connection, |tx| {
            tx.execute("INSERT OR REPLACE INTO bar VALUES(10)", ())
                .unwrap();
        });
        receiver.recv().unwrap();
        do_tx(&mut connection, |tx| {
            tx.execute("INSERT OR REPLACE INTO bar VALUES(10)", ())
                .unwrap();
        });
        receiver.recv().unwrap();
        do_tx(&mut connection, |tx| {
            tx.execute("DELETE FROM foo WHERE v=10", ()).unwrap();
            tx.execute("DELETE FROM bar WHERE v=10", ()).unwrap();
        });
        receiver.recv().unwrap();

        connection.stop_tracking().unwrap();
    }

    fn do_tx(connection: &mut WatchedConnection<Connection>, f: impl FnOnce(&mut Transaction)) {
        connection.sync_watcher_tables().unwrap();
        let mut tx = connection.transaction().unwrap();
        f(&mut tx);
        tx.sql_commit_transaction().unwrap();
        connection.publish_watcher_changes().unwrap();
    }
}
