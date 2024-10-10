use sqlite_watcher::connection::Connection;
use sqlite_watcher::watcher::{TableObserver, Watcher};
use std::collections::BTreeSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use tempdir::TempDir;

// Simple example which starts 2 connections on the same database with 2 observers.
// It should print at least one entry for each observer:
// ```
// Updated tables: [observer-1] {"foo"}
// Updated tables: [observer-2] {"foo"}
// ```
fn main() {
    let tmp_dir = TempDir::new("sqlite-watcher-rusqlite").unwrap();
    let db_file = tmp_dir.path().join("db.sqlite3");
    let connection1 = rusqlite::Connection::open(&db_file).unwrap();
    let connection2 = rusqlite::Connection::open(&db_file).unwrap();
    let watcher = Watcher::new().unwrap();

    let connection1 = Connection::new(connection1, Arc::clone(&watcher)).unwrap();
    let connection2 = Connection::new(connection2, Arc::clone(&watcher)).unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();

    let observer1 = Observer::new("observer-1", sender.clone());
    let observer2 = Observer::new("observer-2", sender);

    let observer_handle_1 = watcher.add_observer(Box::new(observer1)).unwrap();
    let observer_handle_2 = watcher.add_observer(Box::new(observer2)).unwrap();

    connection1
        .execute(
            "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)",
            (),
        )
        .unwrap();

    let thread_handles = [
        (connection1, observer_handle_1),
        (connection2, observer_handle_2),
    ]
    .into_iter()
    .map(|(mut connection, observer_handle)| {
        let watcher_cloned = Arc::clone(&watcher);
        std::thread::spawn(move || {
            connection.sync_watcher_tables().unwrap();
            let tx = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            tx.execute("INSERT INTO foo (value) VALUES (?)", rusqlite::params![400])
                .unwrap();
            tx.commit().unwrap();
            connection.publish_watcher_changes().unwrap();
            watcher_cloned.remove_observer(observer_handle).unwrap();
        })
    })
    .collect::<Vec<_>>();

    while let Ok((observer_name, updated_tables)) = receiver.recv() {
        println!("Updated tables: [{observer_name}] {updated_tables:?}")
    }

    for thread_handle in thread_handles {
        thread_handle.join().unwrap();
    }
}

struct Observer {
    name: String,
    sender: Sender<(String, BTreeSet<String>)>,
}

impl Observer {
    pub fn new(name: impl Into<String>, sender: Sender<(String, BTreeSet<String>)>) -> Observer {
        Self {
            name: name.into(),
            sender,
        }
    }
}

impl TableObserver for Observer {
    fn tables(&self) -> Vec<String> {
        vec!["foo".to_owned()]
    }

    fn on_tables_changed(&self, tables: &BTreeSet<String>) {
        self.sender
            .send((self.name.clone(), tables.clone()))
            .unwrap()
    }
}
