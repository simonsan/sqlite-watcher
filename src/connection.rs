use crate::watcher::{ObservedTableOp, Watcher};
use fixedbitset::FixedBitSet;
use std::error::Error;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tracing::debug;

#[cfg(feature = "rusqlite")]
pub mod rusqlite;

#[cfg(feature = "sqlx")]
pub mod sqlx;

/// Defines an implementation capable of executing SQL statement on a sqlite connection.
///
/// This is required so we can set up the temporary triggers and tables required to
/// track changes.
pub trait SqlExecutor {
    type Error: Error;
    /// This method will execute a query which returns 0 or N rows with one column of type `usize`.
    ///
    /// # Errors
    ///
    /// Should return error if the query failed.
    fn sql_query_values(&mut self, query: &str) -> Result<Vec<usize>, Self::Error>;

    /// Execute an sql statement which does not return any rows.
    ///
    /// # Errors
    ///
    /// Should return error if the query failed.
    fn sql_execute(&mut self, query: &str) -> Result<(), Self::Error>;
}

/// Defines an implementation of a sqlite connection from which we can create an [`SqlTransaction`].
#[allow(clippy::module_name_repetitions)]
pub trait SqlConnection: SqlExecutor {
    /// Create a new transaction for the connection.
    ///
    /// # Errors
    ///
    /// Should return an error if the transaction can't be created.
    fn sql_transaction(
        &mut self,
    ) -> Result<impl SqlTransaction<Error = Self::Error> + '_, Self::Error>;
}

/// Defines a transaction on a sqlite connection.
pub trait SqlTransaction: SqlExecutor {
    /// Commit the current transaction.
    ///
    /// # Errors
    ///
    /// Should return an error if a transaction can't be committed.
    fn sql_commit_transaction(self) -> Result<(), Self::Error>;
}

/// Defines an implementation capable of executing SQL statement on a sqlite connection.
///
/// This is required so we can set up the temporary triggers and tables required to
/// track changes.
pub trait SqlExecutorAsync {
    type Error: Error;
    /// This method will execute a query which returns 0 or N rows with one column of type `usize`.
    ///
    /// # Errors
    ///
    /// Should return error if the query failed.
    fn sql_query_values(
        &mut self,
        query: &str,
    ) -> impl Future<Output = Result<Vec<usize>, Self::Error>> + Send;

    /// Execute an sql statement which does not return any rows.
    ///
    /// # Errors
    ///
    /// Should return error if the query failed.
    fn sql_execute(&mut self, query: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Defines an implementation of a sqlite connection from which we can create an [`crate::connection::SqlTransaction`].
#[allow(clippy::module_name_repetitions)]
pub trait SqlConnectionAsync: SqlExecutorAsync {
    /// Create a new transaction for the connection.
    ///
    /// # Errors
    ///
    /// Should return an error if the transaction can't be created.
    fn sql_transaction(
        &mut self,
    ) -> impl Future<Output = Result<impl SqlTransactionAsync<Error = Self::Error> + '_, Self::Error>>
           + Send;
}

/// Defines a transaction on a sqlite connection.
pub trait SqlTransactionAsync: SqlExecutorAsync {
    /// Commit the current transaction.
    ///
    /// # Errors
    ///
    /// Should return an error if a transaction can't be committed.
    fn sql_commit_transaction(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Building block to provide tracking capabilities to any type of sqlite connection which
/// implements the [`SqlConnection`] trait.
///
/// # Initialization
///
/// It's recommended to call [`State::set_pragmas()`] to enable in memory temporary tables and recursive
/// triggers. If your connection already has this set up, this can be skipped.
///
/// Next you need to create the infrastructure to track changes. This can be accomplished with
/// [`State::start_tracking()`].
///
/// # Tracking changes
///
/// To make sure we only track required tables always call [`State::sync_tables()`] before a query/statement
/// or a transaction.
///
/// When the query/statement or transaction are completed, call [`State::publish_changes()`] to check
/// which tables have been modified and send this information to the watcher.
///
/// # Disable Tracking
///
/// If you wish to remove all the tracking infrastructure from a connection on which
/// [`State::start_tracking()`] was called, then call [`State::stop_tracking()`].
///
/// # See Also
///
/// The [`Connection`] type provided by this crate provides an example integration implementation.
pub struct State {
    tracked_tables: FixedBitSet,
    last_sync_version: u64,
}

impl State {
    /// Enable required pragmas for execution.
    ///
    /// # Errors
    ///
    /// Returns error if the pragma changes failed.
    pub fn set_pragmas<C: SqlConnection>(connection: &mut C) -> Result<(), C::Error> {
        connection.sql_execute("PRAGMA temp_store = MEMORY")?;
        connection.sql_execute("PRAGMA recursive_triggers='ON'")?;
        Ok(())
    }

    /// Enable required pragmas for execution.
    ///
    /// # Errors
    ///
    /// Returns error if the pragma changes failed.
    pub async fn set_pragmas_async<C: SqlConnectionAsync>(
        connection: &mut C,
    ) -> Result<(), C::Error> {
        connection.sql_execute("PRAGMA temp_store = MEMORY").await?;
        connection
            .sql_execute("PRAGMA recursive_triggers='ON'")
            .await?;
        Ok(())
    }

    /// Prepare the `connection` for tracking.
    ///
    /// This will create the temporary table used to track change.
    ///
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub fn start_tracking<C: SqlConnection>(connection: &mut C) -> Result<(), C::Error> {
        // create tracking table and cleanup previous data if re-used from a connection pool.
        let mut tx = connection.sql_transaction()?;
        tx.sql_execute(&create_tracking_table_query())?;
        tx.sql_execute(&empty_tracking_table_query())?;
        tx.sql_commit_transaction()
    }

    /// Prepare the `connection` for tracking.
    ///
    /// This will create the temporary table used to track change.
    ///
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub async fn start_tracking_async<C: SqlConnectionAsync>(
        connection: &mut C,
    ) -> Result<(), C::Error> {
        // create tracking table and cleanup previous data if re-used from a connection pool.
        let mut tx = connection.sql_transaction().await?;
        tx.sql_execute(&create_tracking_table_query()).await?;
        tx.sql_execute(&empty_tracking_table_query()).await?;
        tx.sql_commit_transaction().await
    }

    /// Remove all triggers and the tracking table from `connection`.
    //
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub fn stop_tracking<C: SqlConnection>(
        &self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let tables = watcher.observed_tables();
        let mut tx = connection.sql_transaction()?;
        for (id, table_name) in tables.into_iter().enumerate() {
            drop_triggers(&mut tx, &table_name, id)?;
        }
        tx.sql_execute(&drop_tracking_table_query())?;
        tx.sql_commit_transaction()
    }

    /// Remove all triggers and the tracking table from `connection`.
    //
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub async fn stop_tracking_async<C: SqlConnectionAsync>(
        &self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let tables = watcher.observed_tables();
        let mut tx = connection.sql_transaction().await?;
        for (id, table_name) in tables.into_iter().enumerate() {
            drop_triggers_async(&mut tx, &table_name, id).await?;
        }
        tx.sql_execute(&drop_tracking_table_query()).await?;
        tx.sql_commit_transaction().await
    }

    /// Create a new instance without initializing any connection.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tracked_tables: FixedBitSet::new(),
            last_sync_version: 0,
        }
    }

    /// Synchronize the table list from the watcher.
    ///
    /// This method will create new triggers for tables that are not being watched over this
    /// connection and remove triggers for tables that are no longer observed by the watcher.
    ///
    /// # Errors
    ///
    /// Returns error if creation or removal of triggers failed.
    #[tracing::instrument(level=tracing::Level::DEBUG, skip(self, connection, watcher))]
    pub fn sync_tables<C: SqlConnection>(
        &mut self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let Some(new_version) = self.should_sync(watcher) else {
            return Ok(());
        };

        debug!("Syncing tables from observer");
        let Some((new_tracker_state, tracker_changes)) = self.calculate_sync_changes(watcher)
        else {
            debug!("No changes");
            return Ok(());
        };

        let mut tx = connection.sql_transaction()?;
        for change in tracker_changes {
            match change {
                ObservedTableOp::Add(table_name, id) => {
                    debug!("Add watcher for table {table_name} id={id}");
                    create_triggers(&mut tx, &table_name, id)?;
                }
                ObservedTableOp::Remove(table_name, id) => {
                    debug!("Remove watcher for table {table_name}");
                    drop_triggers(&mut tx, &table_name, id)?;
                }
            }
        }
        tx.sql_commit_transaction()?;

        self.apply_sync_changes(new_tracker_state, new_version);

        Ok(())
    }

    /// Synchronize the table list from the watcher.
    ///
    /// This method will create new triggers for tables that are not being watched over this
    /// connection and remove triggers for tables that are no longer observed by the watcher.
    ///
    /// # Errors
    ///
    /// Returns error if creation or removal of triggers failed.
    #[tracing::instrument(level=tracing::Level::DEBUG, skip(self, connection, watcher))]
    pub async fn sync_tables_async<C: SqlConnectionAsync>(
        &mut self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let Some(new_version) = self.should_sync(watcher) else {
            return Ok(());
        };

        debug!("Syncing tables from observer");
        let Some((new_tracker_state, tracker_changes)) = self.calculate_sync_changes(watcher)
        else {
            debug!("No changes");
            return Ok(());
        };

        let mut tx = connection.sql_transaction().await?;
        for change in tracker_changes {
            match change {
                ObservedTableOp::Add(table_name, id) => {
                    debug!("Add watcher for table {table_name} id={id}");
                    create_triggers_async(&mut tx, &table_name, id).await?;
                }
                ObservedTableOp::Remove(table_name, id) => {
                    debug!("Remove watcher for table {table_name}");
                    drop_triggers_async(&mut tx, &table_name, id).await?;
                }
            }
        }
        tx.sql_commit_transaction().await?;

        self.apply_sync_changes(new_tracker_state, new_version);

        Ok(())
    }

    /// Check the tracking table and report finding to the [Watcher].
    ///
    /// The table where the changes are tracked is read and reset. Any
    /// table that has been modified will be communicated to the [Watcher], which in turn
    /// will notify the respective [TableObserver].
    ///
    /// # Errors
    ///
    /// Returns error if we failed to read from the temporary tables.
    ///
    /// [Watcher]: `crate::watcher::Watcher`
    /// [TableObserver]: `crate::watcher::TableObserver`
    #[tracing::instrument(level=tracing::Level::DEBUG, skip(self, connection, watcher))]
    pub fn publish_changes<C: SqlConnection>(
        &mut self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let mut result = FixedBitSet::with_capacity(self.tracked_tables.len());

        let query = select_updated_tables_query();
        let modified_table_ids = connection.sql_query_values(&query)?;
        for id in modified_table_ids {
            debug!("Table {} has been modified", id);
            result.set(id, true);
        }

        if !result.is_clear() {
            // Reset updated values.
            connection.sql_execute(&reset_updated_tables_query())?;
        }

        watcher.publish_changes(result);

        Ok(())
    }

    /// Check the tracking table and report finding to the [Watcher].
    ///
    /// The table where the changes are tracked is read and reset. Any
    /// table that has been modified will be communicated to the [Watcher], which in turn
    /// will notify the respective [TableObserver].
    ///
    /// # Errors
    ///
    /// Returns error if we failed to read from the temporary tables.
    ///
    /// [Watcher]: `crate::watcher::Watcher`
    /// [TableObserver]: `crate::watcher::TableObserver`
    #[tracing::instrument(level=tracing::Level::DEBUG, skip(self, connection, watcher))]
    pub async fn publish_changes_async<C: SqlConnectionAsync>(
        &mut self,
        connection: &mut C,
        watcher: &Watcher,
    ) -> Result<(), C::Error> {
        let mut result = FixedBitSet::with_capacity(self.tracked_tables.len());

        let query = select_updated_tables_query();
        let modified_table_ids = connection.sql_query_values(&query).await?;
        for id in modified_table_ids {
            debug!("Table {} has been modified", id);
            result.set(id, true);
        }

        if !result.is_clear() {
            // Reset updated values.
            connection
                .sql_execute(&reset_updated_tables_query())
                .await?;
        }

        watcher.publish_changes_async(result).await;

        Ok(())
    }

    fn should_sync(&self, watcher: &Watcher) -> Option<u64> {
        let service_version = watcher.tables_version();
        if service_version == self.last_sync_version {
            None
        } else {
            Some(service_version)
        }
    }

    /// Determine which tables should start and/or stop being watched.
    fn calculate_sync_changes(
        &self,
        watcher: &Watcher,
    ) -> Option<(FixedBitSet, Vec<ObservedTableOp>)> {
        let (new_tracker_state, tracker_changes) =
            watcher.calculate_sync_changes(&self.tracked_tables);

        if tracker_changes.is_empty() {
            return None;
        }

        Some((new_tracker_state, tracker_changes))
    }

    /// Once we are satisfied with the changes, apply the new state.
    fn apply_sync_changes(&mut self, new_tracker_state: FixedBitSet, new_version: u64) {
        // Update local tracker bitset
        self.tracked_tables = new_tracker_state;
        self.last_sync_version = new_version;
    }
}

/// Connection abstraction that provides on possible implementation which uses the building
/// blocks ([`State`]) provided by this crate.
///
/// For simplicity, it takes ownership of an existing type which implements [`SqlConnection`] and
/// initializes all the tracking infrastructure. The original type can still be accessed as
/// [`Connection`] implements both [`Deref`] and [`DerefMut`].
///
/// # Remarks
///
/// To make sure all changes are capture, it's recommended to always call
/// [`Connection::sync_watcher_tables()`]
/// before any query/statement or transaction.
///
/// # Example
///
/// ## Single Query/Statement
///
/// ```rust
/// use sqlite_watcher::connection::Connection;
/// use sqlite_watcher::connection::SqlConnection;
/// use sqlite_watcher::watcher::Watcher;
///
/// pub fn track_changes<C:SqlConnection>(connection: C) {
///     let watcher = Watcher::new().unwrap();
///     let mut connection = Connection::new(connection, watcher).unwrap();
///
///     // Sync tables so we are up to date.
///     connection.sync_watcher_tables().unwrap();
///
///     connection.sql_execute("sql query here").unwrap();
///
///     // Publish changes to the watcher
///     connection.publish_watcher_changes().unwrap();
/// }
/// ```
///
/// ## Transaction
///
/// ```rust
/// use sqlite_watcher::connection::Connection;
/// use sqlite_watcher::connection::{SqlConnection, SqlTransaction, SqlExecutor};
/// use sqlite_watcher::watcher::Watcher;
///
/// pub fn track_changes<C:SqlConnection>(connection: C) {
///     let watcher = Watcher::new().unwrap();
///     let mut connection = Connection::new(connection, watcher).unwrap();
///
///     // Sync tables so we are up to date.
///     connection.sync_watcher_tables().unwrap();
///
///     let mut tx = connection.sql_transaction().unwrap();
///
///     tx.sql_execute("sql query here").unwrap();
///     tx.sql_execute("sql query here").unwrap();
///     tx.sql_execute("sql query here").unwrap();
///
///     tx.sql_commit_transaction().unwrap();
///
///     // Publish changes to the watcher
///     connection.publish_watcher_changes().unwrap();
/// }
/// ```
pub struct Connection<C: SqlConnection> {
    state: State,
    watcher: Arc<Watcher>,
    connection: C,
}
impl<C: SqlConnection> Connection<C> {
    /// Create a new connection with `connection` and `watcher`.
    ///
    /// See [`State::start_tracking()`] for more information about initialization.
    ///
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub fn new(mut connection: C, watcher: Arc<Watcher>) -> Result<Self, C::Error> {
        let state = State::new();
        State::set_pragmas(&mut connection)?;
        State::start_tracking(&mut connection)?;
        Ok(Self {
            state,
            watcher,
            connection,
        })
    }

    /// Sync tables from the [`Watcher`] and update tracking infrastructure.
    ///
    /// See [`State::sync_tables()`] for more information.
    ///
    /// # Errors
    ///
    /// Returns error if we failed to sync the changes to the database.
    pub fn sync_watcher_tables(&mut self) -> Result<(), C::Error> {
        self.state.sync_tables(&mut self.connection, &self.watcher)
    }

    /// Check if any tables have changed and notify the [`Watcher`]
    ///
    /// See [`State::publish_changes()`] for more information.
    ///
    /// It is recommended to call this method
    ///
    /// # Errors
    ///
    /// Returns error if we failed to check for changes.
    pub fn publish_watcher_changes(&mut self) -> Result<(), C::Error> {
        self.state
            .publish_changes(&mut self.connection, &self.watcher)
    }

    /// Disable all tracking on this connection.
    ///
    /// See [`State::stop_tracking`] for more details.
    ///
    /// # Errors
    ///
    /// Returns error if the queries failed.
    pub fn stop_tracking(&mut self) -> Result<(), C::Error> {
        self.state
            .stop_tracking(&mut self.connection, &self.watcher)
    }

    /// Consume the current connection and take ownership of the real sql connection.
    ///
    /// # Remarks
    ///
    /// This does not stop the tracking infrastructure enabled on the connection.
    /// Use [`Self::stop_tracking()`] to disable it first.
    pub fn take(self) -> C {
        self.connection
    }
}

/// Same as [`Connection`] but with an async executor.
#[allow(clippy::module_name_repetitions)]
pub struct ConnectionAsync<C: SqlConnectionAsync> {
    state: State,
    watcher: Arc<Watcher>,
    connection: C,
}
impl<C: SqlConnectionAsync> ConnectionAsync<C> {
    /// Create a new connection with `connection` and `watcher`.
    ///
    /// See [`State::start_tracking()`] for more information about initialization.
    ///
    /// # Errors
    ///
    /// Returns error if the initialization failed.
    pub async fn new(mut connection: C, watcher: Arc<Watcher>) -> Result<Self, C::Error> {
        let state = State::new();
        State::set_pragmas_async(&mut connection).await?;
        State::start_tracking_async(&mut connection).await?;
        Ok(Self {
            state,
            watcher,
            connection,
        })
    }

    /// See [`Connection::sync_watcher_tables`] for more details.
    ///
    /// # Errors
    ///
    /// Returns error if we failed to sync the changes to the database.
    pub async fn sync_watcher_tables(&mut self) -> Result<(), C::Error> {
        self.state
            .sync_tables_async(&mut self.connection, &self.watcher)
            .await
    }

    /// See [`Connection::publish_watcher_changes`] for more details.
    ///
    /// # Errors
    ///
    /// Returns error if we failed to check for changes.
    pub async fn publish_watcher_changes(&mut self) -> Result<(), C::Error> {
        self.state
            .publish_changes_async(&mut self.connection, &self.watcher)
            .await
    }

    /// See [`Connection::stop_tracking`] for more details.
    ///
    /// # Errors
    ///
    /// Returns error if the queries failed.
    pub async fn stop_tracking(&mut self) -> Result<(), C::Error> {
        self.state
            .stop_tracking_async(&mut self.connection, &self.watcher)
            .await
    }

    /// Consume the current connection and take ownership of the real sql connection.
    ///
    /// # Remarks
    ///
    /// This does not stop the tracking infrastructure enabled on the connection.
    /// Use [`Self::stop_tracking()`] to disable it first.
    pub fn take(self) -> C {
        self.connection
    }
}

impl<C: SqlConnectionAsync> Deref for ConnectionAsync<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<C: SqlConnectionAsync> DerefMut for ConnectionAsync<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<C: SqlConnectionAsync> AsRef<C> for ConnectionAsync<C> {
    fn as_ref(&self) -> &C {
        &self.connection
    }
}

impl<C: SqlConnectionAsync> AsMut<C> for ConnectionAsync<C> {
    fn as_mut(&mut self) -> &mut C {
        &mut self.connection
    }
}

impl<C: SqlConnection> Deref for Connection<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<C: SqlConnection> DerefMut for Connection<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<C: SqlConnection> AsRef<C> for Connection<C> {
    fn as_ref(&self) -> &C {
        &self.connection
    }
}

impl<C: SqlConnection> AsMut<C> for Connection<C> {
    fn as_mut(&mut self) -> &mut C {
        &mut self.connection
    }
}

const TRACKER_TABLE_NAME: &str = "rsqlite_watcher_version_tracker";

const TRIGGER_LIST: [(&str, &str); 3] = [
    ("INSERT", "insert"),
    ("UPDATE", "update"),
    ("DELETE", "delete"),
];

#[inline]
fn create_tracking_table_query() -> String {
    format!("CREATE TEMP TABLE IF NOT EXISTS `{TRACKER_TABLE_NAME}` (table_id INTEGER PRIMARY KEY, updated INTEGER)")
}
#[inline]
fn empty_tracking_table_query() -> String {
    format!("DELETE FROM `{TRACKER_TABLE_NAME}`")
}
#[inline]
fn drop_tracking_table_query() -> String {
    format!("DROP TABLE IF EXISTS `{TRACKER_TABLE_NAME}`")
}

#[inline]
fn create_trigger_query(
    writer: &mut impl std::fmt::Write,
    table_name: &str,
    trigger: &str,
    trigger_name: &str,
    table_id: usize,
) {
    write!(
        writer,
        r#"
CREATE TEMP TRIGGER IF NOT EXISTS `{TRACKER_TABLE_NAME}_trigger_{table_name}_{trigger_name}` AFTER {trigger} ON `{table_name}`
BEGIN
    UPDATE  `{TRACKER_TABLE_NAME}` SET updated=1 WHERE table_id={table_id};
END
            "#
    )
        .expect("should not fail");
}

#[inline]
fn insert_table_id_into_tracking_table_query(writer: &mut impl std::fmt::Write, id: usize) {
    write!(writer, "INSERT INTO `{TRACKER_TABLE_NAME}` VALUES ({id},0)").expect("Should not fail");
}

#[inline]
fn drop_trigger_query(writer: &mut impl std::fmt::Write, table_name: &str, trigger_name: &str) {
    write!(
        writer,
        "DROP TRIGGER IF EXISTS `{TRACKER_TABLE_NAME}_trigger_{table_name}_{trigger_name}`"
    )
    .expect("should not fail");
}

#[inline]
fn remove_table_id_from_tracking_table_query(writer: &mut impl std::fmt::Write, table_id: usize) {
    write!(
        writer,
        "DELETE FROM `{TRACKER_TABLE_NAME}` WHERE table_id={table_id}"
    )
    .expect("should not fail");
}

#[inline]
fn select_updated_tables_query() -> String {
    format!("SELECT table_id  FROM `{TRACKER_TABLE_NAME}` WHERE updated=1")
}

#[inline]
fn reset_updated_tables_query() -> String {
    format!("UPDATE `{TRACKER_TABLE_NAME}` SET updated=0 WHERE updated=1")
}

/// Create tracking triggers for `table` with `id`.
///
/// # Errors
///
/// Return error if the query failed.
fn create_triggers<Ex: SqlExecutor>(
    executor: &mut Ex,
    table: &str,
    id: usize,
) -> Result<(), Ex::Error> {
    let mut query = String::with_capacity(64);
    for (trigger, trigger_name) in TRIGGER_LIST {
        query.clear();
        create_trigger_query(&mut query, table, trigger, trigger_name, id);
        executor.sql_execute(&query)?;
    }

    query.clear();
    insert_table_id_into_tracking_table_query(&mut query, id);
    executor.sql_execute(&query)?;
    Ok(())
}

/// Create tracking triggers for `table` with `id`.
///
/// # Errors
///
/// Return error if the query failed.
async fn create_triggers_async<Ex: SqlExecutorAsync>(
    executor: &mut Ex,
    table: &str,
    id: usize,
) -> Result<(), Ex::Error> {
    let mut query = String::with_capacity(64);
    for (trigger, trigger_name) in TRIGGER_LIST {
        query.clear();
        create_trigger_query(&mut query, table, trigger, trigger_name, id);
        executor.sql_execute(&query).await?;
    }

    query.clear();
    insert_table_id_into_tracking_table_query(&mut query, id);
    executor.sql_execute(&query).await?;
    Ok(())
}

/// Remove tracking triggers for `table` with `id`.
///
/// # Errors
///
/// Return error if the query failed.
fn drop_triggers<Ex: SqlExecutor>(
    executor: &mut Ex,
    table: &str,
    id: usize,
) -> Result<(), Ex::Error> {
    let mut query = String::with_capacity(64);
    for (_, trigger_name) in TRIGGER_LIST {
        query.clear();
        drop_trigger_query(&mut query, table, trigger_name);
        executor.sql_execute(&query)?;
    }
    query.clear();
    remove_table_id_from_tracking_table_query(&mut query, id);
    executor.sql_execute(&query)?;
    Ok(())
}

/// Remove tracking triggers for `table` with `id`.
///
/// # Errors
///
/// Return error if the query failed.
async fn drop_triggers_async<Ex: SqlExecutorAsync>(
    executor: &mut Ex,
    table: &str,
    id: usize,
) -> Result<(), Ex::Error> {
    let mut query = String::with_capacity(64);
    for (_, trigger_name) in TRIGGER_LIST {
        query.clear();
        drop_trigger_query(&mut query, table, trigger_name);
        executor.sql_execute(&query).await?;
    }
    query.clear();
    remove_table_id_from_tracking_table_query(&mut query, id);
    executor.sql_execute(&query).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::connection::State;
    use crate::watcher::{new_test_observer, ObservedTableOp, TableObserver, Watcher};
    use std::collections::BTreeSet;
    use std::sync::mpsc::{Receiver, SyncSender};
    use std::sync::Mutex;

    pub struct TestObserver {
        expected: Mutex<Vec<BTreeSet<String>>>,
        tables: Vec<String>,
        // Channel is here to make sure we don't trigger a merge of multiple pending updates.
        checked_channel: SyncSender<()>,
    }

    impl TestObserver {
        pub fn new(
            tables: Vec<String>,
            expected: impl IntoIterator<Item = BTreeSet<String>>,
        ) -> (Self, Receiver<()>) {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(0);
            let mut expected = expected.into_iter().collect::<Vec<_>>();
            expected.reverse();
            (
                Self {
                    expected: Mutex::new(expected),
                    tables,
                    checked_channel: sender,
                },
                receiver,
            )
        }
    }

    impl TableObserver for TestObserver {
        fn tables(&self) -> Vec<String> {
            self.tables.clone()
        }

        fn on_tables_changed(&self, tables: &BTreeSet<String>) {
            let expected = self.expected.lock().unwrap().pop().unwrap();
            assert_eq!(*tables, expected);
            self.checked_channel.send(()).unwrap();
        }
    }

    #[test]
    fn connection_state() {
        let service = Watcher::new().unwrap();

        let observer_1 = new_test_observer(["foo", "bar"]);
        let observer_2 = new_test_observer(["bar"]);
        let observer_3 = new_test_observer(["bar", "omega"]);

        let mut local_state = State::new();

        assert!(local_state.should_sync(&service).is_none());
        let observer_id_1 = service.add_observer(observer_1).unwrap();
        let foo_table_id = service.get_table_id("foo").unwrap();
        let bar_table_id = service.get_table_id("bar").unwrap();
        {
            let new_version = local_state
                .should_sync(&service)
                .expect("Should have new version");
            let (tracker, ops) = local_state
                .calculate_sync_changes(&service)
                .expect("must have changes");
            assert!(tracker[foo_table_id]);
            assert!(tracker[bar_table_id]);
            assert_eq!(ops.len(), 2);
            assert_eq!(
                ops[0],
                ObservedTableOp::Add("foo".to_string(), foo_table_id)
            );
            assert_eq!(
                ops[1],
                ObservedTableOp::Add("bar".to_string(), bar_table_id)
            );

            local_state.apply_sync_changes(tracker, new_version);
        }

        let observer_id_2 = service.add_observer(observer_2).unwrap();
        assert!(local_state.should_sync(&service).is_none());

        let observer_id_3 = service.add_observer(observer_3).unwrap();
        let omega_table_id = service.get_table_id("omega").unwrap();
        {
            let new_version = local_state
                .should_sync(&service)
                .expect("Should have new version");
            let (tracker, ops) = local_state
                .calculate_sync_changes(&service)
                .expect("must have changes");
            assert!(tracker[foo_table_id]);
            assert!(tracker[bar_table_id]);
            assert!(tracker[omega_table_id]);
            assert_eq!(ops.len(), 1);
            assert_eq!(
                ops[0],
                ObservedTableOp::Add("omega".to_string(), omega_table_id)
            );

            local_state.apply_sync_changes(tracker, new_version);
        }

        service.remove_observer(observer_id_2).unwrap();
        assert!(local_state.should_sync(&service).is_none());

        service.remove_observer(observer_id_3).unwrap();
        {
            let new_version = local_state
                .should_sync(&service)
                .expect("Should have new version");
            let (tracker, ops) = local_state
                .calculate_sync_changes(&service)
                .expect("must have changes");
            assert!(tracker[foo_table_id]);
            assert!(tracker[bar_table_id]);
            assert!(!tracker[omega_table_id]);
            assert_eq!(ops.len(), 1);
            assert_eq!(
                ops[0],
                ObservedTableOp::Remove("omega".to_string(), omega_table_id)
            );

            local_state.apply_sync_changes(tracker, new_version);
        }

        service.remove_observer(observer_id_1).unwrap();
        {
            let new_version = local_state
                .should_sync(&service)
                .expect("Should have new version");
            let (tracker, ops) = local_state
                .calculate_sync_changes(&service)
                .expect("must have changes");
            assert!(!tracker[foo_table_id]);
            assert!(!tracker[bar_table_id]);
            assert!(!tracker[omega_table_id]);
            assert_eq!(ops.len(), 2);
            assert_eq!(
                ops[0],
                ObservedTableOp::Remove("foo".to_string(), foo_table_id)
            );
            assert_eq!(
                ops[1],
                ObservedTableOp::Remove("bar".to_string(), bar_table_id)
            );

            local_state.apply_sync_changes(tracker, new_version);
        }
    }
}
