use super::{descriptor::DatabaseDescriptor, DAT_GZ_EXT, MDBX_DAT};
use crate::remote::RemoteStore;
use itertools::Itertools;
use reth_db::{
    mdbx::{DatabaseFlags, Env, EnvKind, WriteMap},
    TableType,
};
use reth_primitives::ChainSpec;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Default)]
pub struct DatabaseInitializer {
    prefix: String,
    path: PathBuf,
}

impl DatabaseInitializer {
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_owned();
        self
    }

    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = path.as_ref().to_owned();
        self
    }

    pub async fn init(
        &self,
        remote: &RemoteStore,
        chain_spec: ChainSpec,
        descriptor: impl DatabaseDescriptor<Arc<Env<WriteMap>>>,
    ) -> eyre::Result<Arc<Env<WriteMap>>> {
        // Initialize local database (create if does not exist).
        let local = self.initialize_database(descriptor.default_tables())?;
        // Get database progress.
        let progress = descriptor.progress(Arc::clone(&local))?.unwrap_or_default();
        // Restore database if remote has more data.
        let db = self.restore_database(local, remote, progress).await?;
        descriptor.ensure_genesis(Arc::clone(&db), chain_spec)?;
        Ok(db)
    }

    async fn restore_database(
        &self,
        local: Arc<Env<WriteMap>>,
        remote: &RemoteStore,
        progress: u64,
    ) -> eyre::Result<Arc<Env<WriteMap>>> {
        let snapshots = remote.list(Some(&self.prefix)).await?;

        // Sort snapshots by key
        let snapshots = snapshots
            .into_iter()
            .map(|s| {
                let key = s.key().unwrap();
                (key.to_owned(), self.get_snapshot_progress(key))
            })
            .sorted_by_key(|s| s.1);
        // Filter snapshot by local progress
        let best_snapshot = snapshots.rev().next().filter(|s| s.1 > progress);

        if let Some((key, _)) = best_snapshot {
            drop(local);
            let contents = remote.retrieve(&key).await?.unwrap();
            fs::write(self.path.join(MDBX_DAT), contents)?;
            let db = Arc::new(Env::<WriteMap>::open(&self.path, EnvKind::RW)?);
            Ok(db)
        } else {
            Ok(local)
        }
    }

    fn initialize_database(
        &self,
        tables: &[(TableType, &str)],
    ) -> eyre::Result<Arc<Env<WriteMap>>> {
        std::fs::create_dir_all(&self.path)?;
        let db = Env::<WriteMap>::open(&self.path, EnvKind::RW)?;

        let tx = db.inner.begin_rw_txn()?;
        for (table_type, table) in tables {
            let flags = match table_type {
                TableType::Table => DatabaseFlags::default(),
                TableType::DupSort => DatabaseFlags::DUP_SORT,
            };
            tx.create_db(Some(table), flags)?;
        }
        tx.commit()?;

        Ok(Arc::new(db))
    }

    fn get_snapshot_progress(&self, key: &str) -> u64 {
        let key = key.strip_prefix(&self.prefix).unwrap();
        let key = key.strip_suffix(DAT_GZ_EXT).unwrap();
        let block: u64 = key.parse().unwrap();
        block
    }
}
