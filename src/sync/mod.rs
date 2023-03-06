use std::path::Path;

use crate::{
    database::{SplitDatabase, BODIES_PREFIX, DAT_GZ_EXT, HEADERS_PREFIX, MDBX_DAT, STATE_PREFIX},
    remote::RemoteStore,
};
use reth_db::database::Database;
use reth_interfaces::p2p::{
    bodies::downloader::BodyDownloader, headers::downloader::HeaderDownloader,
};
use reth_primitives::{BlockNumber, H256};

mod headers_sync;
pub use headers_sync::HeadersSync;

mod bodies_sync;
pub use bodies_sync::BodiesSync;

mod state_sync;
pub use state_sync::StateSync;

#[derive(Debug, Clone, Copy)]
pub struct Tip {
    hash: H256,
    number: BlockNumber,
}

impl Tip {
    pub fn new(hash: H256, number: BlockNumber) -> Self {
        Self { hash, number }
    }
}

pub async fn run_sync_with_snapshots<'a, DB: Database, H: HeaderDownloader, B: BodyDownloader>(
    mut headers_sync: HeadersSync<DB, H>,
    mut bodies_sync: BodiesSync<DB, B>,
    mut state_sync: StateSync<'a, DB>,
    tip: Tip,
    remote: RemoteStore,
    db: SplitDatabase,
) -> eyre::Result<()> {
    let last_headers_progress = headers_sync.get_progress()?;
    headers_sync.run(tip.clone()).await?;

    let new_headers_progress = headers_sync.get_progress()?;
    if new_headers_progress > last_headers_progress {
        save_single_snapshot(&remote, HEADERS_PREFIX, &db.headers_path, new_headers_progress)
            .await?;
    }

    let last_bodies_progress = bodies_sync.get_progress()?;
    bodies_sync.run(tip.clone()).await?;

    let new_bodies_progress = bodies_sync.get_progress()?;
    if new_bodies_progress > last_bodies_progress {
        save_single_snapshot(&remote, BODIES_PREFIX, &db.bodies_path, new_bodies_progress).await?;
    }

    let snapshot_interval = 100_000;
    let mut sync_from = state_sync.get_progress()? + 1;
    while sync_from <= tip.number {
        let sync_until =
            tip.number.min(sync_from + snapshot_interval - (sync_from % snapshot_interval));
        state_sync.run(sync_from..=sync_until).await?;
        sync_from = sync_until + 1;

        if sync_until != tip.number ||
            (sync_until == tip.number && tip.number % snapshot_interval == 0)
        {
            tracing::trace!(target: "sync", block = sync_until, "Creating state snapshot");
            let snapshot_key = format!("{STATE_PREFIX}{sync_until}{DAT_GZ_EXT}");
            let state_db_path = db.state_path.join(MDBX_DAT);
            remote.save(&snapshot_key, &state_db_path).await?;
        }
    }

    Ok(())
}

async fn save_single_snapshot(
    remote: &RemoteStore,
    prefix: &str,
    path: &Path,
    progress: BlockNumber,
) -> eyre::Result<()> {
    let snapshot_key = format!("{prefix}{progress}{DAT_GZ_EXT}");
    remote.save(&snapshot_key, &path.join(MDBX_DAT)).await?;

    // Clean up any previous snapshot entries
    for entry in remote.list(Some(prefix)).await? {
        let key = entry.key().unwrap();
        if !key.ends_with(&snapshot_key) {
            remote.delete(key).await?;
        }
    }
    Ok(())
}
