use crate::{database::MDBX_DAT, remote::digitalocean::store::DigitalOceanStore};
use reth_db::database::Database;
use reth_interfaces::p2p::{
    bodies::downloader::BodyDownloader, headers::downloader::HeaderDownloader,
};
use reth_primitives::{BlockNumber, H256};
use std::{fs, path::Path};

mod headers_sync;
pub use headers_sync::HeadersSync;

mod state_sync;
pub use state_sync::StateSync;

pub async fn run_sync<DB: Database, H: HeaderDownloader, B: BodyDownloader>(
    mut headers_sync: HeadersSync<DB, H>,
    headers_db_path: &Path,
    mut state_sync: StateSync<DB, B>,
    state_db_path: &Path,
    (tip_num, tip_hash): (BlockNumber, H256),
    remote: DigitalOceanStore,
) -> eyre::Result<()> {
    let last_number = headers_sync.get_last_header_number()?;

    if tip_num > last_number {
        headers_sync.run(tip_hash).await?;

        // TODO: make non-blocking
        let snapshot_key = format!("headers-{tip_num}.dat.gz");
        remote.save(&snapshot_key, &fs::read(headers_db_path.join(MDBX_DAT))?).await?;

        // Clean up any previous header entries
        for entry in remote.list(Some("headers-")).await? {
            let key = entry.key().unwrap();
            if !key.ends_with(&snapshot_key) {
                remote.delete(key).await?;
            }
        }
    }

    let snapshot_interval = 100_000;
    let mut sync_from = state_sync.get_progress()?;
    while sync_from <= last_number {
        let sync_until =
            last_number.min(sync_from + snapshot_interval - (sync_from % snapshot_interval));
        state_sync.run(sync_from..sync_until + 1).await?;
        sync_from = sync_until + 1;

        if sync_until != last_number ||
            (sync_until == last_number && last_number % snapshot_interval == 0)
        {
            tracing::trace!(target: "sync", block = sync_until, "Creating state snapshot");
            let snapshot_key = format!("state-snapshots/state-{sync_until}.dat.gz");
            remote.save(&snapshot_key, &fs::read(state_db_path.join(MDBX_DAT))?).await?;
        }
    }

    Ok(())
}
