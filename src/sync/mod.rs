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
    mut _state_sync: StateSync<DB, B>,
    _state_sync_db_path: &Path,
    (tip_num, tip_hash): (BlockNumber, H256),
    remote: DigitalOceanStore,
) -> eyre::Result<()> {
    let last_number = headers_sync.get_last_header_number()?;

    if tip_num > last_number {
        headers_sync.run(tip_hash).await?;

        // TODO: make non-blocking
        let headers_db_content = fs::read(headers_db_path.join(MDBX_DAT))?;
        let snapshot_path = format!("headers-{tip_num}.dat.gz");
        remote.save(&snapshot_path, &headers_db_content).await?;

        // Clean up any previous header entries
        for entry in remote.list(Some("headers-")).await? {
            let key = entry.key().unwrap();
            if !key.ends_with(&snapshot_path) {
                remote.delete(key).await?;
            }
        }
    }

    // state_sync.run(last_number).await?;

    Ok(())
}
