use std::{fs, path::Path};

use crate::{database::MDBX_DAT, remote::github::GithubRemoteStore};
use reth_db::{cursor::DbCursorRO, database::Database, tables, transaction::DbTx};
use reth_interfaces::p2p::{
    bodies::downloader::BodyDownloader, headers::downloader::HeaderDownloader,
};
use reth_provider::ProviderError;

mod headers_sync;
pub use headers_sync::HeadersSync;

mod state_sync;
pub use state_sync::StateSync;

pub async fn run_sync<DB: Database, H: HeaderDownloader, B: BodyDownloader>(
    mut headers_sync: HeadersSync<DB, H>,
    headers_db_path: &Path,
    mut state_sync: StateSync<DB, B>,
    state_sync_db_path: &Path,
    remote: GithubRemoteStore,
) -> eyre::Result<()> {
    headers_sync.run().await?;

    let (last_number, _) = headers_sync
        .db
        .view(|tx| tx.cursor_read::<tables::CanonicalHeaders>()?.last())??
        .ok_or(ProviderError::CanonicalHeader { block_number: 0 })?;

    // TODO: make non-blocking
    let headers_db_content = fs::read(headers_db_path.join(MDBX_DAT))?;
    remote
        .save(
            "headers.dat.gz",
            headers_db_content,
            &format!("headers snapshot at block #{last_number}"),
        )
        .await?;

    // state_sync.run(last_number).await?;

    Ok(())
}
