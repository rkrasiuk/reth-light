use futures::StreamExt;
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
    Error as DatabaseError,
};
use reth_interfaces::p2p::headers::downloader::{HeaderDownloader, SyncTarget};
use reth_primitives::{SealedHeader, H256};
use reth_provider::ProviderError;
use reth_stages::stages::{SyncGap, HEADERS};

pub struct HeadersSync<DB, H> {
    pub db: DB,
    header_downloader: H,
    tip: H256,
}

impl<DB: Database, H: HeaderDownloader> HeadersSync<DB, H> {
    pub fn new(db: DB, header_downloader: H, tip: H256) -> Self {
        Self { db, header_downloader, tip }
    }

    pub async fn run(&mut self) -> eyre::Result<()> {
        // Download headers
        let headers_progress = HEADERS.get_progress(&self.db.tx()?)?.unwrap_or_default();
        while let Some(gap) = self.get_sync_gap(headers_progress)? {
            if !gap.is_closed() {
                self.header_downloader.update_sync_gap(gap.local_head, gap.target);

                let headers =
                    self.header_downloader.next().await.ok_or(eyre::eyre!("channel closed"))?;
                tracing::trace!(target: "sync::headers", len = headers.len(), "Downloaded headers");
                self.db.update(|tx| {
                    let mut cursor_header = tx.cursor_write::<tables::Headers>()?;
                    let mut cursor_canonical = tx.cursor_write::<tables::CanonicalHeaders>()?;
                    for header in headers.clone().into_iter().rev() {
                        let header_hash = header.hash();
                        let header_number = header.number;
                        let header = header.unseal();

                        cursor_header.insert(header_number, header)?;
                        cursor_canonical.insert(header_number, header_hash)?;
                    }
                    HEADERS.save_progress(tx, headers.last().unwrap().number)?;
                    Ok::<(), DatabaseError>(())
                })??;
            }
        }

        let progress = HEADERS.get_progress(&self.db.tx()?)?.unwrap_or_default();
        tracing::trace!(target: "sync::headers", progress, "Finished syncing headers");

        Ok(())
    }

    fn get_sync_gap(&self, stage_progress: u64) -> eyre::Result<Option<SyncGap>> {
        let tx = self.db.tx()?;

        // Create a cursor over canonical header hashes
        let mut cursor = tx.cursor_read::<tables::CanonicalHeaders>()?;
        let mut header_cursor = tx.cursor_read::<tables::Headers>()?;

        // Get head hash and reposition the cursor
        let (head_num, head_hash) = cursor
            .seek_exact(stage_progress)?
            .ok_or(ProviderError::CanonicalHeader { block_number: stage_progress })?;

        // Construct head
        let (_, head) = header_cursor
            .seek_exact(head_num)?
            .ok_or(ProviderError::Header { number: head_num })?;
        let local_head = head.seal(head_hash);

        // Look up the next header
        let next_header = cursor
            .next()?
            .map(|(next_num, next_hash)| -> eyre::Result<SealedHeader> {
                let (_, next) = header_cursor
                    .seek_exact(next_num)?
                    .ok_or(ProviderError::Header { number: next_num })?;
                Ok(next.seal(next_hash))
            })
            .transpose()?;

        let target = match next_header {
            Some(header) if stage_progress + 1 != header.number => SyncTarget::Gap(header),
            None => SyncTarget::Tip(self.tip),
            _ => return Ok(None),
        };

        Ok(Some(SyncGap { local_head, target }))
    }
}
