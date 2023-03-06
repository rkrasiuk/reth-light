use futures::TryStreamExt;
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    models::{StoredBlockBody, StoredBlockOmmers, StoredBlockWithdrawals},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_interfaces::p2p::bodies::{downloader::BodyDownloader, response::BlockResponse};
use reth_primitives::{BlockNumber, SealedHeader};
use reth_provider::ProviderError;
use reth_stages::stages::BODIES;

use super::Tip;

pub struct BodiesSync<DB, B> {
    db: DB,
    downloader: B,
}

impl<DB: Database, B: BodyDownloader> BodiesSync<DB, B> {
    pub fn new(db: DB, downloader: B) -> Self {
        Self { db, downloader }
    }

    pub fn get_progress(&self) -> eyre::Result<BlockNumber> {
        Ok(BODIES.get_progress(&self.db.tx()?)?.unwrap_or_default())
    }

    pub fn get_last_body(&self) -> eyre::Result<StoredBlockBody> {
        let (_, body) = self
            .db
            .view(|tx| tx.cursor_read::<tables::BlockBodies>()?.last())??
            .ok_or(ProviderError::BlockBody { number: 0 })?;
        Ok(body)
    }

    pub async fn run(&mut self, tip: Tip) -> eyre::Result<()> {
        let progress = self.get_progress()?;

        if tip.number <= progress {
            tracing::info!(target: "sync::bodies", progress, tip = tip.number, "Nothing to sync");
            return Ok(())
        }

        let mut latest_block_number = progress;
        let start_block = progress + 1;
        self.downloader.set_download_range(start_block..tip.number + 1)?;
        tracing::trace!(target: "sync::bodies", progress = progress, "Commencing sync");

        while latest_block_number < tip.number {
            let bodies = self.downloader.try_next().await?.ok_or(eyre::eyre!("channel closed"))?;
            let last_body = self.get_last_body()?;
            let mut current_tx_id = last_body.start_tx_id + last_body.tx_count;

            let tx = self.db.tx_mut()?;
            let mut body_cursor = tx.cursor_write::<tables::BlockBodies>()?;
            let mut tx_cursor = tx.cursor_write::<tables::Transactions>()?;
            let mut ommers_cursor = tx.cursor_write::<tables::BlockOmmers>()?;
            let mut withdrawals_cursor = tx.cursor_write::<tables::BlockWithdrawals>()?;

            let range =
                bodies.first().unwrap().block_number()..=bodies.last().unwrap().block_number();
            tracing::trace!(target: "sync::bodies", ?range, "Inserting bodies");

            for response in bodies {
                let block_number = response.block_number();
                latest_block_number = block_number;

                match response {
                    BlockResponse::Full(block) => {
                        let body = StoredBlockBody {
                            start_tx_id: current_tx_id,
                            tx_count: block.body.len() as u64,
                        };
                        body_cursor.append(block_number, body)?;

                        for transaction in block.body {
                            tx_cursor.append(current_tx_id, transaction)?;
                            current_tx_id += 1;
                        }

                        if !block.ommers.is_empty() {
                            let ommers =
                                block.ommers.into_iter().map(SealedHeader::unseal).collect();
                            ommers_cursor.append(block_number, StoredBlockOmmers { ommers })?;
                        }

                        if let Some(withdrawals) = block.withdrawals {
                            if !withdrawals.is_empty() {
                                withdrawals_cursor
                                    .append(block_number, StoredBlockWithdrawals { withdrawals })?;
                            }
                        }
                    }
                    BlockResponse::Empty(_) => {
                        body_cursor.append(
                            block_number,
                            StoredBlockBody { start_tx_id: current_tx_id, tx_count: 0 },
                        )?;
                    }
                };
            }

            tracing::trace!(target: "sync::bodies", progress = latest_block_number, "Progress updated");
            BODIES.save_progress(&tx, latest_block_number)?;
        }

        tracing::trace!(target: "sync::bodies", progress = latest_block_number, "Finished syncing");
        Ok(())
    }
}
