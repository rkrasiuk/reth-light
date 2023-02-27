use futures::{future, StreamExt, TryStreamExt};
use rayon::prelude::*;
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
    Error as DatabaseError,
};
use reth_executor::execution_result::{AccountChangeSet, AccountInfoChangeSet};
use reth_interfaces::p2p::{
    bodies::{downloader::BodyDownloader, response::BlockResponse},
    headers::downloader::{HeaderDownloader, SyncTarget},
};
use reth_primitives::{
    Address, ChainSpec, Hardfork, SealedBlock, SealedHeader, StorageEntry, H256, U256,
};
use reth_provider::{LatestStateProviderRef, ProviderError};
use reth_revm::database::{State, SubState};
use reth_stages::stages::SyncGap;
use std::sync::Arc;
use tracing::*;

use crate::uploader::GithubUploader;

pub struct StateSync<H, B, DB> {
    headers_db: DB,
    state_db: DB,
    header_downloader: H,
    body_downloader: B,
    uploader: GithubUploader,
    chain_spec: Arc<ChainSpec>,
    tip: H256,
}

impl<H: HeaderDownloader, B: BodyDownloader, DB: Database> StateSync<H, B, DB> {
    pub fn new(
        headers_db: DB,
        state_db: DB,
        header_downloader: H,
        body_downloader: B,
        uploader: GithubUploader,
        chain_spec: Arc<ChainSpec>,
        tip: H256,
    ) -> Self {
        Self {
            headers_db,
            state_db,
            header_downloader,
            body_downloader,
            uploader,
            chain_spec,
            tip,
        }
    }

    pub async fn run(&mut self) -> eyre::Result<()> {
        // Download headers
        let current_progress = 0; // TODO:
        let gap = self.get_sync_gap(current_progress).await?;
        if !gap.is_closed() {
            self.header_downloader
                .update_sync_gap(gap.local_head, gap.target);

            while let Some(headers) = self.header_downloader.next().await {
                self.headers_db.update(|tx| {
                    let mut cursor_header = tx.cursor_write::<tables::Headers>()?;
                    let mut cursor_canonical = tx.cursor_write::<tables::CanonicalHeaders>()?;
                    for header in headers.clone().into_iter().rev() {
                        let header_hash = header.hash();
                        let header_number = header.number;
                        let header = header.unseal();

                        cursor_header.insert(header_number, header)?;
                        cursor_canonical.insert(header_number, header_hash)?;
                    }
                    Ok::<(), DatabaseError>(())
                })??;
            }
        }

        // Download and execute bodies
        let mut latest = 0;
        let mut td = self.chain_spec.genesis.difficulty;
        while let Some(bodies) = self.body_downloader.try_next().await? {
            tracing::trace!(target: "sync", len = bodies.len(), "Downloaded bodies");
            let blocks = bodies
                .into_iter()
                .map(|body| -> eyre::Result<_> {
                    td += body.header().difficulty;
                    let block = match body {
                        BlockResponse::Empty(header) => SealedBlock {
                            header,
                            ..Default::default()
                        },
                        BlockResponse::Full(block) => block,
                    };
                    let senders = block
                        .body
                        .par_iter()
                        .map(|transaction| {
                            transaction.recover_signer().ok_or(eyre::eyre!(
                                "failed to recover sender for tx {}",
                                transaction.hash
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((block, senders, td.clone()))
                })
                .collect::<eyre::Result<Vec<_>>>()?;

            let (start, end) = (
                blocks.first().unwrap().0.number,
                blocks.last().unwrap().0.number,
            );
            latest = end;
            tracing::trace!(target: "sync", start, end, "Executing blocks");

            let tx = self.state_db.tx_mut()?;
            let mut state_provider = SubState::new(State::new(LatestStateProviderRef::new(&tx)));
            let mut changesets = Vec::with_capacity(blocks.len());
            for (block, senders, td) in blocks {
                let block_number = block.number;
                let changeset = reth_executor::executor::execute_and_verify_receipt(
                    &block.unseal(),
                    td,
                    Some(senders),
                    &self.chain_spec,
                    &mut state_provider,
                )
                .map_err(|error| {
                    eyre::eyre!("Execution error at block #{block_number}: {error:?}")
                })?;
                changesets.push((changeset, block_number));
            }
            tracing::trace!(target: "sync", start, end, "Executed blocks");

            // apply changes to plain database.
            for (results, block_number) in changesets.into_iter() {
                let spurious_dragon_active = self
                    .chain_spec
                    .fork(Hardfork::SpuriousDragon)
                    .active_at_block(block_number);
                // insert state change set
                for result in results.tx_changesets.into_iter() {
                    for (address, account_change_set) in result.changeset.into_iter() {
                        let AccountChangeSet {
                            account,
                            wipe_storage,
                            storage,
                        } = account_change_set;
                        trace!(target: "sync", ?address,  ?account, wipe_storage, "Applying account changeset");
                        self.apply_account_changeset(
                            &tx,
                            account,
                            address,
                            spurious_dragon_active,
                        )?;

                        // cast key to H256 and trace the change
                        let storage = storage
                            .into_iter()
                            .map(|(key, (old_value, new_value))| {
                                (H256(key.to_be_bytes()), old_value, new_value)
                            })
                            .collect::<Vec<_>>();

                        if wipe_storage {
                            // delete all entries
                            tx.delete::<tables::PlainStorageState>(address, None)?;

                            // insert storage changeset
                            for (key, _, new_value) in storage {
                                // old values are already cleared.
                                if new_value != U256::ZERO {
                                    tx.put::<tables::PlainStorageState>(
                                        address,
                                        StorageEntry {
                                            key,
                                            value: new_value,
                                        },
                                    )?;
                                }
                            }
                        } else {
                            // insert storage changeset
                            for (key, old_value, new_value) in storage {
                                let old_entry = StorageEntry {
                                    key,
                                    value: old_value,
                                };
                                let new_entry = StorageEntry {
                                    key,
                                    value: new_value,
                                };

                                // Always delete old value as duplicate table, put will not override it
                                tx.delete::<tables::PlainStorageState>(address, Some(old_entry))?;
                                if new_value != U256::ZERO {
                                    tx.put::<tables::PlainStorageState>(address, new_entry)?;
                                }
                            }
                        }
                    }
                    // insert bytecode
                    for (hash, bytecode) in result.new_bytecodes.into_iter() {
                        // make different types of bytecode. Checked and maybe even analyzed (needs to
                        // be packed). Currently save only raw bytes.
                        let bytecode = bytecode.bytes();
                        trace!(target: "sync", ?hash, ?bytecode, len = bytecode.len(), "Inserting bytecode");
                        tx.put::<tables::Bytecodes>(hash, bytecode[..bytecode.len()].to_vec())?;

                        // NOTE: bytecode bytes are not inserted in change set and can be found in
                        // separate table
                    }
                }

                // If there are any post block changes, we will add account changesets to db.
                for (address, changeset) in results.block_changesets.into_iter() {
                    trace!(target: "sync", ?address, "Applying block reward");
                    self.apply_account_changeset(&tx, changeset, address, spurious_dragon_active)?;
                }
            }
        }

        self.uploader.upload_state(latest).await?;

        Ok(())
    }

    /// Apply the changes from the changeset to a database transaction.
    pub fn apply_account_changeset<'a, TX: DbTxMut<'a>>(
        &self,
        tx: &TX,
        changeset: AccountInfoChangeSet,
        address: Address,
        has_state_clear_eip: bool,
    ) -> eyre::Result<()> {
        match changeset {
            AccountInfoChangeSet::Changed { new, .. } => {
                tx.put::<tables::PlainAccountState>(address, new)?;
            }
            AccountInfoChangeSet::Created { new } => {
                if has_state_clear_eip && new.is_empty() {
                    return Ok(());
                }
                tx.put::<tables::PlainAccountState>(address, new)?;
            }
            AccountInfoChangeSet::Destroyed { .. } => {
                tx.delete::<tables::PlainAccountState>(address, None)?;
            }
            AccountInfoChangeSet::NoChange => {}
        }
        Ok(())
    }

    /// Get the head and tip of the range we need to sync
    ///
    /// See also [SyncTarget]
    async fn get_sync_gap(&self, stage_progress: u64) -> eyre::Result<SyncGap> {
        let tx = self.headers_db.tx()?;

        // Create a cursor over canonical header hashes
        let mut cursor = tx.cursor_read::<tables::CanonicalHeaders>()?;
        let mut header_cursor = tx.cursor_read::<tables::Headers>()?;

        // Get head hash and reposition the cursor
        let (head_num, head_hash) =
            cursor
                .seek_exact(stage_progress)?
                .ok_or(ProviderError::CanonicalHeader {
                    block_number: stage_progress,
                })?;

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

        // Decide the tip or error out on invalid input.
        // If the next element found in the cursor is not the "expected" next block per our current
        // progress, then there is a gap in the database and we should start downloading in
        // reverse from there. Else, it should use whatever the forkchoice state reports.
        let target = match next_header {
            Some(header) if stage_progress + 1 != header.number => SyncTarget::Gap(header),
            None => SyncTarget::Tip(self.tip),
            _ => eyre::bail!("database corrupt. attempted to look up header #{stage_progress}"),
        };

        Ok(SyncGap { local_head, target })
    }
}
