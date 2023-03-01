use futures::TryStreamExt;
use rayon::prelude::*;
use reth_db::{
    cursor::DbCursorRO,
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_executor::execution_result::{AccountChangeSet, AccountInfoChangeSet, ExecutionResult};
use reth_interfaces::p2p::bodies::{downloader::BodyDownloader, response::BlockResponse};
use reth_primitives::{
    Address, BlockNumber, ChainSpec, Hardfork, SealedBlock, StorageEntry, H256, U256,
};
use reth_provider::LatestStateProviderRef;
use reth_revm::database::{State, SubState};
use reth_stages::stages::EXECUTION;
use std::{ops::Range, sync::Arc};

use crate::database::provider::LatestSplitStateProvider;

pub struct StateSync<DB, B> {
    state_db: DB,
    headers_db: DB,
    body_downloader: B,
    chain_spec: Arc<ChainSpec>,
}

impl<DB: Database, B: BodyDownloader> StateSync<DB, B> {
    pub fn new(
        state_db: DB,
        headers_db: DB,
        body_downloader: B,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self { state_db, headers_db, body_downloader, chain_spec }
    }

    pub fn get_td(&self, block: BlockNumber) -> eyre::Result<U256> {
        if block == 0 {
            return Ok(self.chain_spec.genesis.difficulty)
        }

        let mut td = U256::ZERO;
        let tx = self.state_db.tx()?;
        for entry in tx.cursor_read::<tables::Headers>()?.walk_range(..=block)? {
            let (_, header) = entry?;
            td += header.difficulty;
        }
        Ok(td)
    }

    pub fn get_progress(&self) -> eyre::Result<BlockNumber> {
        Ok(EXECUTION.get_progress(&self.state_db.tx()?)?.unwrap_or_default())
    }

    pub async fn run(&mut self, range: Range<BlockNumber>) -> eyre::Result<()> {
        tracing::trace!(target: "sync", ?range, "Downloading bodies");
        self.body_downloader.set_download_range(range.clone())?;

        let mut latest = range.start;
        let mut td = self.get_td(range.start)?;

        while latest + 1 < range.end {
            let bodies =
                self.body_downloader.try_next().await?.ok_or(eyre::eyre!("channel closed"))?;
            tracing::trace!(target: "sync", len = bodies.len(), "Downloaded bodies");
            let blocks = bodies
                .into_iter()
                .map(|body| -> eyre::Result<_> {
                    td += body.header().difficulty;
                    let block = match body {
                        BlockResponse::Empty(header) => {
                            SealedBlock { header, ..Default::default() }
                        }
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

            let (start, end) = (blocks.first().unwrap().0.number, blocks.last().unwrap().0.number);
            latest = end;
            tracing::trace!(target: "sync", start, end, "Executing blocks");

            let tx = self.state_db.tx_mut()?;
            let headers_tx = self.headers_db.tx_mut()?;
            let mut state_provider =
                SubState::new(State::new(LatestSplitStateProvider::new(&headers_tx, &tx)));
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
                changesets.push((block_number, changeset));
            }
            tracing::trace!(target: "sync", start, end, "Executed blocks");

            // apply changes to plain database.
            for (block_number, result) in changesets.into_iter() {
                self.apply_state_changes(&tx, block_number, result)?;
            }

            EXECUTION.save_progress(&tx, latest)?;
            tx.commit()?;
            tracing::trace!(target: "sync", progress = latest, "Plain state updated");
        }

        // tracing::trace!(target: "sync", latest, "Uploading state");
        // self.uploader.upload_state(latest).await?;

        Ok(())
    }

    fn apply_state_changes<'a, Tx: DbTxMut<'a>>(
        &self,
        tx: &Tx,
        block: BlockNumber,
        result: ExecutionResult,
    ) -> eyre::Result<()> {
        let spurious_dragon_active =
            self.chain_spec.fork(Hardfork::SpuriousDragon).active_at_block(block);

        for result in result.tx_changesets.into_iter() {
            for (address, account_change_set) in result.changeset.into_iter() {
                let AccountChangeSet { account, wipe_storage, storage } = account_change_set;
                self.apply_account_changeset(tx, account, address, spurious_dragon_active)?;

                let storage = storage
                    .into_iter()
                    .map(|(key, (old_value, new_value))| {
                        (H256(key.to_be_bytes()), old_value, new_value)
                    })
                    .collect::<Vec<_>>();

                if wipe_storage {
                    tx.delete::<tables::PlainStorageState>(address, None)?;

                    for (key, _, new_value) in storage {
                        if new_value != U256::ZERO {
                            tx.put::<tables::PlainStorageState>(
                                address,
                                StorageEntry { key, value: new_value },
                            )?;
                        }
                    }
                } else {
                    for (key, old_value, new_value) in storage {
                        tx.delete::<tables::PlainStorageState>(
                            address,
                            Some(StorageEntry { key, value: old_value }),
                        )?;
                        if new_value != U256::ZERO {
                            tx.put::<tables::PlainStorageState>(
                                address,
                                StorageEntry { key, value: new_value },
                            )?;
                        }
                    }
                }
            }

            for (hash, bytecode) in result.new_bytecodes.into_iter() {
                let bytecode = bytecode.bytes();
                tx.put::<tables::Bytecodes>(hash, bytecode[..bytecode.len()].to_vec())?;
            }
        }

        for (address, changeset) in result.block_changesets.into_iter() {
            self.apply_account_changeset(tx, changeset, address, spurious_dragon_active)?;
        }
        Ok(())
    }

    /// Apply the changes from the changeset to a database transaction.
    fn apply_account_changeset<'a, Tx: DbTxMut<'a>>(
        &self,
        tx: &Tx,
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
                    return Ok(())
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
}
