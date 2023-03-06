use crate::database::LatestSplitStateProvider;
use rayon::prelude::*;
use reth_db::{
    cursor::DbCursorRO,
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_executor::{
    execution_result::{AccountChangeSet, AccountInfoChangeSet, ExecutionResult},
    executor::Executor,
};
use reth_primitives::{Address, Block, BlockNumber, ChainSpec, Hardfork, StorageEntry, H256, U256};
use reth_provider::{test_utils::NoopProvider, ProviderError};
use reth_revm::database::{State, SubState};
use reth_stages::stages::EXECUTION;
use std::ops::RangeInclusive;

pub struct StateSync<'a, DB> {
    headers_db: DB,
    bodies_db: DB,
    state_db: DB,
    commit_threshold: u64,
    executor: Executor<'a, NoopProvider>,
}

impl<'a, DB: Database> StateSync<'a, DB> {
    pub fn new(
        headers_db: DB,
        bodies_db: DB,
        state_db: DB,
        commit_threshold: u64,
        chain_spec: ChainSpec,
    ) -> Self {
        Self {
            headers_db,
            bodies_db,
            state_db,
            commit_threshold,
            executor: Executor::from(chain_spec),
        }
    }

    pub fn get_td(&self, block: BlockNumber) -> eyre::Result<U256> {
        if block == 0 {
            return Ok(self.executor.chain_spec.genesis.difficulty)
        }

        let mut td = U256::ZERO;
        let tx = self.headers_db.tx()?;
        for entry in tx.cursor_read::<tables::Headers>()?.walk_range(..=block)? {
            let (_, header) = entry?;
            td += header.difficulty;
        }
        Ok(td)
    }

    pub fn get_progress(&self) -> eyre::Result<BlockNumber> {
        Ok(EXECUTION.get_progress(&self.state_db.tx()?)?.unwrap_or_default())
    }

    pub async fn run(&mut self, range: RangeInclusive<BlockNumber>) -> eyre::Result<()> {
        tracing::trace!(target: "sync::state", ?range, "Commencing state sync");

        let mut td = self.get_td(*range.start())?; // TODO:
        tracing::trace!(target: "sync::state", td = td.to_string(), "Total difficulty calculated");

        let mut progress = self.get_progress()?;
        while progress < *range.end() {
            let start = progress + 1;
            let range = start..=range.end().clone().min(start + self.commit_threshold);
            std::thread::scope(|scope| {
                let handle = std::thread::Builder::new()
                    .stack_size(50 * 1024 * 1024)
                    .spawn_scoped(scope, || self.execute_inner(range, &mut td))
                    .expect("Expects that thread name is not null");
                handle.join().expect("Expects for thread to not panic")
            })?;
            progress = self.get_progress()?;
        }

        Ok(())
    }

    fn execute_inner(&self, range: RangeInclusive<BlockNumber>, td: &mut U256) -> eyre::Result<()> {
        let headers_tx = self.headers_db.tx_mut()?;
        let bodies_tx = self.bodies_db.tx()?;
        let tx = self.state_db.tx_mut()?;

        tracing::trace!(target: "sync::state", ?range, "Retrieving bodies");
        let mut headers_cursor = headers_tx.cursor_read::<tables::Headers>()?;
        let mut bodies_cursor = bodies_tx.cursor_read::<tables::BlockBodies>()?;
        let mut ommers_cursor = bodies_tx.cursor_read::<tables::BlockOmmers>()?;
        let mut withdrawals_cursor = bodies_tx.cursor_read::<tables::BlockWithdrawals>()?;
        let mut tx_cursor = bodies_tx.cursor_read::<tables::Transactions>()?;

        let block_batch = headers_cursor
            .walk_range(range.clone())?
            .map(|entry| -> Result<_, eyre::Error> {
                let (number, header) = entry?;
                *td += header.difficulty;
                let (_, body) =
                    bodies_cursor.seek_exact(number)?.ok_or(ProviderError::BlockBody { number })?;
                let (_, stored_ommers) = ommers_cursor.seek_exact(number)?.unwrap_or_default();
                let withdrawals =
                    withdrawals_cursor.seek_exact(number)?.map(|(_, w)| w.withdrawals);
                Ok((header, td.clone(), body, stored_ommers.ommers, withdrawals))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut state_provider =
            SubState::new(State::new(LatestSplitStateProvider::new(&headers_tx, &tx)));
        let mut changesets = Vec::with_capacity(block_batch.len());
        for (header, td, body, ommers, withdrawals) in block_batch {
            let block_number = header.number;

            let mut tx_walker = tx_cursor.walk(Some(body.start_tx_id))?;
            let mut transactions = Vec::with_capacity(body.tx_count as usize);
            // get next N transactions.
            for index in body.tx_id_range() {
                let (tx_index, tx) =
                    tx_walker.next().ok_or(ProviderError::EndOfTransactionTable)??;
                if tx_index != index {
                    tracing::error!(target: "sync::stages::execution", block = header.number, expected = index, found = tx_index, ?body, "Transaction gap");
                    return Err(ProviderError::TransactionsGap { missing: tx_index }.into())
                }
                transactions.push(tx);
            }

            let senders = transactions
                .par_iter()
                .map(|transaction| {
                    transaction
                        .recover_signer()
                        .ok_or(eyre::eyre!("failed to recover sender for tx {}", transaction.hash))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let mut executor = self.executor.with_db(&mut state_provider);
            let changeset = executor
                .execute_and_verify_receipt(
                    &Block { header, body: transactions, ommers, withdrawals },
                    td,
                    Some(senders),
                )
                .map_err(|error| {
                    eyre::eyre!("Execution error at block #{block_number}: {error:?}")
                })?;
            changesets.push((block_number, changeset));
        }
        tracing::trace!(target: "sync::state", ?range, "Executed blocks");

        // apply changes to plain database.
        let mut latest = None;
        for (block_number, result) in changesets.into_iter() {
            latest = Some(block_number);
            self.apply_state_changes(&tx, block_number, result)?;
        }

        let latest = latest.unwrap();
        EXECUTION.save_progress(&tx, latest)?;
        tx.commit()?;
        tracing::trace!(target: "sync::state", progress = latest, "Plain state updated");
        Ok(())
    }

    fn apply_state_changes<'tx, Tx: DbTxMut<'tx>>(
        &self,
        tx: &Tx,
        block: BlockNumber,
        result: ExecutionResult,
    ) -> eyre::Result<()> {
        let spurious_dragon_active =
            self.executor.chain_spec.fork(Hardfork::SpuriousDragon).active_at_block(block);

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
    fn apply_account_changeset<'tx, Tx: DbTxMut<'tx>>(
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
