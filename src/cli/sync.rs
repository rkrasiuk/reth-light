use crate::{
    cli::dirs::{HeadersDbPath, StateDbPath},
    database::{init_headers_db, init_state_db},
    remote::{config::GithubStoreConfig, github::GithubRemoteStore},
    sync::{run_sync, HeadersSync, StateSync},
};
use clap::{crate_version, Parser, ValueEnum};
use eyre::Context;
use fdlimit::raise_fd_limit;
use futures::{pin_mut, StreamExt};
use reth::{
    args::NetworkArgs,
    dirs::{ConfigPath, PlatformPath},
    node::events,
    runner::CliContext,
};
use reth_consensus::beacon::BeaconConsensus;
use reth_db::mdbx::{Env, WriteMap};
use reth_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use reth_interfaces::{
    consensus::{Consensus, ForkchoiceState},
    p2p::{bodies::downloader::BodyDownloader, headers::downloader::HeaderDownloader},
};
use reth_network::{error::NetworkError, NetworkConfig, NetworkHandle, NetworkManager};
use reth_network_api::NetworkInfo;
use reth_primitives::{ChainSpec, Head, H256};
use reth_provider::{BlockProvider, HeaderProvider, ShareableDatabase};
use reth_staged_sync::{utils::chainspec::genesis_value_parser, Config};
use reth_tasks::TaskExecutor;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::watch;
use tracing::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
enum SyncMode {
    Headers,
    State,
}

/// Start the node
#[derive(Debug, Parser)]
pub struct Command {
    #[arg(value_enum)]
    mode: SyncMode,

    #[arg(long, value_name = "FILE", verbatim_doc_comment, default_value_t)]
    config: PlatformPath<ConfigPath>,

    #[arg(long, value_name = "PATH", verbatim_doc_comment, default_value_t)]
    headers_db: PlatformPath<HeadersDbPath>,

    #[arg(long, value_name = "PATH", verbatim_doc_comment, default_value_t)]
    state_db: PlatformPath<StateDbPath>,

    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "mainnet",
        value_parser = genesis_value_parser
    )]
    chain: ChainSpec,

    #[clap(flatten)]
    network: NetworkArgs,

    #[arg(long = "debug.tip", help_heading = "Debug")]
    tip: H256,
}

impl Command {
    /// Execute `node` command
    pub async fn execute(self, ctx: CliContext) -> eyre::Result<()> {
        info!(target: "reth::cli", "reth {} starting", crate_version!());

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit();

        let mut config: Config = self.load_config()?;
        info!(target: "reth::cli", path = %self.config, "Configuration loaded");

        let remote_store = GithubRemoteStore::new(GithubStoreConfig {
            email: "rokrassyuk@gmail.com".to_owned(),
            name: "Roman Krasiuk".to_owned(),
            owner: "rkrasiuk".to_owned(),
            repository: "reth-state-snapshots".to_owned(),
            token: std::env::var("GITHUB_TOKEN").expect("failed to read auth token"),
            agent: None,
        })?;

        info!(target: "reth::cli", headers_db = %self.headers_db, "Opening headers database");
        let headers_db =
            init_headers_db(&self.headers_db, &remote_store, self.chain.clone()).await?;
        info!(target: "reth::cli", "Headers database opened");

        let (consensus, _forkchoice_state_tx) = self.init_consensus()?;
        info!(target: "reth::cli", "Consensus engine initialized");

        self.init_trusted_nodes(&mut config);

        info!(target: "reth::cli", "Connecting to P2P network");
        let network_config =
            self.load_network_config(&config, Arc::clone(&headers_db), ctx.task_executor.clone());
        let network = self.start_network(network_config, &ctx.task_executor, ()).await?;
        info!(target: "reth::cli", peer_id = %network.peer_id(), local_addr = %network.local_addr(), "Connected to P2P network");

        ctx.task_executor.spawn(events::handle_events(
            Some(network.clone()),
            network.event_listener().map(Into::into),
        ));

        // TODO:
        let state_db = init_state_db(&self.state_db, &remote_store, self.chain.clone()).await?;
        let fetch_client = Arc::new(network.fetch_client().await?);
        let header_downloader = ReverseHeadersDownloaderBuilder::from(config.stages.headers)
            .build(fetch_client.clone(), consensus.clone())
            .into_task_with(&ctx.task_executor);

        let body_downloader = BodiesDownloaderBuilder::from(config.stages.bodies)
            .build(fetch_client.clone(), consensus.clone(), Arc::clone(&headers_db))
            .into_task_with(&ctx.task_executor);

        let headers_sync = HeadersSync::new(headers_db, header_downloader, self.tip);
        let state_sync = StateSync::new(state_db, body_downloader, Arc::new(self.chain.clone()));

        // Run sync
        let (rx, tx) = tokio::sync::oneshot::channel();
        info!(target: "reth::cli", "Starting state sync");
        ctx.task_executor.spawn_critical_blocking("state sync task", async move {
            let res = run_sync(
                headers_sync,
                self.headers_db.as_ref(),
                state_sync,
                self.state_db.as_ref(),
                remote_store,
            )
            .await;
            let _ = rx.send(res);
        });

        tx.await??;

        info!(target: "reth::cli", "State sync has finished.");

        Ok(())
    }

    fn load_config(&self) -> eyre::Result<Config> {
        confy::load_path::<Config>(&self.config).wrap_err("Could not load config")
    }

    fn init_trusted_nodes(&self, config: &mut Config) {
        config.peers.connect_trusted_nodes_only = self.network.trusted_only;

        if !self.network.trusted_peers.is_empty() {
            info!(target: "reth::cli", "Adding trusted nodes");
            self.network.trusted_peers.iter().for_each(|peer| {
                config.peers.trusted_nodes.insert(*peer);
            });
        }
    }

    fn init_consensus(&self) -> eyre::Result<(Arc<dyn Consensus>, watch::Sender<ForkchoiceState>)> {
        let (consensus, notifier) = BeaconConsensus::builder().build(self.chain.clone());

        debug!(target: "reth::cli", tip = %self.tip, "Tip manually set");
        notifier.send(ForkchoiceState {
            head_block_hash: self.tip,
            safe_block_hash: self.tip,
            finalized_block_hash: self.tip,
        })?;

        Ok((consensus, notifier))
    }

    /// Spawns the configured network and associated tasks and returns the [NetworkHandle] connected
    /// to that network.
    async fn start_network<C>(
        &self,
        config: NetworkConfig<C>,
        task_executor: &TaskExecutor,
        _pool: (),
    ) -> Result<NetworkHandle, NetworkError>
    where
        C: BlockProvider + HeaderProvider + Clone + Unpin + 'static,
    {
        let client = config.client.clone();
        let (handle, network, _txpool, eth) =
            NetworkManager::builder(config).await?.request_handler(client).split_with_handle();

        let known_peers_file = self.network.persistent_peers_file();
        task_executor.spawn_critical_with_signal("p2p network task", |shutdown| async move {
            run_network_until_shutdown(shutdown, network, known_peers_file).await
        });

        task_executor.spawn_critical("p2p eth request handler", async move { eth.await });

        Ok(handle)
    }

    fn load_network_config(
        &self,
        config: &Config,
        db: Arc<Env<WriteMap>>,
        executor: TaskExecutor,
    ) -> NetworkConfig<ShareableDatabase<Arc<Env<WriteMap>>>> {
        let head = Head {
            number: 0,
            hash: self.chain.genesis_hash(),
            timestamp: self.chain.genesis.timestamp,
            difficulty: self.chain.genesis.difficulty,
            total_difficulty: self.chain.genesis.difficulty,
        };
        self.network
            .network_config(config, self.chain.clone())
            .with_task_executor(Box::new(executor))
            .set_head(head)
            .build(ShareableDatabase::new(db, self.chain.clone()))
    }
}

/// Drives the [NetworkManager] future until a [Shutdown](reth_tasks::shutdown::Shutdown) signal is
/// received. If configured, this writes known peers to `persistent_peers_file` afterwards.
async fn run_network_until_shutdown<C>(
    shutdown: reth_tasks::shutdown::Shutdown,
    network: NetworkManager<C>,
    persistent_peers_file: Option<PathBuf>,
) where
    C: BlockProvider + HeaderProvider + Clone + Unpin + 'static,
{
    pin_mut!(network, shutdown);

    tokio::select! {
        _ = &mut network => {},
        _ = shutdown => {},
    }

    if let Some(file_path) = persistent_peers_file {
        let known_peers = network.all_peers().collect::<Vec<_>>();
        if let Ok(known_peers) = serde_json::to_string_pretty(&known_peers) {
            trace!(target : "reth::cli", peers_file =?file_path, num_peers=%known_peers.len(), "Saving current peers");
            match std::fs::write(&file_path, known_peers) {
                Ok(_) => {
                    info!(target: "reth::cli", peers_file=?file_path, "Wrote network peers to file");
                }
                Err(err) => {
                    warn!(target: "reth::cli", ?err, peers_file=?file_path, "Failed to write network peers to file");
                }
            }
        }
    }
}
