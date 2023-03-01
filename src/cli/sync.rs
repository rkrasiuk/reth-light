use crate::{
    cli::dirs::{HeadersDbPath, StateDbPath},
    database::{init_headers_db, init_state_db, split::SplitDatabase},
    remote::digitalocean::store::DigitalOceanStore,
    sync::{run_sync_with_snapshots, HeadersSync, StateSync, Tip},
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
    utils::get_single_header,
};
use reth_consensus::beacon::BeaconConsensus;
use reth_db::mdbx::{Env, WriteMap};
use reth_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use reth_network::{
    error::NetworkError, FetchClient, NetworkConfig, NetworkHandle, NetworkManager,
};
use reth_network_api::NetworkInfo;
use reth_primitives::{BlockHashOrNumber, ChainSpec, Head, H256};
use reth_provider::{BlockProvider, HeaderProvider, ShareableDatabase};
use reth_staged_sync::{utils::chainspec::genesis_value_parser, Config};
use reth_tasks::TaskExecutor;
use std::{path::PathBuf, sync::Arc};
use tracing::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
enum SyncEnum {
    Headers,
    State,
}

/// Start the node
#[derive(Debug, Parser)]
pub struct Command {
    // #[arg(value_enum)]
    // sync: SyncEnum,
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

        // Raise the fd limit of the process. Does not do anything on windows.
        raise_fd_limit();

        let mut config: Config = self.load_config()?;
        info!(target: "reth::cli", path = %self.config, "Configuration loaded");

        let remote =
            DigitalOceanStore::new("fra1".to_owned(), "reth-state-snapshots".to_owned()).await;

        info!(target: "reth::cli", headers_db = %self.headers_db, "Opening split database");
        let headers_db = init_headers_db(&self.headers_db, &remote, self.chain.clone()).await?;
        info!(target: "reth::cli", "Split database opened");

        let (consensus, _forkchoice_state_tx) =
            BeaconConsensus::builder().build(self.chain.clone());
        info!(target: "reth::cli", "Consensus engine initialized");

        self.init_trusted_nodes(&mut config);

        info!(target: "reth::cli", "Connecting to P2P network");
        let network_config =
            self.load_network_config(&config, headers_db.clone(), ctx.task_executor.clone());
        let network = self.start_network(network_config, &ctx.task_executor, ()).await?;
        info!(target: "reth::cli", peer_id = %network.peer_id(), local_addr = %network.local_addr(), "Connected to P2P network");

        ctx.task_executor.spawn(events::handle_events(
            Some(network.clone()),
            network.event_listener().map(Into::into),
        ));

        let fetch_client = network.fetch_client().await?;
        let tip = Tip::new(self.tip, self.fetch_tip(fetch_client.clone(), self.tip).await?);

        let db = SplitDatabase::new(
            &self.headers_db,
            headers_db,
            &self.state_db,
            init_state_db(&self.state_db, &remote, self.chain.clone(), tip).await?,
        );

        let fetch_client = Arc::new(fetch_client);
        let header_downloader = ReverseHeadersDownloaderBuilder::from(config.stages.headers)
            .build(fetch_client.clone(), consensus.clone())
            .into_task_with(&ctx.task_executor);
        let body_downloader = BodiesDownloaderBuilder::from(config.stages.bodies)
            .build(fetch_client.clone(), consensus.clone(), db.headers())
            .into_task_with(&ctx.task_executor);

        let headers_sync = HeadersSync::new(db.headers(), header_downloader);

        let state_sync =
            StateSync::new(db.state(), db.headers(), body_downloader, Arc::new(self.chain.clone()));

        // Run sync
        let (rx, tx) = tokio::sync::oneshot::channel();
        info!(target: "reth::cli", "Starting state sync");
        ctx.task_executor.spawn_critical_blocking("state sync task", async move {
            let res = run_sync_with_snapshots(headers_sync, state_sync, tip, remote, db).await;
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

    async fn fetch_tip(
        &self,
        fetch_client: FetchClient,
        tip: H256,
    ) -> Result<u64, reth_interfaces::Error> {
        info!(target: "reth::cli", ?tip, "Fetching tip block number from the network.");
        loop {
            match get_single_header(fetch_client.clone(), BlockHashOrNumber::Hash(tip)).await {
                Ok(tip_header) => {
                    info!(target: "reth::cli", ?tip, number = tip_header.number, "Successfully fetched tip block number");
                    return Ok(tip_header.number)
                }
                Err(error) => {
                    error!(target: "reth::cli", %error, "Failed to fetch the tip. Retrying...");
                }
            }
        }
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
