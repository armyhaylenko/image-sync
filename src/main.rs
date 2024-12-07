//! All fields are made public for the purposes of simplicity.

mod error;
mod filesystem;

use arrayvec::ArrayVec;
use blake3::Hash;
use chrono::NaiveDate;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Notify,
};

use libp2p::{
    gossipsub, mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
};
use tracing_subscriber::EnvFilter;

use crate::error::Error;
use crate::filesystem::NaiveFs;

const DESIRED_PEERS: usize = 2;
const DATE_FORMAT: &str = "%Y-%m-%d";

#[derive(Debug)]
enum SyncCmd {
    AddPeer(PeerId),
    AdvanceSync,
    EndSync,
}

#[derive(Debug, Deserialize, Serialize)]
enum SyncRequestInner {
    // contains the date in DATE_FORMAT format
    GetDateDirHash(String),
    // contains the date in DATE_FORMAT format
    // contains a vector of hex-encoded image hashes that need to be retrieved
    GetDateImages(String, Vec<String>),
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncRequest {
    pub inner: SyncRequestInner,
}

#[derive(Debug, Deserialize, Serialize)]
enum SyncResponseInner {
    // contains the date in DATE_FORMAT format
    // contains the dir hash
    DateDirHash(String, String),
    // contains the date in DATE_FORMAT format
    // contains a vector of hex-encoded image hashes that need to be retrieved
    // contains a vector of actual image bytes sequences that were requested
    DateImages(String, Vec<String>, Vec<Vec<u8>>),
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncResponse {
    sender: PeerId,
    inner: SyncResponseInner,
}

#[derive(Debug, Deserialize, Serialize)]
enum SyncMessage {
    Request(SyncRequest),
    Response(SyncResponse),
}

/// Our sync is a process that has a start and an end.
/// These individual states break down the process.
#[derive(Debug)]
enum NodeSyncState {
    /// Waiting for mDNS to discover the peers.
    /// - .0 is the number of peers we have
    /// - when .0 == DESIRED_PEERS, we can advance.
    WaitingPeers(AtomicUsize),
    /// Syncing.
    /// - .0 is the best date processed
    /// - .1 is the desired date
    /// - when .0 == .1, we can advance to the next state.
    /// - if .0 is None, it means the sync has not progressed yet.
    SyncProgress(Option<NaiveDate>, NaiveDate),
    /// We synced everything and are shutting down.
    SyncDone,
}

#[derive(Debug)]
struct NodeState {
    pub sync: NodeSyncState,
    pub regular_peers: ArrayVec<PeerId, DESIRED_PEERS>,
}

/// logic here
/// we have 3 nodes – [boot, p1, p2]
///
/// the sync process for one day is as follows:
/// - bootnode announces sync day to peers
/// - p1 & p2 make requests to the bootnode to determine the differences of state between boot<->p1
/// and boot<->p2
/// - bootnode exchanges the data with p1 and p2 (bidirectionally) so that their state is equal
/// - when all network participants signal sync ok for the day, bootnode starts the process for the
/// next day, continuing up to & including the end point.

#[derive(Debug)]
struct NodeConfig {
    pub sync_start_point: NaiveDate,
    pub sync_end_point: NaiveDate,
}

/// This is a model of a node, very specifically tied to this test task.
#[derive(Debug)]
struct Node {
    pub config: NodeConfig,
    pub state: Arc<Mutex<NodeState>>,
    pub sync_tx: Arc<Sender<SyncCmd>>,
    pub sync_rx: Receiver<SyncCmd>,
    pub outbound_tx: Sender<SyncMessage>,
    pub inbound_rx: Receiver<SyncMessage>,
    pub next_date_notify: Arc<Notify>,
    pub sync_end_notify: Arc<Notify>,
    pub fs: Arc<NaiveFs>,
}

impl Node {
    pub fn create_with_fs(
        fs: NaiveFs,
    ) -> (
        Self,
        Arc<Sender<SyncCmd>>,
        Receiver<SyncMessage>,
        Sender<SyncMessage>,
    ) {
        let (sync_tx, sync_rx) = channel(1);
        let (outbound_tx, outbound_rx) = channel(1);
        let (inbound_tx, inbound_rx) = channel(1);
        let sync_tx = Arc::new(sync_tx);
        let this = Self {
            config: NodeConfig {
                sync_start_point: crate::filesystem::AVAILABLE_DATES[0],
                sync_end_point: crate::filesystem::AVAILABLE_DATES[29],
            },
            state: Arc::new(Mutex::new(NodeState {
                sync: NodeSyncState::WaitingPeers(AtomicUsize::new(0)),
                regular_peers: Default::default(),
            })),
            sync_tx: sync_tx.clone(),
            sync_rx,
            outbound_tx,
            inbound_rx,
            next_date_notify: Arc::new(Notify::new()),
            sync_end_notify: Arc::new(Notify::new()),
            fs: Arc::new(fs),
        };

        (this, sync_tx, outbound_rx, inbound_tx)
    }

    pub async fn process_sync_cmd(&mut self, cmd: SyncCmd) -> Result<(), Error> {
        tracing::debug!(?cmd, "Processing sync command");
        let mut state = self.state.lock().await;
        let NodeState {
            ref mut sync,
            ref mut regular_peers,
        } = &mut *state;
        match cmd {
            SyncCmd::AddPeer(peer_id) => {
                if let NodeSyncState::WaitingPeers(ref mut available_peers) = sync {
                    if available_peers.load(std::sync::atomic::Ordering::SeqCst) < DESIRED_PEERS {
                        if !regular_peers.contains(&peer_id) {
                            tracing::debug!(%peer_id, "adding peer to regular peers");
                            regular_peers.push(peer_id);
                            available_peers.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }

                    if available_peers.load(std::sync::atomic::Ordering::SeqCst) == DESIRED_PEERS {
                        tracing::info!("got all peers, starting sync");
                        *sync = NodeSyncState::SyncProgress(None, self.config.sync_end_point);
                        self.next_date_notify.notify_one();
                    }
                } else {
                    tracing::warn!(?cmd, ?state, "Bad state for this cmd");
                }
            }
            SyncCmd::AdvanceSync => {
                if let NodeSyncState::SyncProgress(best, desired) = sync {
                    let processed_date = if let Some(ref best) = best {
                        *best
                    } else {
                        self.config.sync_start_point.pred_opt().unwrap()
                    };
                    if &processed_date == desired {
                        tracing::info!("processed all dates, ending sync");
                        self.sync_end_notify.notify_one();
                        return Ok(());
                    }
                } else {
                    tracing::warn!(?cmd, ?state, "Bad state for this cmd");
                }
            }
            SyncCmd::EndSync => {
                let root_hash = self.fs.root_hash.read().await.to_hex();
                tracing::info!(%root_hash, "Sync finished!");
                *sync = NodeSyncState::SyncDone;
            }
        }

        Ok(())
    }

    pub async fn process_request(
        &self,
        my_peer_id: PeerId,
        request: SyncRequest,
    ) -> Result<SyncResponse, Error> {
        tracing::debug!(?request, "Processing request");
        match request.inner {
            SyncRequestInner::GetDateDirHash(date) => {
                let parsed_date = NaiveDate::parse_from_str(&date, "DATE_FORMAT")
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let dir_hash = self.fs.dir_state(&parsed_date).await?;
                Ok(SyncResponse {
                    sender: my_peer_id,
                    inner: SyncResponseInner::DateDirHash(date, dir_hash.to_hex().to_string()),
                })
            }
            SyncRequestInner::GetDateImages(date, hex_hashes) => {
                let parsed_date = NaiveDate::parse_from_str(&date, "DATE_FORMAT")
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let hashes = hex_hashes
                    .iter()
                    .map(|h| Hash::from_hex(h.clone()))
                    .collect::<Result<Vec<Hash>, _>>()
                    .map_err(|_| Error::InvalidHashFormat)?;
                let images = self.fs.get_images_by_hashes(parsed_date, &hashes).await?;

                Ok(SyncResponse {
                    sender: my_peer_id,
                    inner: SyncResponseInner::DateImages(
                        date,
                        hex_hashes,
                        images.into_iter().map(|i| i.data).collect(),
                    ),
                })
            }
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                cmd = self.sync_rx.recv() => {
                    self.process_sync_cmd(cmd.expect("channel closed")).await?;
                    let state = self.state.lock().await;
                    if matches!(state.sync, NodeSyncState::SyncDone) {
                        tracing::info!("finished sync");
                        break Ok(())
                    }
                },
                _ = self.next_date_notify.notified() => {
                    tracing::info!("Requested to perform sync");
                    let state = self.state.lock().await;
                    let NodeSyncState::SyncProgress(ref best_date, _) = state.sync else {
                        unreachable!();
                    };
                    let best_date = best_date.clone().unwrap_or(self.config.sync_start_point);
                    self.outbound_tx.send(SyncMessage::Request(SyncRequest {
                        inner: SyncRequestInner::GetDateDirHash(best_date.format(DATE_FORMAT).to_string()),
                    })).await.expect("channel closed");
                    for _peer_id in state.regular_peers.iter() {
                        // let SyncResponse { sender, inner: response } = self.inbound_rx.recv().await.expect("channel closed");
                        // let SyncResponseInner::DateDirHash(date, hash) = response else {
                        //     unreachable!("concurrency is correct here");
                        // };

                        // tracing::info!(%sender, %date, %hash, "Got data from peer about directory");
                    }
                    self.sync_tx.send(SyncCmd::AdvanceSync).await.expect("channel closed");
                },
                _ = self.sync_end_notify.notified() => {
                    tracing::info!("State transitioned to sync end, finishing");
                    self.sync_tx.send(SyncCmd::EndSync).await.expect("channel closed");
                }
            }
        }
    }
}

// We create a custom network behaviour that combines Gossipsub and Mdns.
#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let key = libp2p::identity::Keypair::generate_ed25519();
    let my_peer_id = key.public().to_peer_id();
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let message_id = blake3::hash(&message.data);
                gossipsub::MessageId(message_id.as_bytes().to_vec())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()?;

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(MyBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))) // Allows us to observe pings indefinitely.
        .build();

    // Tell the swarm to listen on localhost and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new("image-sync");
    // subscribes to our topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let fs = filesystem::NaiveFs::random();
    tracing::debug!(?fs, "Created filesystem");

    let (mut node, sync_cmd_tx, mut outbound_rx, mut inbound_tx) = Node::create_with_fs(fs);

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        sync_cmd_tx.send(SyncCmd::AddPeer(peer_id)).await?;
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(_list))) => {
                    unimplemented!("handling expired peers is out of scope");
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    let message = serde_json::from_slice::<SyncMessage>(&message.data).expect("expected a valid message");
                    println!(
                        "Got message: '{:?}' with id: {id} from peer: {peer_id}",
                        message,
                    );

                    match message {
                        SyncMessage::Request(sync_request) => {
                            let response = node.process_request(my_peer_id, sync_request).await?;
                            inbound_tx.send(SyncMessage::Response(response)).await.expect("channel closed");
                        },
                        SyncMessage::Response(sync_response) => todo!(),
                    };

                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                }
                _ => {}
            },
            request = outbound_rx.recv() => {
                tracing::debug!(?request, "want to send request to network");
                let request_bytes = serde_json::to_vec(
                    &request.expect("channel closed")
                ).unwrap();
                if let Err(e) = swarm
                    .behaviour_mut().gossipsub
                    .publish(topic.clone(), request_bytes) {
                        tracing::error!(%e, "failed to send request to the mesh");
                    }
            },
            _ = node.run() => {
                tracing::warn!("task joined unexpectedly");
            }
        }
    }
}
