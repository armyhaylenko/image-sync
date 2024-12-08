//! All fields are made public for the purposes of simplicity.

mod error;
mod filesystem;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use arrayvec::ArrayVec;
use chrono::NaiveDate;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot, Notify,
};
use tokio::sync::{Mutex, RwLock};

use libp2p::{
    gossipsub, mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
};
use tracing_subscriber::EnvFilter;

use crate::error::Error;
use crate::filesystem::{NaiveFs, DATE_FORMAT_DIR, DATE_FORMAT_IMG};

const DESIRED_PEERS: usize = 2;

#[derive(Debug)]
enum SyncCmd {
    AddPeer(PeerId),
    AdvanceSync,
}

#[derive(Debug, Deserialize, Serialize)]
enum SyncMessage {
    AnnounceDirectoryHash {
        date: String,
        directory_hash: String,
    },
    AnnounceDirectoryImages {
        date: String,
        // vec of image formatted dates (which are the ids) and the respective hashes
        directory_images: Vec<(String, String)>,
    },
}

#[derive(Debug)]
struct ProcessSyncMessageRequest {
    pub my_peer_id: PeerId,
    pub from_peer_id: PeerId,
    pub message: SyncMessage,
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
    SyncProgress(NaiveDate),
    /// We synced everything and are shutting down.
    SyncDone,
}

#[derive(Debug)]
struct LocalDateState {
    has_announced_dir_images: AtomicBool,
    image_locations: HashMap<String, PeerId>,
    heard_peers: HashSet<PeerId>,
}

#[derive(Debug)]
struct NodeState {
    pub sync: NodeSyncState,
    pub regular_peers: ArrayVec<PeerId, DESIRED_PEERS>,
    pub date_states: Arc<RwLock<HashMap<NaiveDate, LocalDateState>>>,
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

#[derive(Debug, Clone)]
struct Senders {
    pub sync: Arc<Sender<SyncCmd>>,
    pub gossipsub: Arc<Sender<SyncMessage>>,
    pub inbound: Arc<Sender<ProcessSyncMessageRequest>>,
}

/// This is a model of a node, very specifically tied to this test task.
#[derive(Debug)]
struct Node {
    pub config: NodeConfig,
    pub state: Arc<Mutex<NodeState>>,
    pub senders: Senders,
    pub sync_rx: Receiver<SyncCmd>,
    pub inbound_rx: Receiver<ProcessSyncMessageRequest>,
    pub next_date_notify: Arc<Notify>,
    pub fs: Arc<NaiveFs>,
}

impl Node {
    pub fn create_with_fs(fs: NaiveFs) -> (Self, Senders, Receiver<SyncMessage>) {
        let (sync_tx, sync_rx) = channel(1);
        let (gossipsub_tx, gossipsub_rx) = channel(DESIRED_PEERS + 1);
        let (inbound_tx, inbound_rx) = channel(DESIRED_PEERS + 1);
        let senders = Senders {
            sync: Arc::new(sync_tx),
            gossipsub: Arc::new(gossipsub_tx),
            inbound: Arc::new(inbound_tx),
        };

        let this = Self {
            config: NodeConfig {
                sync_start_point: crate::filesystem::AVAILABLE_DATES[0],
                sync_end_point: crate::filesystem::AVAILABLE_DATES[29],
            },
            state: Arc::new(Mutex::new(NodeState {
                sync: NodeSyncState::WaitingPeers(AtomicUsize::new(0)),
                regular_peers: Default::default(),
                date_states: Arc::new(RwLock::new(HashMap::new())),
            })),
            senders: senders.clone(),
            sync_rx,
            inbound_rx,
            next_date_notify: Arc::new(Notify::new()),
            fs: Arc::new(fs),
        };

        (this, senders, gossipsub_rx)
    }

    pub async fn process_sync_cmd(&mut self, cmd: SyncCmd) -> Result<(), Error> {
        tracing::debug!(?cmd, "Processing sync command");
        let mut state = self.state.lock().await;
        let NodeState {
            ref mut sync,
            ref mut regular_peers,
            ref date_states,
            ..
        } = &mut *state;
        match cmd {
            SyncCmd::AddPeer(peer_id) => {
                if let NodeSyncState::WaitingPeers(ref mut available_peers) = sync {
                    if available_peers.load(Ordering::SeqCst) < DESIRED_PEERS {
                        if !regular_peers.contains(&peer_id) {
                            tracing::debug!(%peer_id, "adding peer to regular peers");
                            regular_peers.push(peer_id);
                            available_peers.fetch_add(1, Ordering::SeqCst);
                        }
                    }

                    if available_peers.load(Ordering::SeqCst) == DESIRED_PEERS {
                        tracing::info!("got all peers, starting sync");
                        *sync = NodeSyncState::SyncProgress(self.config.sync_start_point);
                        self.next_date_notify.notify_one();
                    }
                } else {
                    tracing::warn!(?cmd, ?state, "Bad state for this cmd");
                }
            }
            SyncCmd::AdvanceSync => {
                if let NodeSyncState::SyncProgress(best) = sync {
                    let next = best.succ_opt().unwrap();
                    *best = next;
                    tracing::debug!(%next, "notifying to process the next date");
                    if *best == self.config.sync_end_point {
                        tracing::info!("processed all dates, ending sync");
                        *sync = NodeSyncState::SyncDone;
                        let root_hash = self.fs.root_hash.read().await.to_hex();
                        tracing::info!(%root_hash, "Sync finished!");
                        let local_states = date_states.read().await;
                        for (date, state) in local_states.iter() {
                            tracing::info!(
                                processed_date = %date,
                                peers_responded = ?state.heard_peers,
                                dir_images_announced = %state.has_announced_dir_images.load(Ordering::SeqCst),
                                missing_image_locations = ?state.image_locations,
                                "Stats for the date"
                            );
                        }

                        return Ok(());
                    }
                    self.next_date_notify.notify_one();
                } else {
                    tracing::warn!(?cmd, ?state, "Bad state for this cmd");
                }
            }
        }

        Ok(())
    }

    pub async fn process_message(
        &self,
        message_request: ProcessSyncMessageRequest,
    ) -> Result<Option<SyncMessage>, Error> {
        let ProcessSyncMessageRequest {
            my_peer_id,
            from_peer_id,
            message,
        } = message_request;
        tracing::debug!(%my_peer_id, ?message, "Processing sync message");
        match message {
            SyncMessage::AnnounceDirectoryHash {
                date,
                directory_hash: announced_directory_hash,
            } => {
                let parsed_date = NaiveDate::parse_from_str(&date, DATE_FORMAT_DIR)
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let state = self.state.lock().await;
                let mut local_states = state.date_states.write().await;
                let local_date_state_ref =
                    local_states
                        .entry(parsed_date)
                        .or_insert_with(|| LocalDateState {
                            has_announced_dir_images: AtomicBool::new(false),
                            image_locations: HashMap::new(),
                            heard_peers: HashSet::new(),
                        });

                let my_dir_hash = self.fs.dir_state(&parsed_date).await?;

                if my_dir_hash.to_hex().to_string() == announced_directory_hash
                    && local_date_state_ref.heard_peers.insert(from_peer_id)
                    && local_date_state_ref.heard_peers.len() == DESIRED_PEERS
                {
                    self.senders
                        .sync
                        .send(SyncCmd::AdvanceSync)
                        .await
                        .expect("channel closed");
                    return Ok(None);
                }

                let did_announce_for_date = local_date_state_ref
                    .has_announced_dir_images
                    .load(Ordering::SeqCst);

                if did_announce_for_date {
                    // TODO
                    return Ok(None);
                }

                let my_images_for_this_date = self
                    .fs
                    .get_images_for_date(&parsed_date)
                    .await?
                    .into_iter()
                    .map(|(hash, image)| {
                        (
                            image.created_at.format(DATE_FORMAT_IMG).to_string(),
                            hash.to_hex().to_string(),
                        )
                    })
                    .collect();
                local_date_state_ref
                    .has_announced_dir_images
                    .store(true, Ordering::SeqCst);
                Ok(Some(SyncMessage::AnnounceDirectoryImages {
                    date,
                    directory_images: my_images_for_this_date,
                }))
            }
            SyncMessage::AnnounceDirectoryImages {
                date,
                directory_images,
            } => {
                let parsed_date = NaiveDate::parse_from_str(&date, DATE_FORMAT_DIR)
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let state = self.state.lock().await;
                let mut local_states = state.date_states.write().await;
                let local_date_state_ref =
                    local_states
                        .entry(parsed_date)
                        .or_insert_with(|| LocalDateState {
                            has_announced_dir_images: AtomicBool::new(false),
                            image_locations: HashMap::new(),
                            heard_peers: HashSet::new(),
                        });

                let have_images = self
                    .fs
                    .get_images_for_date(&parsed_date)
                    .await?
                    .into_iter()
                    .map(|(hash, _)| hash.to_hex().to_string())
                    .collect::<HashSet<String>>();
                let provided_images = HashSet::<String>::from_iter(
                    directory_images.into_iter().map(|(_, hash)| hash),
                );

                if local_date_state_ref.heard_peers.insert(from_peer_id)
                    && local_date_state_ref.heard_peers.len() == DESIRED_PEERS
                {
                    self.senders
                        .sync
                        .send(SyncCmd::AdvanceSync)
                        .await
                        .expect("channel closed");
                }
                for img_hash in have_images.difference(&provided_images) {
                    local_date_state_ref
                        .image_locations
                        .insert(img_hash.clone(), from_peer_id);
                }

                Ok(None)
            }
        }
    }

    pub async fn run(&mut self, finish_tx: oneshot::Sender<()>) -> Result<(), Error> {
        loop {
            tokio::select! {
                cmd = self.sync_rx.recv() => {
                    self.process_sync_cmd(cmd.expect("channel closed")).await?;
                    let state = self.state.lock().await;
                    if matches!(state.sync, NodeSyncState::SyncDone) {
                        tracing::info!("finished sync");
                        finish_tx.send(()).expect("finish channel closed");
                        tokio::time::sleep(tokio::time::Duration::from_millis(5_000)).await;
                        break Ok(());
                    }
                },
                _ = self.next_date_notify.notified() => {
                    tracing::info!("Requested to perform sync");
                    let state = self.state.lock().await;

                    let NodeSyncState::SyncProgress(ref best_date) = state.sync else {
                        unreachable!();
                    };

                    let processing_date = best_date.succ_opt().unwrap();
                    tracing::debug!(%processing_date, "Processing sync for date");
                    state.date_states.write().await.entry(
                        processing_date).or_insert_with(||
                        LocalDateState {
                            has_announced_dir_images: AtomicBool::new(false),
                            image_locations: HashMap::new(),
                            heard_peers: HashSet::new(),
                        },
                    );
                    let processing_date_pretty = processing_date.format(DATE_FORMAT_DIR).to_string();

                    let my_hash = self
                        .fs
                        .dir_state(&processing_date)
                        .await?;

                    self.senders
                        .gossipsub
                        .send(SyncMessage::AnnounceDirectoryHash { date: processing_date_pretty, directory_hash: my_hash.to_hex().to_string() })
                        .await
                        .expect("channel closed");
                },
                inbound_msg = self.inbound_rx.recv() => {
                    tracing::debug!(?inbound_msg, "processing msg");
                    let maybe_response = self.process_message(inbound_msg.expect("channel closed")).await?;
                    if let Some(response) = maybe_response {
                        tracing::debug!(?response, "want to reply");
                        self.senders.gossipsub.send(response).await.expect("channel closed");
                    }
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
        // .without_quic()
        .with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID.
            // let message_id_fn = |message: &gossipsub::Message| {
            //     let message_id = blake3::hash(&message.data);
            //     gossipsub::MessageId(message_id.as_bytes().to_vec())
            // };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                // .mesh_outbound_min(0)
                // .mesh_n_low(1)
                // .mesh_n(1)
                // .allow_self_origin(true)
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
                // .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
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
    // swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new("image-sync");
    // subscribes to our topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let fs = filesystem::NaiveFs::random();
    // tracing::debug!(?fs, "Created filesystem");
    tracing::debug!("Created filesystem");

    let (mut node, senders, mut gossipsub_rx) = Node::create_with_fs(fs);
    let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::task::spawn(async move {
        let result = node.run(finish_tx).await;
        tracing::warn!(?result, "task joined unexpetedly");
    });
    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        tracing::debug!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        tracing::debug!(%my_peer_id, expired_peer_id = %peer_id, "mDNS discover peer has expired");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed {
                    peer_id,
                    topic: subscribed_to_topic,
                })) => {
                    if subscribed_to_topic == topic.hash() {
                        tracing::info!(
                            %my_peer_id,
                            subscriber_peer_id = %peer_id,
                            subscribed_to_topic = %subscribed_to_topic.as_str(),
                            "New topic peer"
                        );
                        senders.sync.send(SyncCmd::AddPeer(peer_id)).await?;
                    }

                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    let received_message = serde_json::from_slice::<SyncMessage>(&message.data).expect("expected a valid message");
                    tracing::debug!(%id, from = %peer_id, ?received_message, "Got message from gossipsub");

                    senders.inbound.send(
                        ProcessSyncMessageRequest {
                            my_peer_id,
                            from_peer_id: peer_id,
                            message: received_message,
                        }
                    )
                    .await
                    .expect("channel closed");
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    tracing::info!("Local node is listening on {address}");
                }
                ev => {
                    tracing::debug!(?ev, "got other event");
                }
            },
            request = gossipsub_rx.recv() => {
                tracing::debug!(?request, "want to send request to network");
                let request_bytes = serde_json::to_vec(&request.expect("channel closed")).unwrap();

                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), request_bytes)
                {
                    tracing::error!(%e, "failed to send request to the mesh");
                }
            },
            _ = &mut finish_rx => {
                tracing::debug!("finish_rx triggered, bye!");
                break Ok(());
            },
        }
    }
}
