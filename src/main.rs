//! All fields are made public for the purposes of simplicity.

mod error;
mod filesystem;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use arrayvec::ArrayVec;
use chrono::NaiveDate;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
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
use crate::filesystem::{NaiveFs, DATE_FORMAT_DIR, DATE_FORMAT_IMG};

use self::filesystem::AVAILABLE_DATES;

const DESIRED_PEERS: usize = 2;

// the _peer_id fields are used to make the commands unique
// so that GS does not dedupe them.
#[derive(Debug, Deserialize, Serialize)]
enum SyncMessage {
    AnnounceDirectoryHash {
        _peer_id: PeerId,
        date: String,
        directory_hash: String,
    },
    AnnounceDirectoryImages {
        _peer_id: PeerId,
        date: String,
        directory_images: Vec<(String, String)>,
    },
    Finished {
        _peer_id: PeerId,
    },
}

#[derive(Debug)]
struct ProcessSyncMessageRequest {
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
    WaitingPeers(usize),
    SyncProgress,
    /// We synced everything and are shutting down.
    SyncDone,
}

/// Local state for a specific date in sync.
///
/// Since in our case the number of peers is predeterminted and
/// we trust them, we can record peer messages for this date,
/// and when either all the peers agree on the date and dir contents,
/// or annonce their own contents so we could sync, the date is considered
/// processes.
#[derive(Debug, Default)]
struct LocalDateState {
    has_announced_dir_images: bool,
    image_locations: HashMap<String, PeerId>,
    heard_peers: HashSet<PeerId>,
}

#[derive(Debug)]
struct NodeState {
    pub sync: NodeSyncState,
    pub peers: ArrayVec<PeerId, DESIRED_PEERS>,
    pub date_states: HashMap<NaiveDate, LocalDateState>,
    pub remaining_to_sync: usize,
}

/// Placeholder for potential extensibility.
#[derive(Debug)]
struct NodeConfig {
    pub sync_start_point: NaiveDate,
    pub sync_end_point: NaiveDate,
}

/// This is a model of a node, very specifically tied to this test task.
#[derive(Debug)]
struct Node {
    pub config: NodeConfig,
    pub state: NodeState,
    pub gossipsub_tx: Sender<SyncMessage>,
    pub inbound_rx: Receiver<ProcessSyncMessageRequest>,
    pub peers_ready: Notify,
    pub fs: NaiveFs,
}

impl Node {
    pub fn create_with_fs(
        fs: NaiveFs,
    ) -> (
        Self,
        Receiver<SyncMessage>,
        Sender<ProcessSyncMessageRequest>,
    ) {
        // channel to send and receive gossipsub messages
        let (gossipsub_tx, gossipsub_rx) = channel(100);
        // channels to communicate/receive responses from the mesh
        let (inbound_tx, inbound_rx) = channel(100);

        let this = Self {
            config: NodeConfig {
                sync_start_point: crate::filesystem::AVAILABLE_DATES[0],
                sync_end_point: crate::filesystem::AVAILABLE_DATES
                    [crate::filesystem::AVAILABLE_DATES.len() - 1],
            },
            state: NodeState {
                sync: NodeSyncState::WaitingPeers(0),
                peers: Default::default(),
                date_states: HashMap::new(),
                remaining_to_sync: AVAILABLE_DATES.len(),
            },
            gossipsub_tx,
            inbound_rx,
            peers_ready: Notify::new(),
            fs,
        };

        (this, gossipsub_rx, inbound_tx)
    }

    pub fn add_peer(&mut self, peer_id: PeerId) -> Result<(), Error> {
        let NodeSyncState::WaitingPeers(ref mut available_peers) = self.state.sync else {
            tracing::warn!(state = ?self.state, "Bad state to add peer");
            return Ok(());
        };

        if *available_peers < DESIRED_PEERS && !self.state.peers.contains(&peer_id) {
            tracing::debug!(%peer_id, "adding peer to regular peers");
            self.state.peers.push(peer_id);
            *available_peers += 1;
        }

        if *available_peers == DESIRED_PEERS {
            tracing::info!("got all peers, starting sync");
            self.state.sync = NodeSyncState::SyncProgress;
            self.peers_ready.notify_one();
        }

        Ok(())
    }

    pub async fn process_message(
        &mut self,
        my_peer_id: PeerId,
        message_request: ProcessSyncMessageRequest,
    ) -> Result<Option<SyncMessage>, Error> {
        let ProcessSyncMessageRequest {
            from_peer_id,
            message,
        } = message_request;
        tracing::debug!(%my_peer_id, ?message, "Processing sync message");
        match message {
            SyncMessage::AnnounceDirectoryHash {
                date,
                directory_hash: announced_directory_hash,
                ..
            } => {
                let parsed_date = NaiveDate::parse_from_str(&date, DATE_FORMAT_DIR)
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let local_date_state_ref = self.state.date_states.entry(parsed_date).or_default();

                let my_dir_hash = self.fs.dir_state(&parsed_date).await?;

                if my_dir_hash.to_hex().to_string() == announced_directory_hash {
                    local_date_state_ref.heard_peers.insert(from_peer_id);
                    if local_date_state_ref.heard_peers.len() == DESIRED_PEERS {
                        self.state.remaining_to_sync =
                            self.state.remaining_to_sync.saturating_sub(0);
                    }
                    return Ok(None);
                }

                if local_date_state_ref.has_announced_dir_images {
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
                local_date_state_ref.has_announced_dir_images = true;
                Ok(Some(SyncMessage::AnnounceDirectoryImages {
                    _peer_id: my_peer_id,
                    date,
                    directory_images: my_images_for_this_date,
                }))
            }
            SyncMessage::AnnounceDirectoryImages {
                date,
                directory_images,
                ..
            } => {
                let parsed_date = NaiveDate::parse_from_str(&date, DATE_FORMAT_DIR)
                    .map_err(|_| Error::InvalidDateFormat(date.clone()))?;
                let local_date_state_ref = self.state.date_states.entry(parsed_date).or_default();

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

                for img_hash in have_images.difference(&provided_images) {
                    local_date_state_ref
                        .image_locations
                        .insert(img_hash.clone(), from_peer_id);
                }
                local_date_state_ref.heard_peers.insert(from_peer_id);
                if local_date_state_ref.heard_peers.len() == DESIRED_PEERS {
                    self.state.remaining_to_sync = self.state.remaining_to_sync.saturating_sub(1);
                }

                Ok(None)
            }
            SyncMessage::Finished { _peer_id } => {
                tracing::info!(%from_peer_id, "Peer says it has synced");
                self.state.peers.retain(|p| *p != from_peer_id);
                Ok(None)
            }
        }
    }

    pub async fn run(&mut self, my_peer_id: PeerId) -> Result<(), Error> {
        loop {
            tokio::select! {
                _ = self.peers_ready.notified() => {
                    tracing::trace!("Requested to perform sync");

                    let mut processing_date = self.config.sync_start_point;
                    let end_date = self.config.sync_end_point;
                    while processing_date != end_date {
                        tracing::trace!(%processing_date, "Processing sync for date");
                        let processing_date_pretty = processing_date.format(DATE_FORMAT_DIR).to_string();
                        let my_hash = self
                            .fs
                            .dir_state(&processing_date)
                            .await?;

                        self.gossipsub_tx
                            .send(SyncMessage::AnnounceDirectoryHash {
                                _peer_id: my_peer_id,
                                date: processing_date_pretty,
                                directory_hash: my_hash.to_hex().to_string()
                            })
                            .await
                            .expect("channel closed");
                        processing_date = processing_date.succ_opt().unwrap();
                    }


                },
                inbound_msg = self.inbound_rx.recv() => {
                        tracing::debug!(?inbound_msg, "Processing inbound message");
                        let maybe_response = self
                            .process_message(my_peer_id, inbound_msg.expect("channel closed"))
                            .await?;
                        if let Some(response) = maybe_response {
                            tracing::trace!(?response, "want to reply");
                            self.gossipsub_tx
                                .send(response)
                                .await
                                .expect("channel closed");
                        }

                        // tracing::info!(?self.state.sync, ?self.state.peers, ?self.state.remaining_to_sync, "LAST STATE");
                        if self.state.remaining_to_sync == 0 && matches!(self.state.sync, NodeSyncState::SyncProgress) {
                            for (date, state) in self.state.date_states.iter() {
                                tracing::info!(
                                    processed_date = %date,
                                    peers_responded = ?state.heard_peers,
                                    dir_images_announced = %state.has_announced_dir_images,
                                    missing_image_locations = ?state.image_locations,
                                    "Stats for the date"
                                );
                            }
                            self.state.sync = NodeSyncState::SyncDone;
                            tracing::info!("saying bye");

                            self.gossipsub_tx
                                .send(SyncMessage::Finished {
                                    _peer_id: my_peer_id,
                                })
                                .await
                                .expect("channel closed");
                        }
                        if self.state.peers.len() == 0 && matches!(self.state.sync, NodeSyncState::SyncDone) {
                            tracing::info!("finished sync and processed all peer messages");
                            break Ok(());
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
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(|_m| {
                    use rand::RngCore;
                    let mut v: Vec<u8> = vec![0u8; 32];
                    rand::thread_rng().try_fill_bytes(&mut v).unwrap();
                    gossipsub::MessageId(v)
                })
                .build()?;

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

    // Tell the swarm to listen on bcast and a random, OS-assigned
    // port.
    let listener_id = swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new("image-sync");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let fs = filesystem::NaiveFs::random();

    let (mut node, mut gossipsub_rx, inbound_tx) = Node::create_with_fs(fs);
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
                        // swarm.behaviour_mut().gossipsub.(&peer_id);

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
                        node.add_peer(peer_id)?;
                    }

                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    let received_message =
                        serde_json::from_slice::<SyncMessage>(&message.data).expect("expected a valid message");
                    tracing::info!(%id, from = %peer_id, ?received_message, "Got message from gossipsub");

                    inbound_tx
                        .send(ProcessSyncMessageRequest {
                            from_peer_id: peer_id,
                            message: received_message,
                        })
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
                tracing::info!(?request, "want to send request to network");
                let request_bytes = serde_json::to_vec(&request.expect("channel closed")).unwrap();

                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), request_bytes)
                {
                    tracing::error!(%e, "failed to send request to the mesh");
                }
            },
            res = node.run(my_peer_id) => {
                tracing::info!(?res, "Node exited");
                break ();
            },
            _ = tokio::signal::ctrl_c() => {
                break ();
            }
        }
    }

    swarm.remove_listener(listener_id);

    Ok(())
}
