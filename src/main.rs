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
    oneshot, Notify,
};

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
    WaitingPeers(usize),
    /// Syncing.
    /// - .0 is the best date processed
    /// - when the best date is equal to the target date,
    /// the sync is considered done.
    SyncProgress(NaiveDate),
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
    pub sync_tx: Sender<SyncCmd>,
    pub sync_rx: Receiver<SyncCmd>,
    pub gossipsub_tx: Sender<SyncMessage>,
    pub inbound_rx: Receiver<ProcessSyncMessageRequest>,
    pub next_date_notify: Notify,
    pub fs: NaiveFs,
}

impl Node {
    pub fn create_with_fs(
        fs: NaiveFs,
    ) -> (
        Self,
        Sender<SyncCmd>,
        Receiver<SyncMessage>,
        Sender<ProcessSyncMessageRequest>,
    ) {
        // channel to send and receive sync commands
        let (sync_tx, sync_rx) = channel(1);
        // channel to send and receive gossipsub messages
        let (gossipsub_tx, gossipsub_rx) = channel(DESIRED_PEERS + 1);
        // channels to communicate/receive responses from the mesh
        let (inbound_tx, inbound_rx) = channel(DESIRED_PEERS + 1);

        let this = Self {
            config: NodeConfig {
                sync_start_point: crate::filesystem::AVAILABLE_DATES[0],
                sync_end_point: crate::filesystem::AVAILABLE_DATES[29],
            },
            state: NodeState {
                sync: NodeSyncState::WaitingPeers(0),
                peers: Default::default(),
                date_states: HashMap::new(),
            },
            sync_tx: sync_tx.clone(),
            sync_rx,
            gossipsub_tx,
            inbound_rx,
            next_date_notify: Notify::new(),
            fs,
        };

        (this, sync_tx, gossipsub_rx, inbound_tx)
    }

    pub async fn process_sync_cmd(&mut self, cmd: SyncCmd) -> Result<(), Error> {
        tracing::debug!(?cmd, "Processing sync command");
        let NodeState {
            ref mut sync,
            peers: ref mut regular_peers,
            ref date_states,
            ..
        } = &mut self.state;
        match cmd {
            SyncCmd::AddPeer(peer_id) => {
                let NodeSyncState::WaitingPeers(ref mut available_peers) = sync else {
                    tracing::warn!(?cmd, state = ?self.state, "Bad state for this cmd");
                    return Ok(());
                };

                if *available_peers < DESIRED_PEERS && !regular_peers.contains(&peer_id) {
                    tracing::debug!(%peer_id, "adding peer to regular peers");
                    regular_peers.push(peer_id);
                    *available_peers += 1;
                }

                if *available_peers == DESIRED_PEERS {
                    tracing::info!("got all peers, starting sync");
                    *sync = NodeSyncState::SyncProgress(self.config.sync_start_point);
                    self.next_date_notify.notify_one();
                }
            }
            SyncCmd::AdvanceSync => {
                let NodeSyncState::SyncProgress(best) = sync else {
                    tracing::warn!(?cmd, state = ?self.state, "Bad state for this cmd");
                    return Ok(());
                };
                let next = best.succ_opt().unwrap();
                *best = next;
                if *best == self.config.sync_end_point {
                    tracing::info!("processed all dates, ending sync");
                    *sync = NodeSyncState::SyncDone;
                    let root_hash = self.fs.root_hash.read().await.to_hex();
                    tracing::info!(%root_hash, "Sync finished!");
                    for (date, state) in date_states.iter() {
                        tracing::info!(
                            processed_date = %date,
                            peers_responded = ?state.heard_peers,
                            dir_images_announced = %state.has_announced_dir_images,
                            missing_image_locations = ?state.image_locations,
                            "Stats for the date"
                        );
                    }

                    return Ok(());
                }
                tracing::debug!(%next, "notifying to process the next date");
                self.next_date_notify.notify_one();
            }
        }

        Ok(())
    }

    pub async fn process_message(
        &mut self,
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
                let local_date_state_ref = self.state.date_states.entry(parsed_date).or_default();

                let my_dir_hash = self.fs.dir_state(&parsed_date).await?;

                if my_dir_hash.to_hex().to_string() == announced_directory_hash
                    && local_date_state_ref.heard_peers.insert(from_peer_id)
                    && local_date_state_ref.heard_peers.len() == DESIRED_PEERS
                {
                    self.sync_tx
                        .send(SyncCmd::AdvanceSync)
                        .await
                        .expect("channel closed");
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

                if local_date_state_ref.heard_peers.insert(from_peer_id)
                    && local_date_state_ref.heard_peers.len() == DESIRED_PEERS
                {
                    self.sync_tx
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
                    if matches!(self.state.sync, NodeSyncState::SyncDone) {
                        tracing::info!("finished sync");
                        finish_tx.send(()).expect("finish channel closed");
                        break Ok(());
                    }
                },
                _ = self.next_date_notify.notified() => {
                    tracing::trace!("Requested to perform sync");

                    let NodeSyncState::SyncProgress(ref best_date) = self.state.sync else {
                        unreachable!();
                    };

                    let processing_date = best_date.succ_opt().unwrap();
                    tracing::trace!(%processing_date, "Processing sync for date");
                    let processing_date_pretty = processing_date.format(DATE_FORMAT_DIR).to_string();

                    let my_hash = self
                        .fs
                        .dir_state(&processing_date)
                        .await?;

                    self.gossipsub_tx
                        .send(SyncMessage::AnnounceDirectoryHash { date: processing_date_pretty, directory_hash: my_hash.to_hex().to_string() })
                        .await
                        .expect("channel closed");
                },
                inbound_msg = self.inbound_rx.recv() => {
                    tracing::debug!(?inbound_msg, "Processing inbound message");
                    let maybe_response = self.process_message(inbound_msg.expect("channel closed")).await?;
                    if let Some(response) = maybe_response {
                        tracing::trace!(?response, "want to reply");
                        self.gossipsub_tx.send(response).await.expect("channel closed");
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
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new("image-sync");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let fs = filesystem::NaiveFs::random();

    let (mut node, sync_tx, mut gossipsub_rx, inbound_tx) = Node::create_with_fs(fs);
    let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::task::spawn(async move {
        if let Err(e) = node.run(finish_tx).await {
            tracing::error!(%e, "Node exited with error");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(5_000)).await;
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
                        sync_tx.send(SyncCmd::AddPeer(peer_id)).await?;
                    }

                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    let received_message = serde_json::from_slice::<SyncMessage>(&message.data).expect("expected a valid message");
                    tracing::debug!(%id, from = %peer_id, ?received_message, "Got message from gossipsub");

                    inbound_tx.send(
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
