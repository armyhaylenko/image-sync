//! All fields are made public for the purposes of simplicity.

mod filesystem;

use arrayvec::ArrayVec;
use chrono::NaiveDate;
use futures::{channel::mpsc, StreamExt};
use std::time::Duration;

use libp2p::{
    gossipsub,
    mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
};
use tracing_subscriber::EnvFilter;

use filesystem::NaiveFs;

// enum NodeRole {
//     Boot,
//     Regular,
// }

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

/// This is a model of a node, very specifically tied to this test task.
struct Node {
    // config of sorts - could be abstracted into a separate struct but we don't care
    pub sync_start_point: NaiveDate,
    pub sync_end_point: NaiveDate,
    // bootnode is the reference point, which sends the first broadcast
    // pub role: NodeRole,
    // simulating the three friends here – we will wait to discover everyone through mdns
    pub peers: ArrayVec<PeerId, 2usize>,
    pub best_date: NaiveDate,
    // todo: really? that complicated?
    pub bump_date_tx: mpsc::Sender<()>,
    pub bump_date_rx: mpsc::Receiver<()>,
    pub fs: NaiveFs,
}

impl Node {}

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

    let _fs = filesystem::NaiveFs::random();

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
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
    // swarm.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new("image-sync");
    // subscribes to our topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // TODO: for this task we NEED a linear flow:
    // 1. wait for the required number of peers
    // 2. kick of communication based on date with all the peers
    // 2.1. for each peer,
    loop {
        tokio::select! {
            // Ok(Some(line)) = stdin.next_line() => {
            //     if let Err(e) = swarm
            //         .behaviour_mut().gossipsub
            //         .publish(topic.clone(), line.as_bytes()) {
            //         println!("Publish error: {e:?}");
            //     }
            // }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        // println!("current peers: {:?}", swarm.behaviour().gossipsub.all_peers().collect::<Vec<(&PeerId, Vec<&TopicHash>)>>());
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discover peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => println!(
                        "Got message: '{}' with id: {id} from peer: {peer_id}",
                        String::from_utf8_lossy(&message.data),
                    ),
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                }
                _ => {}
            }
        }
    }
}
