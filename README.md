# Image syncer

## Design

This is a fully decentralized, libp2p stack based implementation of the test task.

- Gossipsub is used for pubsub in the network
- mDNS is used for peer discovery
- transport is TCP

## Flow

On startup, each peer connects to the swarm. After that, they wait for
the desired number of peers to be available. When that happens, they start
broadcasting their directory **hashes**, as well as receiving messages from the other
peers. If the hashes of the directories match, **image data does not get broadcasted**.

## Running

The desired number of peers is hardcoded to 3.
To see the thing work, just run:

```sh
RUST_LOG=info cargo r
```

in 3 different terminals.

To see the sync where all the data is the same across all peers, run:

```sh
RUST_LOG=info cargo r -- full
```

in 3 different terminals.

## Known issues

Unfortunately, this implementation is not perfect.
I did not find how to force the swarm to flush the connections, and the process sometimes hangs.
The stats are still printed though, meaning the node could report about the desired locations.

## Verifying correctness

It is sufficient to compare the would-be root hashes of the peers.
For the full fs, the value is `0036b4a7ce8b6bdfd2e513ae390f08002c5b3fac38d3f3ffe3f4a6f62eafa148`.
For random filesystems, the values are different, but each peer comes to the same value.
