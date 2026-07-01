# Orleans.Lattice.Replication

Cross-cluster **active-active replication** for [Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice). Layers on top of the core package to move mutations between independent Orleans clusters, so any cluster can write to any tree and concurrent updates converge deterministically.

## What it gives you

- **Active-active, any-cluster writes** - every peer can accept writes to the same tree; there is no primary. Concurrent updates merge algebraically via the same CRDT lattice the core store uses.
- **Atomic visibility across clusters** - multi-key atomic writes stay all-or-nothing on every peer, never exposing a partial batch to a remote reader.
- **Per-tree write-ahead shipping** - a producer captures mutations off the source WAL and a shipper streams them to peers; apply is idempotent and order-tolerant.
- **Bootstrap & snapshot transfer** - a freshly-added cluster is seeded from a peer snapshot, then catches up from the live change stream.
- **Anti-entropy** - background Merkle-walk digest probes detect and repair divergence, with automatic, guarded drift remediation.
- **Pluggable transport** - the wire path is an interface; pair with [Orleans.Lattice.Replication.Grpc](https://www.nuget.org/packages/Orleans.Lattice.Replication.Grpc) for the canonical low-latency gRPC push transport.

## Getting started

Register replication alongside the core lattice and a durable WAL, then declare the peer topology. See the [replication overview](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.replication/README.md) for the full multi-cluster setup, transport wiring, security, and operations guide.
