# Orleans.Lattice.Replication.Grpc

gRPC streaming push transport for [`Orleans.Lattice.Replication`](https://www.nuget.org/packages/Orleans.Lattice.Replication). Replaces the default no-op transport with a sub-second-latency, HTTP/2-multiplexed implementation that frames batches with the canonical `IReplicationBatchEncoder` (Orleans binary serialization) directly into the gRPC stream's `IBufferWriter<byte>` - zero per-batch heap allocation on the hot path.

## What it gives you

- **Low-latency push** - HTTP/2-multiplexed streaming delivers mutation batches to peers in sub-second time, rather than polling.
- **Allocation-free framing** - batches serialize straight into the stream's buffer writer with no intermediate per-batch heap allocation.
- **Canonical wire format** - reuses the same `IReplicationBatchEncoder` and versioned Orleans serialization as the rest of Lattice, so encoders never diverge between transport and core.
- **Drop-in transport** - registers as the replication transport binding; no changes to producer, shipper, apply, or topology code.

## Documentation

See the [gRPC transport guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.replication.grpc/README.md) for the full topology, security (TLS/mTLS), and operations guide.
