# Orleans.Lattice.Replication.Grpc

gRPC unary push transport for [`Orleans.Lattice.Replication`](https://www.nuget.org/packages/Orleans.Lattice.Replication). Replaces the default no-op transport with a sub-second-latency, HTTP/2-multiplexed implementation that frames batches with the canonical `IReplicationBatchEncoder` (Orleans binary serialization) directly into gRPC's serialization-context `IBufferWriter<byte>` - no intermediate per-batch payload buffer on the hot path.

## What it gives you

- **Low-latency push** - one unary call per batch, multiplexed over a cached HTTP/2 channel per peer, delivers mutation batches in sub-second time rather than polling.
- **Copy-free framing** - batches serialize straight into gRPC's buffer writer with no intermediate per-batch payload buffer (the marshaller's only per-call allocation is a small wrapper object).
- **Canonical wire format** - reuses the same `IReplicationBatchEncoder` and versioned Orleans serialization as the rest of Lattice, so encoders never diverge between transport and core.
- **Drop-in transport** - registers as the replication transport binding; no changes to producer, shipper, apply, or topology code.

## Documentation

See the [gRPC transport guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.replication.grpc/README.md) for the full topology, security (TLS/mTLS), and operations guide.
