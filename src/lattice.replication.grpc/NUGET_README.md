# Orleans.Lattice.Replication.Grpc

gRPC streaming push transport for `Orleans.Lattice.Replication`. Replaces the default no-op transport with a sub-second-latency, HTTP/2-multiplexed implementation that frames batches with the canonical `IReplicationBatchEncoder` (Orleans binary serialization) directly into the gRPC stream's `IBufferWriter<byte>` - zero per-batch heap allocation on the hot path.

See [`docs/lattice.replication.grpc/README.md`](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.replication.grpc/README.md) for the full topology, security, and operations guide.
