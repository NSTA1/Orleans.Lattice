# Replication transport seam (`IReplicationTransport`)

`IReplicationTransport` is the public, pluggable seam over the on-the-wire delivery of replication batches between clusters. It frames the in-process call shape that the outbound shipper uses, decouples that call shape from the bytes-on-the-wire (which is the binary-framing seam's concern), and standardises the receiver-side acknowledgement that drives the sender's per-peer cursor advance.

The contract is intentionally narrow: one method, one batch in, one ack out. There is no per-peer state on the transport, no streaming multiplexing in the public surface, and no transport-specific error vocabulary leaking into the call site - those concerns belong to specific implementations.

## API

The interface and value types live in `Orleans.Lattice.Replication`:

```text
public interface IReplicationTransport
{
    Task<ReplicationAck> SendAsync(
        ReplicationBatch batch,
        CancellationToken cancellationToken);
}

public readonly record struct ReplicationBatch
{
    public string TargetClusterId { get; init; }
    public string TreeName { get; init; }
    public string OriginClusterId { get; init; }
    public ReadOnlyMemory<byte> Payload { get; init; }
}

public readonly record struct ReplicationAck
{
    public bool Accepted { get; init; }
    public HybridLogicalClock HighestAppliedHlc { get; init; }
}
```

| `ReplicationBatch` member | Semantics |
|---|---|
| `TargetClusterId` | Stable identifier of the destination cluster. Implementations route the call by this value. Required: must be non-`null` and non-empty. |
| `TreeName` | Name of the local tree this batch was drawn from. Receivers dispatch their per-tree apply pipeline on this id; the per-origin high-water-mark dedup key is `(TreeName, OriginClusterId)`. Required: must be non-`null` and non-empty. |
| `OriginClusterId` | Stable identifier of the local (sending) cluster. Stamped on every captured `ReplogEntry` at commit time and surfaced on the batch so transports that frame entries themselves do not need to re-derive the origin from the payload. Required: must be non-`null` and non-empty. |
| `Payload` | Opaque, framed batch payload. The byte layout is the responsibility of the binary-framing seam (typically Orleans-serializer-encoded `ReplogEntry` records inside a versioned envelope). Implementations treat this as a black box - they do not parse, peek into, or otherwise interpret the bytes. May be empty (heartbeat or keep-alive batch). |

| `ReplicationAck` member | Semantics |
|---|---|
| `Accepted` | `true` when the receiver successfully received and processed the batch. Note that `Accepted` is `true` even when every entry in the batch was de-duplicated by the per-origin high-water-mark - dedup is a successful idempotent apply, not a rejection. `false` when the receiver rejected the batch outright (transport-level error, schema mismatch, unknown tree). |
| `HighestAppliedHlc` | The per-origin high-water-mark for `(TreeName, OriginClusterId)` after the receiver finished processing the batch. The sender advances its per-peer cursor strictly to this value when `Accepted` is `true`; when `Accepted` is `false` this value is undefined and the sender must not consume it. |

`ReplicationBatch` is intentionally **not** Orleans-serialisable: it is the in-process call argument, not the on-the-wire envelope. Wire-format hardening - versioned envelopes, content framing, compression - happens inside `Payload` and is the binary-framing seam's concern. `ReplicationAck` **is** Orleans-serialisable (alias `olr.ak`) because the receiver returns it to the sender across whatever transport is in use, including in-cluster Orleans RPC bridges.

## Send semantics

Three concerns the transport composes for every call:

### 1. Idempotency at the batch boundary

Receivers de-duplicate re-deliveries by the per-origin `(TreeName, OriginClusterId, hlc)` high-water-mark, so a transport that retries a batch on transient failure must not cause double-apply. Implementations are free to retry as aggressively as their reliability story requires; the receiver-side dedup is the correctness guarantee.

### 2. Advance-cursor-on-ack

The sender advances its per-peer cursor strictly to `ReplicationAck.HighestAppliedHlc` when the ack is accepted - never to a value the sender chose locally. This is the canonical at-least-once-delivery, at-most-once-apply contract: a batch may be re-delivered, but the receiver's HWM is the only source of truth for "how far is this peer caught up?" The sender's cursor never overruns the receiver's actual progress.

A receiver that partial-applies a batch (some entries succeeded, some failed) returns the highest HLC it actually advanced its HWM to, and the sender resumes from there on the next call. There is no separate partial-apply error code on the ack envelope - the `HighestAppliedHlc` value already encodes the resume point.

### 3. Concurrency

Implementations are required to be safe for concurrent invocation across distinct `(TargetClusterId, TreeName)` pairs - the canonical outbound shipper fans out across peers and trees in parallel. Concurrent invocation against the *same* `(TargetClusterId, TreeName)` pair is implementation-defined; the canonical shipper serialises calls per pair and relies only on cross-pair concurrency, so transports do not need to add internal serialisation for that case.

## Validation

`SendAsync` throws `ArgumentException` when:

- `batch.TargetClusterId` is `null` or empty.
- `batch.TreeName` is `null` or empty.
- `batch.OriginClusterId` is `null` or empty.

`OperationCanceledException` is thrown when the supplied `CancellationToken` is already cancelled or fires during the send.

## Registration

`AddLatticeReplication` registers the default `IReplicationTransport` implementation as a silo-side singleton:

```text
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

The default registration is `NoOpReplicationTransport` - it validates routing fields, discards the payload, and returns `default(ReplicationAck)` (i.e. `Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero`). The sender's cursor stays put, which is exactly the right behaviour while the rest of the replication pipeline is being wired up but no real transport is configured. Production hosts replace it via standard DI:

```text
services.AddSingleton<IReplicationTransport, MyTransport>();
```

## Future implementations

The single-method seam is the contract upcoming binary-framing and gRPC-streaming-push items plug into. The binary-framing item hardens the byte layout *inside* `Payload`; the gRPC streaming push item drives `SendAsync` from a long-lived server-streaming RPC and surfaces flow control / reconnect / advance-strictly-on-ack at the transport boundary. Neither changes the call shape established here.

The wire format inside `Payload` is the concern of [`IReplicationBatchEncoder`](wire-format.md). The default registration is the Orleans-serializer-backed binary encoder; hosts swap to a different framing (JSON for HTTP debuggability, content-hash-prefixed for deduplication) by replacing the encoder registration via DI. Transports remain agnostic about which encoder produced the bytes.

## Caveats

- **Transports do not interpret the payload.** A transport that needs to make a routing decision based on payload contents (e.g. shed-load on oversize batches) must do so via batch metadata that the framing seam exposes on the call site, not by parsing `Payload` itself. Cross-cutting concerns belong on the call envelope; the wire bytes stay opaque.
- **The ack envelope is not extensible at this seam.** A future item that needs to surface receiver-side flow-control hints (e.g. "throttle to N batches/sec") will do so by extending `ReplicationAck` with new `[Id(n)]` slots backed by stable defaults so legacy receivers decode safely. The single-method `SendAsync` contract does not change.
