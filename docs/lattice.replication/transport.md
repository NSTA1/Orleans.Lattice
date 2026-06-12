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
    public HybridLogicalClock? BlockedAtHlc { get; init; }
    public int? SuggestedBatchSize { get; init; }
    public int? PauseForMs { get; init; }
}
```

| `ReplicationBatch` member | Semantics |
|---|---|
| `TargetClusterId` | Stable identifier of the destination cluster. Implementations route the call by this value. Required: must be non-`null` and non-empty. |
| `TreeName` | Name of the local tree this batch was drawn from. Receivers dispatch their per-tree apply pipeline on this id; the per-origin high-water-mark dedup key is `(TreeName, OriginClusterId)`. Required: must be non-`null` and non-empty. |
| `OriginClusterId` | Stable identifier of the local (sending) cluster. Stamped on every captured `WalRecord` at commit time and surfaced on the batch so transports that frame entries themselves do not need to re-derive the origin from the payload. Required: must be non-`null` and non-empty. |
| `Payload` | Opaque, framed batch payload. The byte layout is the responsibility of the binary-framing seam (typically Orleans-serializer-encoded `WalRecord` records inside a versioned envelope). Implementations treat this as a black box - they do not parse, peek into, or otherwise interpret the bytes. May be empty (heartbeat or keep-alive batch). |

| `ReplicationAck` member | Semantics |
|---|---|
| `Accepted` | `true` when the receiver successfully received and processed the batch. Note that `Accepted` is `true` even when every entry in the batch was de-duplicated by the per-origin high-water-mark - dedup is a successful idempotent apply, not a rejection. `false` when the receiver rejected the batch outright (transport-level error, schema mismatch, unknown tree). |
| `HighestAppliedHlc` | The per-origin high-water-mark for `(TreeName, OriginClusterId)` after the receiver finished processing the batch. The sender advances its per-peer cursor strictly to this value when `Accepted` is `true`; when `Accepted` is `false` this value is undefined and the sender must not consume it. |
| `BlockedAtHlc` | Optional receiver-side blocked-floor pin (lowest HLC across every partially-staged atomic batch). The sender publishes this value to its local `IWalCursorRegistry` so the producer-side WAL GC AND-s `entry.Timestamp < blockedFloor` into its trim predicate; `null` means the receiver has no in-flight admissions for this tree (or is pre-Phase-9 and never stamped the slot). Strictly additive on the wire. |
| `SuggestedBatchSize` | Optional receiver-side flow-control hint: the largest per-tick batch the receiver would like the sender to ship next, in entries. The sender clamps to `[1, options.ShipBatchSize]`; `null` (or any value `<= 0`) means "no preference" and the sender resumes at its configured `ShipBatchSize` (the canonical re-acceleration signal). Strictly additive on the wire. |
| `PauseForMs` | Optional receiver-side flow-control hint: number of milliseconds the sender should pause before its next pump tick. Composes with the shipper's exponential-backoff retry budget via `max(currentBackoffDeadline, now + PauseForMs)` - a receiver-requested pause never shortens an in-progress backoff. `null` or `<= 0` means "no pause requested". Strictly additive on the wire. |

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

```csharp verify
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

The default registration is `NoOpReplicationTransport` - it validates routing fields, discards the payload, and returns `default(ReplicationAck)` (i.e. `Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero`). The sender's cursor stays put, which is exactly the right behaviour while the rest of the replication pipeline is being wired up but no real transport is configured. Production hosts replace it via standard DI:

```csharp verify
sealed class MyTransport : IReplicationTransport
{
    public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        => Task.FromResult(default(ReplicationAck));
}

static void Configure(IServiceCollection services)
{
    services.AddSingleton<IReplicationTransport, MyTransport>();
}
```

## Future implementations

The single-method seam is the contract upcoming binary-framing and gRPC-streaming-push items plug into. The binary-framing item hardens the byte layout *inside* `Payload`; the gRPC streaming push item drives `SendAsync` from a long-lived server-streaming RPC and surfaces flow control / reconnect / advance-strictly-on-ack at the transport boundary. Neither changes the call shape established here.

The wire format inside `Payload` is the concern of [`IReplicationBatchEncoder`](wire-format.md). The default registration is the Orleans-serializer-backed binary encoder; hosts swap to a different framing (JSON for HTTP debuggability, content-hash-prefixed for deduplication) by replacing the encoder registration via DI. Transports remain agnostic about which encoder produced the bytes.

The canonical sender + receiver pair ships in the `Orleans.Lattice.Replication.Grpc` sub-package - see [`grpc-push-transport.md`](grpc-push-transport.md) for topology, registration, and operations notes.

## Framing-only ship path

The shipper's outbound path is unconditionally framing-only. Every batch the shipper hands to `SendAsync` carries a populated `ReplicationBatch.EncodedEnvelope` (a fixed 32-byte header plus length-prefixed pre-encoded entry segments produced by `IReplicationBatchEncoder.EncodeFraming`). Each entry's bytes are the verbatim segment the WAL stored at append time via `IWalStorageProvider.ReadShippingAsync` - no per-tick re-encode through an envelope-level Orleans serializer call, and no producer-side typed-envelope path. `ReplicationBatch.Payload` and `ReplicationBatch.Envelope` remain on the contract for receiver-side and test-fixture compatibility, but the producer-side shipper writes only `EncodedEnvelope`.

Custom transports that want to consume the framing bytes directly read them off `ReplicationBatch.EncodedEnvelope`. There is no separate typed-transport interface or sender-side capability probe - the shipper does not branch on transport type at activation. Bytes-only transports (the default `NoOpReplicationTransport`, host-supplied HTTP-framed transports) lift the framing bytes off `EncodedEnvelope` and forward them as-is.

## Caveats

- **Transports do not interpret the payload.** A transport that needs to make a routing decision based on payload contents (e.g. shed-load on oversize batches) must do so via batch metadata that the framing seam exposes on the call site, not by parsing `Payload` itself. Cross-cutting concerns belong on the call envelope; the wire bytes stay opaque.
- **The ack envelope grows additively, never by breaking change.** New `[Id(n)]` slots backed by nullable defaults (the `BlockedAtHlc`, `SuggestedBatchSize`, and `PauseForMs` slots are the existing precedent) are safe to ship on either side of a peering independently because pre-existing receivers and senders decode the slot as `null`. The single-method `SendAsync` contract does not change. Receiver-side flow-control hints in particular are wired through a pluggable [`IReceiverFlowControlPolicy`](receiver-flow-control.md) seam.

## Metadata pass-through contract

The transport stays dumb about the entries it carries. Specifically, every `IReplicationTransport` implementation must preserve the causal-plus metadata slots on every `WalRecord` verbatim across a round-trip:

- `WalRecord.VectorClock` - the sparse `{originClusterId → HybridLogicalClock}` frontier captured at commit time.
- `WalRecord.DependencySummary` - initially aliased one-to-one with `VectorClock`; reserved as a distinct slot so a future Bloom-filter-shaped summary can ship without re-numbering the wire format.

The transport must not reorder entries, mutate either slot, synthesise an empty frontier when the producer left the slot `null` (legacy peers and pre-causal-plus entries decode `null` and the receiver treats that as the empty frontier), or merge the two slots together. Any normalisation, summary derivation, or merge belongs in the producer / receiver, never in the wire layer.

The contract is pinned by `TransportMetadataPassthroughContractTests` in both `Orleans.Lattice.Replication.Tests` (LoopbackTransport) and `Orleans.Lattice.Replication.Grpc.Tests` (GrpcPushTransport). A new transport implementation should ship a mirror of that fixture parameterised over its own seam.

## Content-hash payload-elision round trip (opt-in, default off)

The push transport above is one-way: one batch in, one ack out. The opt-in content-hash payload-elision feature adds a *second*, bidirectional exchange in front of the push so the shipper can avoid re-sending payloads a peer already holds. It is built on the same per-(tree, peer) content hashing that drives the default-off re-send-rate measurement (`ship.redundant_payloads`), but instead of merely counting redundant re-sends it elides them.

The exchange is a default-no-op method on the `IReplicationDigestProbeTransport` seam (the same bidirectional probe transport the anti-entropy digest probe uses), so existing transports compile and behave unchanged:

```text
public interface IReplicationDigestProbeTransport
{
    Task<ContentManifestResponse> ExchangeContentManifestAsync(
        string targetClusterId,
        ContentManifestRequest request,
        CancellationToken cancellationToken)
        => Task.FromResult(ContentManifestResponse.NotSupported);
}

public readonly record struct ContentManifestEntry
{
    public int EntryIndex { get; init; }
    public string Key { get; init; }
    public ulong ContentHash { get; init; }
    public HybridLogicalClock Hlc { get; init; }
}

public readonly record struct ContentManifestRequest
{
    public string TreeName { get; init; }
    public string OriginClusterId { get; init; }
    public IReadOnlyList<ContentManifestEntry> Entries { get; init; }
}

public readonly record struct ContentManifestResponse
{
    public bool ExchangeSupported { get; init; }
    public IReadOnlyList<int> MissingEntryIndices { get; init; }
    public HybridLogicalClock AdvancedHlc { get; init; }
}
```

### Flow

1. **Sender builds a manifest.** When `LatticeReplicationOptions.ContentHashDedupEnabled` and `ContentHashDedupElisionEnabled` are both set, the shipper hashes the value-carrying point-`Set` entries in the drained batch (FNV-1a 64-bit over op + key + range + value, the same digest the measurement uses) and advertises a `ContentManifestRequest` to the peer. Only eligible entries are manifested - range deletes, saga terminal marks, prepared atomic-batch entries, and zero-HLC entries are never placed in the manifest and always ship verbatim, so atomic-batch boundaries, causal-dependency gating, and per-origin FIFO are preserved.
2. **Receiver answers with the missing set.** For each manifest entry the receiver compares the advertised content hash against the content it has already applied for that key. An entry the receiver does not hold (or holds with a different hash) is reported in `MissingEntryIndices`. An entry the receiver already holds byte-identical is *not* missing - and if the manifest entry's `Hlc` is newer than the receiver's recorded clock for that key (the idempotent re-set of an identical value), the receiver advances its per-origin high-water-mark via a metadata-only apply and reports the advanced clock in `AdvancedHlc`, all without the payload travelling.
3. **Sender ships only the missing payloads.** The shipper drops every elided entry from the outbound batch and ships the remainder through the ordinary `IReplicationTransport.SendAsync` push, then advances its per-peer cursor past the whole originally-drained range (the receiver advanced its high-water-mark for the elided entries during the exchange). When every entry is elided no batch is shipped at all.

### Default-off and rolling-upgrade safety

The default `ExchangeContentManifestAsync` returns `ContentManifestResponse.NotSupported` (`ExchangeSupported = false`), so a transport (or peer) that has not implemented the pull-missing RPC reports "not supported" and the shipper permanently falls back - for the rest of the activation - to shipping the full batch verbatim, byte-identical to today. Capability is learned lazily per shipper activation: the first eligible batch attempts the exchange, and a "not supported" reply latches elision off until the grain re-activates. With elision disabled (the default) the exchange is never attempted and the wire bytes are identical to a build without the feature.

The elision runs on the strict-serial ship path only; a configured pipelining window (`ShipMaxInFlight > 1`) collapses to one while elision is enabled so each batch's exchange + ship + ack completes before the next batch is drained.

> **Honest scope.** The manifest engine, the shipper-side elision wiring, the capability gating, and the options/metrics/dashboard surface ship in this package and are covered by unit and in-process loopback tests. Carrying the `ExchangeContentManifestAsync` RPC across clusters - and advancing the remote receiver's high-water-mark for elided entries - requires a transport binding that implements the method (for example the gRPC binding); until one is wired in, the default no-op keeps every peering wire-identical to today.
