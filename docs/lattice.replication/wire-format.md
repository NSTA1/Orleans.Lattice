# Replication wire format (`IReplicationBatchEncoder`)

`IReplicationBatchEncoder` is the public, pluggable seam over the on-the-wire bytes that an outbound shipper stuffs into [`ReplicationBatch.Payload`](transport.md). It is the encode/decode counterpart to [`IReplicationTransport`](transport.md): the transport delivers opaque bytes between clusters, and the encoder is the only component that knows how to translate a batch of [`WalRecord`](change-feed.md) records to and from those bytes.

The default registration is a binary encoder that uses the Orleans serializer applied to a versioned envelope. Hosts that need a different framing - JSON for HTTP-transport debuggability, a custom envelope for compatibility with an external pipeline, content-hash-prefixed framing for deduplication - replace the registration via standard DI.

## API

The interface and value types live in `Orleans.Lattice.Replication`:

```text
public interface IReplicationBatchEncoder
{
    string ContentType { get; }
    int CurrentWireVersion { get; }
    void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer);
    ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload);
}

public readonly record struct ReplicationBatchEnvelope
{
    public int WireVersion { get; init; }
    public string TreeName { get; init; }
    public string OriginClusterId { get; init; }
    public IReadOnlyList<WalRecord> Entries { get; init; }

    public const int CurrentVersion = 1;
}
```

| `ReplicationBatchEnvelope` member | Semantics |
|---|---|
| `WireVersion` | The wire-format version this envelope was authored against. Receivers compare against `IReplicationBatchEncoder.CurrentWireVersion` and reject payloads carrying a strictly greater value rather than guess at the layout. Hand-constructed envelopes default to `0`; the canonical encoder stamps `CurrentVersion` at encode time when the caller supplies `0`. |
| `TreeName` | Logical tree id the entries were captured from. Mirrors `ReplicationBatch.TreeName` on the surrounding call envelope; receivers route the per-tree apply pipeline on this value. |
| `OriginClusterId` | Stable identifier of the originating cluster. Mirrors `ReplicationBatch.OriginClusterId` on the surrounding call envelope; receivers use it to attribute origin and break replication cycles. |
| `Entries` | The captured `WalRecord` records, in commit order. May be empty (heartbeat / keep-alive batch). Never `null` on a value produced by the canonical encoder; hand-constructed envelopes that leave this default decode as an empty list because the canonical decoder normalises `null` to `Array.Empty<WalRecord>()`. |

The envelope is Orleans-serialisable (alias `olr.be`); the call-shape `ReplicationBatch` is intentionally not. Wire-format hardening - versioned envelopes, content framing, compression - happens *inside* `ReplicationBatch.Payload`, and the envelope is the canonical shape that lives there.

## Why a versioned envelope

Future breaking changes to the on-the-wire shape - new top-level fields that older receivers must reject rather than silently discard, restructured `Entries` collections, alternate carrier formats for typed CRDT deltas - are signalled by bumping `WireVersion`. A receiver compares against its `CurrentWireVersion` strictly:

- `WireVersion <= CurrentWireVersion` → accepted.
- `WireVersion > CurrentWireVersion` → rejected with `NotSupportedException`.

This is the canonical fail-fast posture: an older receiver paired with a newer producer immediately surfaces a deployment-ordering bug rather than mis-applying a payload it cannot interpret. A newer receiver paired with an older producer is the "normal" mixed-version case during a rolling upgrade and decodes the payload directly.

Forward-compatible additions (new `[Id(n)]` slots on `WalRecord` with stable defaults, like the `Mode` slot stamped by the commit-time observer when a tree's replication mode is declared) do not require a wire-version bump: the Orleans serializer's per-field id model handles them transparently and unknown ids on a legacy receiver decode as the default value.

## `WalRecord.Value` strip on CRDT-mode entries

The canonical `OrleansBinaryWalRecordEncoder` strips the `[Id(4)] Value` slot on `MutationKind.Set` entries that satisfy both of the following at encode time:

- `Mode` is a typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, or `OrMap`), i.e. not `LwwRegister`.
- `Delta` is non-`null`.

The receiver-side apply path dispatches every typed CRDT mode through `WalRecord.Delta` and the primitive's `MergeDelta` operation, so the full-state `Value` byte payload is pure overhead on both the storage WAL and the cross-cluster wire (a single encode-at-append seam feeds both). The producer's in-grain `WalRecord` instance still carries `Value` in memory and the leaf store continues to hold the canonical post-merge state; this strip is scoped to the encoded bytes only.

`LwwRegister` entries are not affected: `Value` remains the canonical payload, and a `null` `Value` on a `LwwRegister` `Set` continues to be rejected by the receiver-side apply path as an `ArgumentException`. CRDT-mode entries that for whatever reason ship without a `Delta` (a legacy producer, a hand-constructed entry in a test) also retain `Value` verbatim - the strip is gated on `Delta` presence.

**Consumer impact on `IChangeFeed`:** consumers reading `WalRecord.Value` directly on CRDT-mode entries now observe `null`. Such consumers must either (a) read `Delta` and apply it against their own prior observed state via the matching primitive's `MergeDelta`, or (b) read the producer's leaf store via the public lattice surface (`ILattice.GetAsync` or the typed accessor). The `LwwRegister` consumer contract is unchanged.

This strip does not require a `WireVersion` bump: an absent `[Id]` slot decodes to the type's default (`null` for `byte[]?`), which is exactly the behaviour the receiver-side null-tolerance was prepared to accept.

## Default encoder: Orleans serializer (binary)

The default DI registration is `OrleansBinaryReplicationBatchEncoder`. It serialises the envelope through `Serializer<ReplicationBatchEnvelope>` and tags the payload with the canonical content type:

```text
application/x-orleans-lattice-replog+binary
```

The Orleans serializer is roughly 33% more compact than naive JSON for `byte[]` payloads (which is the common case for replication - every value committed through `ILattice.SetAsync` is a `byte[]`), and avoids JSON's base64 round-trip overhead. The encoder unit tests pin a regression floor on this with a "binary is smaller than `System.Text.Json.JsonSerializer.SerializeToUtf8Bytes`" assertion against a representative payload-heavy batch.

The encoder also enforces four invariants on encode:

- `writer` must be non-`null` (`ArgumentNullException` otherwise).
- `TreeName` and `OriginClusterId` must be non-empty (`ArgumentException` otherwise).
- `WireVersion` must be non-negative (`ArgumentException` otherwise).
- `WireVersion == 0` (the default) is silently stamped to `CurrentWireVersion`; an explicitly-supplied non-zero version round-trips verbatim so a forward-compat producer can author version-targeted payloads.

The encode path appends bytes to the supplied writer via the standard `IBufferWriter<byte>` contract (`GetSpan` / `Advance`); it never resets, rewinds, or otherwise mutates bytes the caller already wrote. Callers that expect a single-batch buffer supply a fresh writer per call.

And two on decode:

- An empty payload throws `ArgumentException`; a malformed payload throws `ArgumentException` wrapping the underlying serializer exception.
- A `WireVersion > CurrentWireVersion` payload throws `NotSupportedException` with the offending version embedded in the message.

## Allocation contract

The encode signature is deliberately `void Encode(envelope, IBufferWriter<byte> writer)` rather than the more obvious `byte[] Encode(envelope)` or `ReadOnlyMemory<byte> Encode(envelope)`. Returning a freshly-allocated buffer per batch would force a per-call heap allocation on the canonical hot path - exactly the path the streaming push transport drives at sub-second cadence - and there is no way for callers to "opt out" of the allocation once it is baked into the signature.

Forcing the writer-supplied shape pushes buffer ownership to the caller, who can choose:

- A pooled writer (typically `ArrayBufferWriter<byte>` reused across batches, or a custom `IBufferWriter<byte>` backed by `ArrayPool<byte>.Shared`).
- The transport's own writer - for the gRPC streaming push transport, the gRPC stream's `IBufferWriter<byte>` is handed in directly so the envelope's bytes never round-trip through a per-batch heap allocation.
- A fresh `ArrayBufferWriter<byte>` per call for tests and debug-tooling that genuinely want a materialised `byte[]` (read it back via `WrittenMemory` / `WrittenSpan`).

The writer's lifetime is the caller's responsibility - the encoder makes no claim on the bytes after `Encode` returns. This matches the ownership model `ReplicationBatch.Payload` already imposes on the bytes it carries.

The decode side accepts `ReadOnlyMemory<byte>` because the envelope graph itself (the `string`s and `IReadOnlyList<WalRecord>`) is unavoidably allocated by the deserialiser - there is no realistic pool-friendly alternative for the materialised payload graph.

## Registration

`AddLatticeReplication` registers the binary encoder as a silo-side singleton via `TryAddSingleton`:

```csharp verify
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

Hosts that need a different framing pre-register their own implementation before calling `AddLatticeReplication` (which respects the existing registration), or replace it after via standard DI:

```csharp verify
using System.Buffers;

sealed class MyEncoder : IReplicationBatchEncoder
{
    public string ContentType => "application/x-myencoder";
    public int CurrentWireVersion => 1;
    public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) { }
    public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => default;
}

static void Configure(IServiceCollection services)
{
    services.AddSingleton<IReplicationBatchEncoder, MyEncoder>();
}
```

The encoder is a singleton so the underlying `Serializer<ReplicationBatchEnvelope>` is resolved once and reused across every batch, which matches the Orleans serializer's thread-safe-by-contract usage pattern.

## Future encoders

A JSON encoder for HTTP-transport debuggability is a future option - JSON's per-field-name framing trades bandwidth for inspectability, which is the right trade for a debugging-only flag on a bootstrap / low-frequency HTTP path. Such an encoder plugs in via the same DI seam without changes to the transport layer or the envelope shape.

A content-hash-prefixed encoder layered on top of the binary format is the natural home for receiver-pull-only-missing-content-hashes deduplication; the seam is intentionally narrow enough that such an encoder can wrap the binary one without re-implementing the framing.

## Caveats

- **Encoders do not see routing context outside the envelope.** A custom encoder cannot dispatch on `ReplicationBatch.TargetClusterId` because the encoder operates on the envelope, not the surrounding call. Cross-cutting concerns belong on the envelope (or on the transport, depending on which layer needs to see the metadata) - never on the encoder.
- **`Entries` equality is by reference.** `ReplicationBatchEnvelope` is a record struct, so its synthesised equality compares the `Entries` reference, not the contents. Tests that need to assert content equality compare the deserialised lists element-by-element rather than relying on envelope-level `Equals`.
## Framing-tail compression

The framing layer (``EncodeFraming`` / ``TryDecodeFraming``) carries an optional **tail compression** byte in the fixed 32-byte header (``EncodedBatchHeader.Compression``). The header itself stays plaintext - readers can route, de-duplicate, and validate batches before deciding to inflate the body - and only the variable-length tail (``treeName``, ``originClusterId``, and the entry segments) is replaced with a length-prefixed compressed block when the algorithm is non-``None``.

This means **no wire-version bump**: a receiver that does not know how to decompress a given algorithm tag fails fast with ``NotSupportedException`` rather than silently dropping data. The encoder''s internal dispatch is keyed on the raw compression byte (not on named ``LatticeCompression`` enum members), so a host-defined algorithm whose tag is in the reserved ``[0x80, 0xFF]`` range round-trips through encode/decode without any core enum churn.

The replication options ``FramingCompression``, ``FramingCompressionLevel`` and ``FramingCompressionMinBatchBytes``, the public DI seam (``ILatticeCompressor`` and ``AddLatticeCompressor``), the tag-space partitioning, the worked example for plugging in a new algorithm, and the testing surface are all documented in **[`docs/lattice/compression.md`](../lattice/compression.md)** - that page is the source of truth for everything compression-related across Orleans.Lattice.

## Wire-version capability negotiation

The fail-fast posture described under [Why a versioned envelope](#why-a-versioned-envelope) is the right default for a deployment-ordering bug, but it is the wrong behaviour during a deliberate rolling upgrade: a newer sender shipping a newer frame to a not-yet-upgraded receiver throws `NotSupportedException` on every batch, the shipper retries forever, and replication to that peer stalls. Wire-version capability negotiation is the surface that makes that window **observable and floor-guarded**: the receiver advertises the version it can decode, the sender records the negotiated target for telemetry, and a peer below the configured minimum floor still fails fast. Actually re-encoding outbound batches at the negotiated older version - the capability that lets a newer sender ship a frame a not-yet-upgraded receiver can decode, and the prerequisite for closing the stall - is future wire-touching work that builds on this surface (see [Scope](#scope)).

### How it works

1. **The receiver advertises its capability.** Every `ReplicationAck` now carries an additive `[Id(5)] int? SupportedWireVersion` slot stamped with the receiver build's `EncodedBatchHeader.CurrentWireVersion`. The slot is strictly additive on the wire (same compatibility profile as `SuggestedBatchSize` / `PauseForMs`): a receiver built before negotiation omits the slot entirely (it decodes as `null`), and a sender built before negotiation ignores it.
2. **The sender computes the negotiated target version.** On each pump tick the shipper feeds the peer's most recently advertised `SupportedWireVersion` into the pure `WireVersionNegotiation.Negotiate(...)` helper, which returns a `WireVersionNegotiationResult` (recorded for telemetry and the floor-guard - it does not itself re-encode the batch):
   - `min(localCurrent, peerAdvertised)` once the peer's capability is known;
   - a conservative `UnknownPeerWireVersionFloor` until the peer has advertised one (the floor defaults to the sender's current version, matching pre-negotiation behaviour);
   - and the genuinely-unsupported hard error (`NotSupportedException`) when the peer advertises a version strictly below the configured `MinimumSupportedWireVersion` - the one case where fail-fast is preserved, because the sender cannot down-encode that far.
3. **Operators can see a mixed-version fleet.** The shipper records the negotiated target version and a downgrade signal to two observable gauges - `orleans.lattice.replication.wire_version.negotiated{tree,peer}` and `orleans.lattice.replication.wire_version.downgrade_active{tree,peer}` (the latter reports `1` while the negotiated target is below the sender's current version, else `0`) - backed by the `WireVersionNegotiationState` singleton.

```csharp verify
var result = WireVersionNegotiation.Negotiate(
    localCurrentVersion: EncodedBatchHeader.CurrentWireVersion,
    minimumSupportedVersion: 1,
    unknownPeerFloorVersion: EncodedBatchHeader.CurrentWireVersion,
    peerAdvertisedVersion: 3);

if (result.DowngradeActive)
{
    // The negotiated target (result.EffectiveWireVersion) is below the
    // local current version; a future re-encode seam would target it.
}
```

### Opting in

Negotiation ships **dark**: `LatticeReplicationOptions.WireVersionNegotiationEnabled` defaults to `false`, so a host that does not opt in frames every batch at the current wire version exactly as before, and the receiver-advertised `SupportedWireVersion` slot is simply never consumed. Hosts performing a heterogeneous rolling upgrade opt in and (optionally) lower the floor so the negotiated target for un-acked first batches is conservative:

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.WireVersionNegotiationEnabled = true;
    o.MinimumSupportedWireVersion = 1;
    o.UnknownPeerWireVersionFloor = 1;
});
```

`LatticeReplicationOptionsValidator` rejects a `MinimumSupportedWireVersion` outside `[1, EncodedBatchHeader.CurrentWireVersion]` and an `UnknownPeerWireVersionFloor` outside `[MinimumSupportedWireVersion, EncodedBatchHeader.CurrentWireVersion]` at first-resolve time.

### Scope

This is the negotiation **surface**: the receiver advertises, the sender computes and records the negotiated target, and the floor-guard hard error is preserved. The negotiated `EffectiveWireVersion` is the seam later wire-touching work consumes to truly re-encode entry bytes at the older version; today the steady-state ship path reuses the bytes the WAL already wrote at append time, so enabling negotiation changes the observability and floor-guard behaviour without altering the on-wire entry payloads. Because the feature is dark by default, there is no behavioural change for a host that does not opt in. The actual re-encode (and the safe on-by-default posture it unlocks) is tracked as a follow-up issue ([#703](https://github.com/NSTA1/Orleans.Lattice/issues/703)).

