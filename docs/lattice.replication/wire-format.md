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
    public const int CurrentMinorVersion = 1;
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

Forward-compatible additions (new `[Id(n)]` slots on `WalRecord` with stable defaults, like the `Mode` slot stamped by the commit-time observer when a tree's replication mode is declared) do not require a wire-version bump: the Orleans serializer's per-field id model handles them transparently and unknown ids on a legacy receiver decode as the default value. Such additive changes bump the diagnostic `CurrentMinorVersion` - separate from `CurrentVersion`, which is reserved for breaking changes that older receivers must reject - so logs and traces can correlate the producer's exact envelope shape; the minor version has no effect on encode/decode.

## `WalRecord.Value` strip on CRDT-mode entries

The canonical `OrleansBinaryWalRecordEncoder` strips the `[Id(4)] Value` slot on `MutationKind.Set` entries that satisfy both of the following at encode time:

- `Mode` is any typed CRDT mode, i.e. not `LwwRegister`.
- `Delta` is non-`null`.

The receiver-side apply path dispatches every typed CRDT mode through `WalRecord.Delta` and the primitive's `MergeDelta` operation, so the full-state `Value` byte payload is pure overhead on both the storage WAL and the cross-cluster wire (a single encode-at-append seam feeds both). For non-prepared CRDT-delta records the producer no longer materialises `Value` at all - `WalRecordBuilder.ForCrdtDelta` leaves the in-grain slot `null`, so the durable writer path pays no O(state) post-merge serialisation to feed a slot the encoder drops anyway. The leaf store still holds the canonical post-merge state (materialised lazily from the typed shadow), but the WAL record itself is delta-only. The activation-time cold-rebuild replay therefore reconstructs the post-fold state by folding `Delta` into the prior visible state rather than reading `Value` back. Prepared saga entries (`IsPrepared`) are the exception and retain `Value` at both layers (see below).

`LwwRegister` entries are not affected: `Value` remains the canonical payload, and a `null` `Value` on a `LwwRegister` `Set` continues to be rejected by the receiver-side apply path as an `ArgumentException`. CRDT-mode entries that for whatever reason ship without a `Delta` (a legacy producer, a hand-constructed entry in a test) also retain `Value` verbatim - the strip is gated on `Delta` presence.

**Consumer impact on `IChangeFeed`:** consumers reading `WalRecord.Value` directly on CRDT-mode entries now observe `null`. Such consumers must either (a) read `Delta` and apply it against their own prior observed state via the matching primitive's `MergeDelta`, or (b) read the producer's leaf store via the public lattice surface (`ILattice.GetAsync` or the typed accessor). The `LwwRegister` consumer contract is unchanged.

This strip does not require a `WireVersion` bump: an absent `[Id]` slot decodes to the type's default (`null` for `byte[]?`), which is exactly the behaviour the receiver-side null-tolerance was prepared to accept.

## Default encoder: Orleans serializer (binary)

The default DI registration is the Orleans-binary batch encoder. It serialises the envelope through `Serializer<ReplicationBatchEnvelope>` and tags the payload with the canonical content type:

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

Dict-less Zstandard (``LatticeCompression.Zstd``) is the **default** framing algorithm: a stock ``AddLatticeReplication`` cluster compresses its framing tail out of the box. The ``Zstd`` compressor is registered unconditionally, so every current-wire-version receiver already decodes the default frame with no extra wiring. The per-batch ``FramingCompressionMinBatchBytes`` threshold (512 bytes) still stamps ``None`` on a tail too small to recoup the fixed compression overhead, and **shared dictionaries** (``ZstdDictionary``) remain opt-in. A host that wants the historical uncompressed framing sets ``FramingCompression = LatticeCompression.None``.

This means **no wire-version bump**: a receiver that does not know how to decompress a given algorithm tag fails fast with ``NotSupportedException`` rather than silently dropping data. The encoder''s internal dispatch is keyed on the raw compression byte (not on named ``LatticeCompression`` enum members), so a host-defined algorithm whose tag is in the reserved ``[0x80, 0xFF]`` range round-trips through encode/decode without any core enum churn.

The replication options ``FramingCompression``, ``FramingCompressionLevel`` and ``FramingCompressionMinBatchBytes``, the public DI seam (``ILatticeCompressor`` and ``AddLatticeCompressor``), the tag-space partitioning, the worked example for plugging in a new algorithm, and the testing surface are all documented in **[`docs/lattice/compression.md`](../lattice/compression.md)** - that page is the source of truth for everything compression-related across Orleans.Lattice.

### Inbound decompression ceiling

The declared *uncompressed length* in the compressed tail is a wire field a hostile or corrupt sender can forge independently of how few compressed bytes it actually ships. Because ``TryDecodeFraming`` rents a buffer sized to that field *before* inflating, an unbounded value drives a multi-gigabyte allocation from a tiny request - the classic decompression-bomb amplification. The gRPC transport decodes framing **before** the shared-secret auth interceptor body runs, so this allocation is reachable pre-auth by any caller that can open a connection.

The decoder therefore bounds the declared length against ``LatticeReplicationOptions.MaxInboundDecompressedBytes`` and rejects an over-ceiling frame with ``ArgumentException`` (routed through the framing decoder's existing corrupt-payload path) before allocating. The ceiling defaults to 64 MB - 16x the 4 MB default ``WalMaxBatchBytes`` - giving generous headroom for a legitimately large or highly compressible batch while capping a hostile sender's decompressed allocation far below what a forged length field could otherwise demand. Operators that ship larger replication batches must raise it in step. The option is detailed in [`docs/lattice/compression.md`](../lattice/compression.md).

## Wire-version capability negotiation

The fail-fast posture described under [Why a versioned envelope](#why-a-versioned-envelope) is the right default for a deployment-ordering bug, but it is the wrong behaviour during a deliberate rolling upgrade: a newer sender shipping a newer frame to a not-yet-upgraded receiver throws `NotSupportedException` on every batch, the shipper retries forever, and replication to that peer stalls. Wire-version capability negotiation is the surface that makes that window **observable, floor-guarded, and - for a last-writer-wins tree - actually decodable by the older peer**: the receiver advertises the version it can decode, the sender computes the negotiated target, down-stamps the outbound framing header to that target so the older receiver can decode and apply the frame, and a peer below the configured minimum floor still fails fast.

### How it works

1. **The receiver advertises its capability.** Every `ReplicationAck` now carries an additive `[Id(5)] int? SupportedWireVersion` slot stamped with the receiver build's `EncodedBatchHeader.CurrentWireVersion`. The slot is strictly additive on the wire (same compatibility profile as `SuggestedBatchSize` / `PauseForMs`): a receiver built before negotiation omits the slot entirely (it decodes as `null`), and a sender built before negotiation ignores it.
2. **The sender computes the negotiated target version and down-stamps the header.** On each pump tick the shipper feeds the peer's most recently advertised `SupportedWireVersion` into the pure `WireVersionNegotiation.Negotiate(...)` helper, which returns a `WireVersionNegotiationResult`:
   - `min(localCurrent, peerAdvertised)` once the peer's capability is known;
   - a conservative `UnknownPeerWireVersionFloor` until the peer has advertised one (the floor defaults to the sender's current version, matching pre-negotiation behaviour);
   - and the genuinely-unsupported hard error (`NotSupportedException`) when the peer advertises a version strictly below the configured `MinimumSupportedWireVersion` - the one case where fail-fast is preserved, because the sender cannot down-encode that far.

   When the negotiated target is below the sender's current version the shipper threads it through `WireVersionDownEncoder` onto the framing header it stamps; when the target equals the current version the verbatim pre-encoded entry hot path is preserved with zero re-encode cost (a true same-version no-op).
3. **Operators can see a mixed-version fleet.** The shipper records the negotiated target version and a downgrade signal to two observable gauges - `orleans.lattice.replication.wire_version.negotiated{tree,peer}` and `orleans.lattice.replication.wire_version.downgrade_active{tree,peer}` (the latter reports `1` while the negotiated target is below the sender's current version, else `0`) - backed by the `WireVersionNegotiationState` singleton.

```csharp verify
var result = WireVersionNegotiation.Negotiate(
    localCurrentVersion: EncodedBatchHeader.CurrentWireVersion,
    minimumSupportedVersion: 1,
    unknownPeerFloorVersion: EncodedBatchHeader.CurrentWireVersion,
    peerAdvertisedVersion: EncodedBatchHeader.CurrentWireVersion - 1);

if (result.DowngradeActive)
{
    // The negotiated target (result.EffectiveWireVersion) is below the
    // local current version; the shipper down-stamps the framing header
    // to it via WireVersionDownEncoder so the older peer can decode it.
}
```

### Version-adaptive down-stamping (`WireVersionDownEncoder`)

`WireVersionDownEncoder` is the consumer of the negotiated target. It prepares the outbound batch's fixed `EncodedBatchHeader` for a target version older than the sender's current build so a current-build sender can ship a frame a not-yet-upgraded receiver decodes **and** applies. The mechanism is deliberately **header-only** - no entry segment is re-serialised - which is correct because every prior framing version *elided* a per-entry field rather than adding one, so the entry-segment bytes the current build produces are already a strict subset of what an older receiver expects:

- **Wire version 4** elided the per-entry `WalRecord.TreeId` slot. The current build also elides it, and a version-4 receiver re-stamps the tree id from the framing tail's `TreeName`. The entry segments are therefore byte-identical between version 4 and version 5.
- **Wire version 5** hoisted the per-entry merge mode into the header's packed slot. `WalRecord.Mode` carries no Orleans `[Id]` tag, so it is never serialised onto an entry segment in any version; a version-4 producer's per-entry mode was uniformly the `LwwRegister` enum default, so a version-4 receiver reads `LwwRegister` for every entry.

Down-stamping to version 4 is consequently exact when - and only when - the batch's merge mode is `LwwRegister` and the framing tail is uncompressed. A version-4 receiver reading the version-5 header's trailing packed 32-bit slot interprets bits 16-23 as part of its 24-bit `AtomicBatchSpanCount`; those bits are zero precisely when `Mode` is the `LwwRegister` default, so the header bytes are then fully version-4-compatible.

The helper refuses (with `NotSupportedException`, the same fail-fast posture as a below-floor peer) to down-stamp the two genuinely un-down-encodable shapes:

- **A CRDT-mode tree** - its per-entry merge dispatch depends on the hoisted header mode a pre-version-5 receiver cannot read, so down-stamping would silently mis-apply the entries.
- **A compressed framing tail** - compression rides the header without a wire-version bump, so a pre-version-5 receiver is not guaranteed to carry the matching `ILatticeCompressor`. The encoder therefore refuses a compressed down-stamp; the shipper resolves this case by dropping compression for the down-stamped peer's batch (see the down-encodable matrix below) so a compressed last-writer-wins tree keeps replicating uncompressed rather than pausing.

The receiver-side apply path restores the elided per-entry fields from the framing context via `IWalRecordEncoder.Decode(span, treeId, mode)`: the tree id comes from the framing tail's `TreeName`, the merge mode from the header's `Mode` field.

### Down-encodable matrix

The shipper owns the down-stamp decision per peer. Compression is the one blocker it resolves by degrading rather than pausing: because framing-tail compression is pure transport framing, shipping a batch *uncompressed* to an older peer is lossless, so a last-writer-wins tree blocked only by compression drops compression for that peer's batch and keeps replicating. CRDT-mode and sub-floor targets cannot be down-encoded at all and pause until the peer is upgraded. `WireVersionDownEncoder.EnsureDownEncodable` itself is unchanged - it still refuses a compressed batch; the shipper validates with `LatticeCompression.None` and stamps the per-peer header uncompressed.

| Merge mode | Framing compression | Target wire version | Down-encodable? | Behaviour |
|------------|---------------------|---------------------|-----------------|-----------|
| LwwRegister | None | >= 4 | Yes | Header-only down-stamp; entry segments shipped verbatim. |
| LwwRegister | Zstd / ZstdDictionary | >= 4 | Yes | Compression auto-dropped for that peer; the batch ships uncompressed (lossless). |
| Any | Any | < 4 | No | Fail-fast; replication to the peer is paused until it is upgraded. |
| CRDT mode | Any | < current | No | Fail-fast; cannot be faithfully represented for a pre-version-5 receiver, paused until upgrade. |

The compression auto-degrade is lossless: compression rides the framing tail only, so an uncompressed frame carries the identical entry bytes a pre-current-version receiver expects. Every down-stamp outcome is observable on the `orleans.lattice.replication.ship.wire_version_down_stamp` counter (tagged by `tree`, `peer`, and `reason`): `compression_dropped` (degraded but still shipping), `blocked_crdt_mode`, and `blocked_unsupported_version`. The two blocked reasons make a paused stream an operator-actionable signal rather than a silent stall, so a blocked CRDT tree is never an invisible stop - the operator sees the counter climb and knows exactly which peer to upgrade.

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

Startup options validation rejects a `MinimumSupportedWireVersion` outside `[1, EncodedBatchHeader.CurrentWireVersion]` and an `UnknownPeerWireVersionFloor` outside `[MinimumSupportedWireVersion, EncodedBatchHeader.CurrentWireVersion]` at first-resolve time.

### Scope and current posture

The down-stamp mechanism is **implemented and tested** for last-writer-wins trees: a current-build sender negotiates the previous wire version for an older peer, down-stamps the framing header, and the previous-version receiver path decodes and applies the entries to field-complete `WalRecord` values. A same-version peer is a true verbatim no-op (the bytes on the wire are byte-identical to a build that never negotiated). A last-writer-wins tree configured with framing compression keeps replicating to an older peer by auto-dropping compression for that peer's batch (shipping it uncompressed - lossless, because compression is framing-only). CRDT-mode trees and sub-floor targets cannot be down-encoded and pause rather than emit a frame the older peer would mis-apply; the pause is observable on the `orleans.lattice.replication.ship.wire_version_down_stamp` counter (`blocked_crdt_mode` / `blocked_unsupported_version`) so it is never a silent stall.

Negotiation ships **dark** (`WireVersionNegotiationEnabled` defaults to `false`), and the default-on flip is **deferred**: the down-stamp is exact for last-writer-wins trees (compressed ones degrade losslessly to uncompressed for the older peer), and flipping the default on safely for a heterogeneous fleet (including CRDT trees) requires the upgrade-direction stall analysis that a later issue will carry. Hosts performing a controlled rolling upgrade of last-writer-wins trees can opt in today. Because the feature is dark by default, there is no behavioural change for a host that does not opt in. The default-on posture is tracked as follow-up work on [#703](https://github.com/NSTA1/Orleans.Lattice/issues/703).

## Per-peer shared-dictionary capability negotiation

Shared-dictionary compression (`LatticeCompression.ZstdDictionary`) can be negotiated per peer over the same ack-based capability channel that carries the wire version. When enabled, a sender only compresses a batch with the configured shared-dictionary id for a peer that has **advertised** that id; otherwise it falls back to plain dictionary-less `Zstd` for that peer. This guarantees no peer ever receives a frame compressed with a dictionary it cannot resolve, so a mixed fleet (some peers carrying the dictionary, some not) keeps shipping during a rolling dictionary rollout.

### How it works

1. **The receiver advertises its dictionary capability.** Every `ReplicationAck` carries an additive `[Id(6)] uint[]? AdvertisedDictionaryIds` slot. When the receiver's registered `ILatticeCompressionDictionaryProvider` also implements `ILatticeCompressionDictionaryCatalog`, the slot is stamped with the sorted set of dictionary ids that provider can resolve; otherwise it is `null`. The slot is strictly additive on the wire (same compatibility profile as `SupportedWireVersion`): a receiver built before dictionary negotiation omits it (decodes as `null`), and a sender built before negotiation ignores it.
2. **The sender negotiates the effective dictionary id.** On each pump tick the shipper feeds the peer's most recently advertised ids into the pure `SharedDictionaryNegotiation.Negotiate(...)` helper, which returns a `SharedDictionaryNegotiationResult`: the configured id is used (`Matched`) when the peer advertised it; otherwise the sender falls back to id `0` (dictionary-less) and stamps `LatticeCompression.Zstd` instead of `ZstdDictionary`, so the receiver decodes a plain Zstd frame. A `null` advertisement is treated as an as-yet-unknown capability and also falls back. The per-peer negotiated state is activation-scoped and refreshed on every ack, so it adapts when a peer reconnects or changes its advertised capability. Unlike wire-version negotiation, this **never** fails fast - the dictionary-less fallback is always decodable.
3. **Operators can see the outcome.** The shipper records the per-`(tree, peer)` negotiation outcome to the `orleans.lattice.replication.ship.dictionary_negotiation{tree,peer,outcome}` counter (`outcome` is `matched`, `fell_back`, `unknown`, or `fingerprint_mismatch` - see [Content-fingerprint safety guard](#content-fingerprint-safety-guard)) and the share of batches shipped with versus without a shared dictionary to `orleans.lattice.replication.ship.dictionary_batches{tree,peer,dictionary}` (`dictionary` is `with_dictionary` or `without_dictionary`).

```csharp verify
var negotiation = SharedDictionaryNegotiation.Negotiate(
    configuredDictionaryId: 7u,
    peerAdvertisedIds: new uint[] { 3u, 7u });

if (negotiation.Matched)
{
    // The peer advertised dictionary id 7, so the sender compresses with
    // it (negotiation.EffectiveDictionaryId). Otherwise FellBack is true
    // and EffectiveDictionaryId is 0 (plain dictionary-less Zstd).
}
```

### Opting in

Negotiation ships **dark**: `LatticeReplicationOptions.DictionaryNegotiationEnabled` defaults to `false`, so a host that does not opt in stamps the configured dictionary id exactly as before and the receiver-advertised `AdvertisedDictionaryIds` slot is simply never consumed. A host rolling out a shared dictionary across a mixed fleet opts in on the `ZstdDictionary` tree:

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.FramingCompression = LatticeCompression.ZstdDictionary;
    o.FramingCompressionDictionaryId = 7u;
    o.DictionaryNegotiationEnabled = true;
});
```

Because the feature is dark by default and the slot is additive, there is no behavioural change and the bytes on the wire are byte-identical for a host that does not opt in.

### Content-fingerprint safety guard

Negotiating on the bare numeric dictionary id alone has a sharp edge: two deployments can map the **same id to different bytes** - an operator slip, or the guaranteed collision when two clusters both auto-train and each labels its first dictionary id 1. With id-only negotiation the sender sees the id advertised, compresses with its own (different) bytes, and the receiver hard-fails decode with `ArgumentException`. The provider contract warns that "changing the bytes behind an id would silently corrupt in-flight frames," but nothing enforced it on the wire.

The fingerprint guard closes that gap by carrying a **content fingerprint** alongside the id:

1. **The receiver advertises (id, fingerprint) pairs.** Every `ReplicationAck` carries an additive `[Id(7)] AdvertisedCompressionDictionary[]? AdvertisedDictionaries` slot in addition to the id-only `[Id(6)] AdvertisedDictionaryIds`. Each `AdvertisedCompressionDictionary` is an `(uint Id, ulong Fingerprint)` pair where the fingerprint is `CompressionDictionaryFingerprint.Compute(bytes)` - the 64-bit FNV-1a of the receiver's dictionary bytes for that id (the same hash family the framing header uses for the origin cluster id, deterministic across processes and architectures). A receiver on the current build populates **both** slots, so an older sender keeps negotiating on the id-only slot while a current sender prefers the fingerprint-gated slot.
2. **The sender gates on (id, fingerprint).** When the peer advertised the fingerprint-bearing slot, the shipper calls the `SharedDictionaryNegotiation.Negotiate(configuredDictionaryId, configuredFingerprint, peerAdvertised)` overload, resolving its own configured dictionary's fingerprint via the registered `ILatticeCompressionDictionaryProvider`. The dictionary is honoured only when the peer advertised the configured id **and** a matching fingerprint; a same-id/different-fingerprint peer falls back to dictionary-less `Zstd` exactly like an absent id, so the receiver never sees an undecodable frame. When the peer advertised only the id-only slot (a build predating the fingerprint slot), the sender negotiates on the id alone exactly as before.
3. **The misconfiguration is legible.** A same-id/different-fingerprint fallback surfaces a distinct `fingerprint_mismatch` value on the `orleans.lattice.replication.ship.dictionary_negotiation{tree,peer,outcome}` counter (joining `matched`, `fell_back`, and `unknown`), and the shipper logs a one-shot warning per activation naming the id to reconcile. The misconfiguration is therefore visible as a recognisable telemetry signal instead of manifesting as receiver-side decode failures.

```csharp verify
var bytes = new byte[] { 1, 2, 3, 4 };
var fingerprint = CompressionDictionaryFingerprint.Compute(bytes);

var negotiation = SharedDictionaryNegotiation.Negotiate(
    configuredDictionaryId: 7u,
    configuredFingerprint: fingerprint,
    peerAdvertised: new[] { new AdvertisedCompressionDictionary(7u, fingerprint) });

if (negotiation.Matched)
{
    // The peer advertised id 7 with a byte-matching fingerprint, so the
    // sender compresses with it. A same-id/different-fingerprint peer
    // instead returns FellBack && FingerprintMismatch and the sender ships
    // plain dictionary-less Zstd.
}
```

The guard needs no opt-in beyond `DictionaryNegotiationEnabled`: when negotiation is on and the peer advertises the fingerprint-bearing slot, the gate is active. The `AdvertisedDictionaries` slot is strictly additive (same compatibility profile as `AdvertisedDictionaryIds`), so a peer predating the fingerprint slot negotiates exactly as before and the bytes on the wire are unchanged for any host that does not opt into dictionary negotiation.

#### Scope: negotiation-layer guard, not a frame-tail fingerprint

The fingerprint is carried on the **advertisement + negotiation** layer (the additive ack slot), not stamped into the framing frame tail. The dictionary slot in `EncodedBatchHeader` carries only the numeric id, and bumping the frame layout to carry a fingerprint would break every current receiver when wire-version negotiation is off (a negotiation-off sender always frames at `CurrentWireVersion`). The negotiation-layer guard fully satisfies the safety goal - a same-id/different-bytes configuration never produces a decode failure - because the sender simply never selects a mismatching dictionary in the first place; a frame-tail fingerprint would be unreachable defense-in-depth given that guard.

