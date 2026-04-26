# Replication wire format (`IReplicationBatchEncoder`)

`IReplicationBatchEncoder` is the public, pluggable seam over the on-the-wire bytes that an outbound shipper stuffs into [`ReplicationBatch.Payload`](transport.md). It is the encode/decode counterpart to [`IReplicationTransport`](transport.md): the transport delivers opaque bytes between clusters, and the encoder is the only component that knows how to translate a batch of [`ReplogEntry`](change-feed.md) records to and from those bytes.

The default registration is a binary encoder that uses the Orleans serializer applied to a versioned envelope. Hosts that need a different framing — JSON for HTTP-transport debuggability, a custom envelope for compatibility with an external pipeline, content-hash-prefixed framing for deduplication — replace the registration via standard DI.

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
    public IReadOnlyList<ReplogEntry> Entries { get; init; }

    public const int CurrentVersion = 1;
}
```

| `ReplicationBatchEnvelope` member | Semantics |
|---|---|
| `WireVersion` | The wire-format version this envelope was authored against. Receivers compare against `IReplicationBatchEncoder.CurrentWireVersion` and reject payloads carrying a strictly greater value rather than guess at the layout. Hand-constructed envelopes default to `0`; the canonical encoder stamps `CurrentVersion` at encode time when the caller supplies `0`. |
| `TreeName` | Logical tree id the entries were captured from. Mirrors `ReplicationBatch.TreeName` on the surrounding call envelope; receivers route the per-tree apply pipeline on this value. |
| `OriginClusterId` | Stable identifier of the originating cluster. Mirrors `ReplicationBatch.OriginClusterId` on the surrounding call envelope; receivers use it to attribute origin and break replication cycles. |
| `Entries` | The captured `ReplogEntry` records, in commit order. May be empty (heartbeat / keep-alive batch). Never `null` on a value produced by the canonical encoder; hand-constructed envelopes that leave this default decode as an empty list because the canonical decoder normalises `null` to `Array.Empty<ReplogEntry>()`. |

The envelope is Orleans-serialisable (alias `olr.be`); the call-shape `ReplicationBatch` is intentionally not. Wire-format hardening — versioned envelopes, content framing, compression — happens *inside* `ReplicationBatch.Payload`, and the envelope is the canonical shape that lives there.

## Why a versioned envelope

Future breaking changes to the on-the-wire shape — new top-level fields that older receivers must reject rather than silently discard, restructured `Entries` collections, alternate carrier formats for typed CRDT deltas — are signalled by bumping `WireVersion`. A receiver compares against its `CurrentWireVersion` strictly:

- `WireVersion <= CurrentWireVersion` → accepted.
- `WireVersion > CurrentWireVersion` → rejected with `NotSupportedException`.

This is the canonical fail-fast posture: an older receiver paired with a newer producer immediately surfaces a deployment-ordering bug rather than mis-applying a payload it cannot interpret. A newer receiver paired with an older producer is the "normal" mixed-version case during a rolling upgrade and decodes the payload directly.

Forward-compatible additions (new `[Id(n)]` slots on `ReplogEntry` with stable defaults, like the `Mode` slot stamped by the commit-time observer when a tree's replication mode is declared) do not require a wire-version bump: the Orleans serializer's per-field id model handles them transparently and unknown ids on a legacy receiver decode as the default value.

## Default encoder: Orleans serializer (binary)

The default DI registration is `OrleansBinaryReplicationBatchEncoder`. It serialises the envelope through `Serializer<ReplicationBatchEnvelope>` and tags the payload with the canonical content type:

```text
application/x-orleans-lattice-replog+binary
```

The Orleans serializer is roughly 33% more compact than naive JSON for `byte[]` payloads (which is the common case for replication — every value committed through `ILattice.SetAsync` is a `byte[]`), and avoids JSON's base64 round-trip overhead. The encoder unit tests pin a regression floor on this with a "binary is smaller than `System.Text.Json.JsonSerializer.SerializeToUtf8Bytes`" assertion against a representative payload-heavy batch.

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

The encode signature is deliberately `void Encode(envelope, IBufferWriter<byte> writer)` rather than the more obvious `byte[] Encode(envelope)` or `ReadOnlyMemory<byte> Encode(envelope)`. Returning a freshly-allocated buffer per batch would force a per-call heap allocation on the canonical hot path — exactly the path the streaming push transport drives at sub-second cadence — and there is no way for callers to "opt out" of the allocation once it is baked into the signature.

Forcing the writer-supplied shape pushes buffer ownership to the caller, who can choose:

- A pooled writer (typically `ArrayBufferWriter<byte>` reused across batches, or a custom `IBufferWriter<byte>` backed by `ArrayPool<byte>.Shared`).
- The transport's own writer — for the gRPC streaming push transport, the gRPC stream's `IBufferWriter<byte>` is handed in directly so the envelope's bytes never round-trip through a per-batch heap allocation.
- A fresh `ArrayBufferWriter<byte>` per call for tests and debug-tooling that genuinely want a materialised `byte[]` (read it back via `WrittenMemory` / `WrittenSpan`).

The writer's lifetime is the caller's responsibility — the encoder makes no claim on the bytes after `Encode` returns. This matches the ownership model `ReplicationBatch.Payload` already imposes on the bytes it carries.

The decode side accepts `ReadOnlyMemory<byte>` because the envelope graph itself (the `string`s and `IReadOnlyList<ReplogEntry>`) is unavoidably allocated by the deserialiser — there is no realistic pool-friendly alternative for the materialised payload graph.

## Registration

`AddLatticeReplication` registers the binary encoder as a silo-side singleton via `TryAddSingleton`:

```text
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

Hosts that need a different framing pre-register their own implementation before calling `AddLatticeReplication` (which respects the existing registration), or replace it after via standard DI:

```text
services.AddSingleton<IReplicationBatchEncoder, MyEncoder>();
```

The encoder is a singleton so the underlying `Serializer<ReplicationBatchEnvelope>` is resolved once and reused across every batch, which matches the Orleans serializer's thread-safe-by-contract usage pattern.

## Future encoders

A JSON encoder for HTTP-transport debuggability is a future option — JSON's per-field-name framing trades bandwidth for inspectability, which is the right trade for a debugging-only flag on a bootstrap / low-frequency HTTP path. Such an encoder plugs in via the same DI seam without changes to the transport layer or the envelope shape.

A content-hash-prefixed encoder layered on top of the binary format is the natural home for receiver-pull-only-missing-content-hashes deduplication; the seam is intentionally narrow enough that such an encoder can wrap the binary one without re-implementing the framing.

## Caveats

- **Encoders do not see routing context outside the envelope.** A custom encoder cannot dispatch on `ReplicationBatch.TargetClusterId` because the encoder operates on the envelope, not the surrounding call. Cross-cutting concerns belong on the envelope (or on the transport, depending on which layer needs to see the metadata) — never on the encoder.
- **`Entries` equality is by reference.** `ReplicationBatchEnvelope` is a record struct, so its synthesised equality compares the `Entries` reference, not the contents. Tests that need to assert content equality compare the deserialised lists element-by-element rather than relying on envelope-level `Equals`.
