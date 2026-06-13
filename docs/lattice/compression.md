# Compression

This document is the source of truth for everything related to compression in Orleans.Lattice: the public seam, the registration pattern, the on-wire tag space, the configuration knobs, and the worked examples for plugging in a custom algorithm. Sibling docs (`wire-format.md`, `configuration.md`, `api.md`) link here instead of repeating the material.

## TL;DR

- The compression seam is the public interface `ILatticeCompressor` in the `Orleans.Lattice` namespace.
- The on-wire tag is a single `byte` (`LatticeCompression`), carried in plaintext alongside the relevant layer's fixed header so receivers can dispatch before allocating an inflate buffer.
- Register compressors on the silo's DI container via `AddLatticeCompressor` - the encoder layer keys its dispatch on the **raw byte**, not the named enum, so hosts can ship custom algorithms without core enum churn.
- Out of the box, replication ships Zstandard (`LatticeCompression.Zstd`) registered automatically by `AddLatticeReplication`, and dict-less Zstd is the **default** framing algorithm - a stock cluster compresses its framing tail with no extra configuration.

## The seam

```text
public interface ILatticeCompressor
{
    LatticeCompression Algorithm { get; }
    int GetMaxCompressedLength(int uncompressedLength);
    int Compress(ReadOnlySpan<byte> source, Span<byte> destination);
    void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength);
}
```

Implementations must be **thread-safe** and **allocation-free on the steady-state hot path**. Callers size their pooled buffers via `GetMaxCompressedLength` before calling `Compress`, and pass the expected uncompressed length verbatim from the wire format's length prefix to `Decompress` (which must validate the recovered length and throw on mismatch).

## The tag space

`LatticeCompression` is a single-byte enum. The tag space is partitioned by range:

| Range | Reservation | Meaning |
|---|---|---|
| `0x00` | Core | `LatticeCompression.None` - no compression, payload is verbatim. |
| `0x01` | Core | `LatticeCompression.Zstd` - RFC 8478 Zstandard, dictionary-less. Only the core `ZstdLatticeCompressor` type may claim this tag. |
| `0x02` | Core | `LatticeCompression.ZstdDictionary` - RFC 8478 Zstandard with a shared dictionary selected by a stable id carried in the framed tail. Only the core `ZstdDictionaryLatticeCompressor` type may claim this tag. |
| `0x03` - `0x7F` | Core (reserved) | Reserved for future core-shipped algorithms. Hosts must not invent values in this range; the encoder rejects such registrations with `ArgumentException` at silo startup. |
| `0x80` - `0xFF` | Host | Available to hosts that ship their own compressor without coordinating with the core library - **including** alternative implementations of a core algorithm (e.g. a host's own Zstd-compatible codec must claim a tag here, not `0x01`). |

The encoder's internal dispatch is **keyed on the raw byte value of `Algorithm`**, not on the named enum members. A host that wants to ship a custom algorithm casts a byte from the host-reserved range into `LatticeCompression`, assigns it as `ILatticeCompressor.Algorithm`, and registers the compressor via DI. Encode and decode round-trip the byte verbatim through the fixed header; no core enum churn is required.

A producer batch whose tag has no registered compressor at the receiver surfaces as `NotSupportedException` from the consuming decoder. This is the wire-version-free way new algorithms ship without coordinated upgrades.

## Registering a compressor

The public registration surface lives in `LatticeCompressionServiceCollectionExtensions` (namespace `Orleans.Lattice`):

```text
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

// Type registration - idempotent (TryAddEnumerable under the hood).
services.AddLatticeCompressor<MyCompressor>();

// Instance registration - use when constructor parameters (e.g. a
// non-default compression level) cannot be resolved from the
// container.
services.AddLatticeCompressor(new MyCompressor(level: 9));
```

The encoder consumes `IEnumerable<ILatticeCompressor>` and builds a `FrozenDictionary<byte, ILatticeCompressor>` at construction time. Two compressors that share the same `Algorithm` tag are rejected with `ArgumentException` so accidental double-registration fails fast at silo startup rather than silently shadowing.

`LatticeCompression.None` cannot be registered - it is the reserved verbatim-payload sentinel and the encoder rejects compressors that claim it.

The core-reserved tag range `[0x00, 0x7F]` is enforced at registration time: only types declared in the core `Orleans.Lattice` assembly may claim a tag in that range. A compressor type from any other assembly whose `Algorithm` is in `[0x00, 0x7F]` is rejected with `ArgumentException` at silo startup. This closes two distinct hazards in one rule:

1. **Undefined core tags** (e.g. `0x42`) - a host claim would silently collide with whatever algorithm a future core release ships at that tag.
2. **Defined core tags** (e.g. `Zstd`, `0x01`) - a host's own non-canonical implementation would silently squat on the wire identity of the canonical core algorithm, producing receiver-side decode failures or corrupted streams against any peer running the core implementation.

Host-defined algorithms - **including** Zstd-compatible variants from a non-core library - must use a tag in `[0x80, 0xFF]`. There is no way for a host to claim a core-range tag, by design.

## Worked example: plugging in a custom algorithm

```csharp verify
using Microsoft.Extensions.DependencyInjection;

// 1. Implement the seam. Pick a byte in the [0x80, 0xFF] host
//    range and cast it to LatticeCompression as the Algorithm tag.
public sealed class MyCompressor : ILatticeCompressor
{
    public LatticeCompression Algorithm => (LatticeCompression)0x90;

    public int GetMaxCompressedLength(int uncompressedLength)
        => uncompressedLength + 16; // worst-case for your codec

    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        // ...compress source into destination, return bytes written...
        source.CopyTo(destination);
        return source.Length;
    }

    public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
    {
        // ...decompress source into destination, validate length...
        source.CopyTo(destination);
    }
}

// 2. Register on the silo.
static void Configure(IServiceCollection services)
{
    services.AddLatticeCompressor<MyCompressor>();
}
```

To use the new tag in the replication framing path, point the option at the same byte:

```text
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.FramingCompression = (LatticeCompression)0x90;
});
```

The option validator accepts any tag in the host-reserved `[0x80, 0xFF]` range without an `Enum.IsDefined` check; mis-typed core-range values (`0x02`-`0x7F` that are not defined enum members) still fail validation at silo startup.

## Replication framing-tail compression

The replication layer is the first in-tree consumer of the compression seam. Dict-less Zstandard (`LatticeCompression.Zstd`) is the **default** framing algorithm: a stock `AddLatticeReplication` cluster compresses its framing tail out of the box, and the `Zstd` compressor is registered unconditionally so every current-wire-version receiver already decodes it. The per-batch threshold (`FramingCompressionMinBatchBytes`, 512 bytes) still skips compression on tiny batches, and shared dictionaries remain opt-in. A host that wants the historical uncompressed framing opts out with `FramingCompression = LatticeCompression.None`:

```text
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    // Dict-less Zstd is the default; set this to LatticeCompression.None to opt out.
    o.FramingCompression = LatticeCompression.Zstd;
    o.FramingCompressionLevel = 3;          // Zstd: 1 (fastest) - 22 (highest ratio), default 3
    o.FramingCompressionMinBatchBytes = 512; // skip compression below this uncompressed tail size
});
```

| Option | Type | Default | Meaning |
|---|---|---|---|
| `FramingCompression` | `LatticeCompression` | `Zstd` | Algorithm tag stamped into the framing header. Defaults to dict-less Zstd; set to `None` to opt out. |
| `FramingCompressionLevel` | `int` | `3` | Zstd compression level. Validated to `[1, 22]` when the algorithm is `Zstd` or `ZstdDictionary`; ignored otherwise. |
| `FramingCompressionMinBatchBytes` | `int` | `512` | Uncompressed-tail threshold below which the shipper stamps `Compression = None` for the batch (heartbeats / small-bursty traffic skip the per-batch fixed overhead). `0` disables the threshold. |
| `FramingCompressionDictionaryId` | `uint` | `0` | Stable id of the shared dictionary the shipper requests when `FramingCompression` is `ZstdDictionary`. `0` means "no dictionary"; required to be non-zero when the algorithm is `ZstdDictionary`. |
| `MaxInboundDecompressedBytes` | `long` | `64 MiB` | Hard ceiling on the **decompressed** size of an inbound compressed framing batch. The framing decoder rejects (with `ArgumentException`) any frame whose declared uncompressed length exceeds this *before* it allocates the inflate buffer, bounding the decompression-bomb amplification a hostile or corrupt sender can drive from a tiny request. This is reachable pre-auth on the gRPC transport - framing is decoded before the shared-secret interceptor body runs. Defaults to 16x the 4 MB `WalMaxBatchBytes` ceiling; raise it in step if you legitimately ship larger batches. Must be `>= 1`. |

Compression is invisible to the apply pipeline - `ReceiverFlowControlContext`, mutation observers, and the WAL replay path see the same plaintext entries regardless of the on-wire tag.

The wire-format layout of the compressed tail (length prefixes, alignment with the fixed plaintext header, no-wire-version-bump guarantee) is documented in [`docs/lattice.replication/wire-format.md`](../lattice.replication/wire-format.md).

## Shared-dictionary Zstandard compression

Replication payloads are highly self-similar: repeated key prefixes, identical value schemas, and recurring CRDT delta shapes recur across batches. Dictionary-less Zstandard (`LatticeCompression.Zstd`) compresses each batch independently with a fresh compressor, so the cross-batch redundancy is invisible to it and small batches compress poorly. The `LatticeCompression.ZstdDictionary` tag (`0x02`) compresses the batch tail against a **shared dictionary** trained on (or representative of) that redundancy, recovering the saving a per-batch compressor cannot see.

The shared dictionary is identified by a stable `uint` **dictionary id**. The id travels in the framed tail (a 4-byte little-endian prefix ahead of the existing length prefixes) so the receiver selects the matching dictionary before inflating; the reserved id `0` means "no dictionary". The framing wire version is unchanged - `None` and `Zstd` frames are byte-identical to before, and the dictionary id rides the variable tail rather than the fixed 32-byte header.

**Obtaining a dictionary.** Operator-supplied (pre-trained) dictionaries are the primary path: a host ships the dictionary asset with its configuration and registers it by id. Register the dictionary bytes and the dictionary-aware compressor on every silo that produces or consumes dictionary frames:

```text
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

// 1. Register the pre-trained dictionary bytes by stable id.
services.AddLatticeCompressionDictionaries(new Dictionary<uint, ReadOnlyMemory<byte>>
{
    [1u] = preTrainedDictionaryBytes,
});

// 2. Register the dictionary-aware Zstandard compressor (wire tag 0x02).
services.AddLatticeZstdDictionaryCompressor(compressionLevel: 3);
```

```text
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.FramingCompression = LatticeCompression.ZstdDictionary;
    o.FramingCompressionDictionaryId = 1u;   // must match a registered dictionary id
    o.FramingCompressionLevel = 3;
});
```

**Graceful fallback.** Shared-dictionary compression is opt-in and degrades safely:

- An encoder that cannot resolve the requested dictionary locally (no dictionary-aware compressor registered, or an unknown id) re-stamps the frame as plain `Zstd` (still decodable by any peer carrying the core Zstd compressor) rather than emitting an unreadable dictionary frame.
- A receiver that cannot resolve the dictionary id (or does not recognise the tag) surfaces `NotSupportedException` from the framing decoder and routes through the existing transient-backoff / dead-letter classification path, rather than silently mis-decoding.

As with every other compression knob, flipping to a non-zero dictionary id requires a coordinated rollout: the dictionary bytes behind the id must be registered on every receiver before any sender selects it.

The before/after ratio is observable - see [`docs/lattice.replication/observability.md`](../lattice.replication/observability.md).

### Auto-trained dictionaries

Operator-supplied dictionaries require an offline training step and a manual roll-out. As an alternative, Orleans.Lattice can **train a shared dictionary at runtime** from a sampled reservoir of representative payloads. Auto-training is **opt-in and off by default**; when disabled it is an allocation-free no-op that emits no telemetry, and the wire format is byte-identical to a build that never references it.

Register the auto-trainer as the dictionary provider alongside the dictionary-aware compressor:

```text
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

// 1. Register the auto-trained dictionary provider (opt-in; disabled by default).
services.AddLatticeAutoTrainingCompressionDictionary(o =>
{
    o.Enabled = true;
    o.MaxSampleCount = 1024;            // reservoir cap (samples)
    o.MaxReservoirBytes = 8 * 1024 * 1024; // reservoir cap (bytes)
    o.MaxSampleBytes = 64 * 1024;       // per-sample cap
    o.SamplingRate = 1.0;               // probability a payload is sampled
    o.DictionaryCapacityBytes = 112 * 1024; // trained dictionary size cap
    o.MinSamplesToTrain = 100;          // floor before a pass runs
    o.MinTrainingInterval = TimeSpan.FromMinutes(5); // cadence cap
    o.RetainedVersionCount = 4;         // recent versions kept resolvable
    o.FirstDictionaryId = 1u;           // monotonic id base (non-zero)
});

// 2. Register the dictionary-aware Zstandard compressor (wire tag 0x02).
services.AddLatticeZstdDictionaryCompressor(compressionLevel: 3);
```

How it works:

- **Bounded reservoir.** Observed payloads are sampled into a reservoir capped by both sample count and total bytes, with a per-sample byte cap and a configurable sampling rate. When the reservoir is full the oldest sample is evicted to admit a new one, so memory use is strictly bounded regardless of traffic volume. Feeding the reservoir is an explicit ingestion hook (`Observe`) and a no-op while disabled.
- **Off-hot-path training.** Training is driven by an explicit pass (`TryTrain`) the host invokes from a turn-safe schedule rather than a hidden background timer, so cadence is deterministic and bounded by `MinTrainingInterval`; a pass below `MinSamplesToTrain`, inside the cadence window, or whose corpus the builder rejects is skipped and never throws to the caller. At most one training pass runs at a time.
- **Versioned roll-over.** Each successful pass builds the dictionary fully, then atomically publishes it under a new monotonically increasing dictionary id paired with a content hash (an in-process FNV-1a digest used only to detect that a freshly trained dictionary is byte-identical to the current one and skip a redundant version bump; it never travels on the wire). A bounded ring of the most recent versions stays resolvable, so a frame compressed against a version that a roll-over has just superseded still decompresses.
- **Safe fallback.** A consumer that lacks the dictionary for a requested id resolves it as absent and degrades through the same decoder path as any unknown id, rather than mis-decoding - so a roll-over never causes data loss.

Auto-training produces a dictionary **locally**. The trained ids are **advertisable**: `AutoTrainingCompressionDictionaryProvider` implements `ILatticeCompressionDictionaryCatalog`, so its live retained-version ids flow through `CompressionDictionaryAdvertisement.Build` onto a receiver's `ReplicationAck.AdvertisedDictionaries`, each paired with the dictionary's content fingerprint. An opted-in sender can therefore fingerprint-gate dictionary compression against an auto-trained dictionary exactly as it does for an operator-supplied one: a peer that has not advertised the id (or advertised it with a different fingerprint) gets dictionary-less `Zstd`, never a frame it cannot decode. Distribution of the trained **bytes** to a peer that does not yet hold them - and the host wiring that pumps sampling and training - is now automated by the single-switch `AddLatticeAutoSharedDictionary` helper described next.

The training cadence, active version, reservoir fill, and the trained-versus-baseline compression ratio are all observable - see the auto-trained-dictionary panels on the Overview dashboard and [`docs/lattice.dashboards/metrics-to-panel-map.md`](../lattice.dashboards/metrics-to-panel-map.md).

### Self-distributing auto-dictionary

`AddLatticeAutoTrainingCompressionDictionary` trains a dictionary locally but still leaves the host to wire sampling into `Observe`, pump `TryTrain` on a schedule, pick the framing id, and provision the trained bytes to every peer out of band. The single-switch `ISiloBuilder.AddLatticeAutoSharedDictionary(...)` composes all of that - plus cross-cluster byte distribution - into one opt-in call, so two clusters converge on a shared dictionary and compress wire traffic with it from one flag and no asset provisioning:

```text
using Orleans.Lattice.Replication;

siloBuilder.AddLatticeAutoSharedDictionary(o =>
{
    o.MaxSampleCount = 1024;
    o.MinTrainingInterval = TimeSpan.FromMinutes(5);
    // ...any CompressionDictionaryTrainingOptions knob; Enabled is forced on.
});
```

The helper registers the auto-training provider, the dictionary-aware Zstandard compressor, the sampling driver, and the training pump, and turns on `LatticeReplicationOptions.AutoSharedDictionaryEnabled`. Default build behaviour is unchanged - with the switch off there is no sampling, no training, no new RPC traffic, and the wire stays byte-identical. Four parts make the auto path work end to end:

- **Sampling.** `ReplicationMutationObserver` feeds outbound point-`Set` payloads into the provider's reservoir through the `ILatticeCompressionDictionarySampler` seam, so the reservoir fills from real ship traffic without host code. Deletes, range marks, and empty values are never sampled.
- **Pumping.** The hosted `AutoSharedDictionaryTrainingService` calls `TryTrain()` on a turn-safe, rate-limited cadence (bounded by `MinTrainingInterval`), off the hot path.
- **Activation.** The provider implements `ILatticeActiveCompressionDictionary`, exposing the freshest trained id as `ActiveDictionaryId`. When the feature is on the shipper auto-selects that id for framing and per-peer negotiation - there is no `FramingCompressionDictionaryId` to hand-set, and the active id tracks each roll-over automatically.
- **Byte distribution.** A receiver that does not yet hold an advertised dictionary id **pulls** the bytes from the advertising peer over the `IReplicationDigestProbeTransport.PullCompressionDictionaryAsync` transport seam (carried by `CompressionDictionaryPullRequest` / `CompressionDictionaryPullResponse`). The pulled bytes are re-verified against the advertised content fingerprint and installed through the content-addressed, idempotent `ILatticeCompressionDictionarySink.TryInstall` before they are ever used: an id already resolving to byte-identical bytes is a no-op success, and the same id with *different* bytes is rejected, so a pulled payload can never overwrite an in-use dictionary. The convergence walk lives in `CompressionDictionaryConvergence.ConvergeAsync` and runs automatically before each negotiation.

The pull seam defaults to a no-op that reports "not supported", so an un-upgraded peer or an unbound transport simply falls back to dictionary-less `Zstd` and stays wire-identical - the feature is rolling-upgrade safe. Because dictionary ids stay locally assigned (never content-derived), the same-id/different-bytes collision two auto-training clusters would otherwise hit is caught by the fingerprint guard and resolved by pulling the peer's bytes rather than by a brittle global id scheme. Convergence pulls are observable on the `ship.dictionary_convergence` counter (tagged `tree`/`peer`/`outcome`) and its Replication-dashboard panel.

### Scope

The shipped surface covers operator-supplied (pre-trained) dictionaries, runtime auto-training, and per-peer capability negotiation end to end: dictionary carriage in the tail, receiver-side dictionary selection, graceful fallback, opt-in options, validation, versioned roll-over, before/after observability, and **per-peer capability negotiation** of shared-dictionary support over the ack-based capability channel. With `DictionaryNegotiationEnabled` set on a `ZstdDictionary` tree, a receiver advertises which dictionary ids it can resolve (on `ReplicationAck.AdvertisedDictionaryIds`, sourced from a provider that implements `ILatticeCompressionDictionaryCatalog`) and a sender only compresses a batch with the configured dictionary id for a peer that has advertised it, falling back to plain dictionary-less `Zstd` for any peer that has not - so a mixed fleet keeps shipping during a rolling dictionary rollout and no peer ever receives a frame it cannot decode. The per-peer negotiated state is refreshed on every ack, so it adapts on reconnect or capability change. The negotiation is default-off and additive on the wire; the encoder's local-availability fallback remains the last-resort safety net. A receiver additionally advertises a **content fingerprint** per dictionary id (on `ReplicationAck.AdvertisedDictionaries`), and an opted-in sender gates dictionary compression on `(id, fingerprint)` rather than the bare id: if two deployments map the same id to different bytes (an operator slip, or two clusters that each auto-trained an id 1 dictionary), the fingerprints disagree, the sender falls back to dictionary-less `Zstd`, and a distinct `fingerprint_mismatch` negotiation outcome plus a one-shot warning surface the misconfiguration instead of letting it manifest as a receiver-side decode failure - see [`docs/lattice.replication/wire-format.md`](../lattice.replication/wire-format.md). Auto-training and negotiation compose: a dynamically trained dictionary's id is advertised through the same catalog seam, so a trained dictionary can be negotiated per peer once it is provisioned to the receiver. Provisioning is no longer a manual step either: with `AddLatticeAutoSharedDictionary` the trained **bytes** are distributed to a peer that lacks them over the pull seam and installed fingerprint-verified before use (see [Self-distributing auto-dictionary](#self-distributing-auto-dictionary)), closing the last out-of-band gap.


## Azure Table WAL row-payload compression

The Azure Table WAL provider is the second in-tree consumer of the compression seam. Each WAL entry row has its `Payload` column compressed before it is persisted, shrinking the retained on-disk footprint - and the per-append managed allocations - of larger mutations. **Compression is enabled by default** (`Compression = LatticeCompression.Zstd`): `AddAzureTableWalStorage` registers the Zstd compressor automatically, so the default needs no extra wiring. Configure or opt out per tree via `AzureTableWalStorageOptions`:

```text
siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";

    // Compression is ON by default (Zstd, level 3). To turn it off:
    //   o.Compression = LatticeCompression.None;
    // To change the encoded size below which a row is left uncompressed:
    //   o.CompressionMinPayloadBytes = 256;
});
```

| Option | Type | Default | Safe to change after data exists? | Meaning |
|---|---|---|---|---|
| `Compression` | `LatticeCompression` | `Zstd` (on) | Yes - existing rows decode by their own stored tag, so old and new rows coexist. | Algorithm tag applied to each entry row's `Payload`. Defaults to `Zstd`. Set `None` to write the verbatim Orleans-binary-serialised `WalRecord`; a host tag in `[0x80, 0xFF]` selects a custom algorithm. |
| `CompressionMinPayloadBytes` | `int` | `256` | Yes - only governs whether *new* rows are compressed; never affects how stored rows read back. | Encoded-payload threshold below which a row is stored uncompressed (tag `None`) even when `Compression` is enabled. `0` compresses every row. Validated to be non-negative. See [Choosing the threshold](#choosing-the-compressionminpayloadbytes-threshold). |

The Zstd **compression level** (set on the registered `ZstdLatticeCompressor` - see [Registration and the level-coupling note](#registration-and-the-level-coupling-note)) is likewise safe to change after rows exist: the level steers only how *new* rows are compressed and is never needed to read a stored row back. Each compressed payload is a self-describing Zstd frame carrying its own length prefix, so a row written at one level decompresses unchanged regardless of the level configured later. Rows written at different levels (or with different algorithms) freely coexist in the same table.

When a row is compressed, its `Payload` column holds `[4-byte little-endian uncompressed length][compressed bytes]` and the row's `Compression` column carries the algorithm tag (stored as an `int` because Azure Table Storage has no single-byte EDM property type). On read the provider keys on the row's stored tag - **not** the reader's `Compression` option or level - so a reader configured with `Compression = None` still inflates compressed rows as long as the matching `ILatticeCompressor` is registered. Rows written before this column existed decode the absent property to `0` (`None`) and read back verbatim, so the change is backwards-compatible with no migration.

Both encode paths compress (`AppendBatchAsync` and the shipper's pre-encoded `AppendEncodedBatchAsync` fast path) and both read paths inflate (`ReadAsync` and `ReadEncodedAsync`), so the shipper still observes verbatim encoded `WalRecord` bytes regardless of the on-disk tag.

**Inflation guard.** If compressing a payload does not actually shrink it - i.e. the compressed bytes plus the 4-byte length prefix are not smaller than the input, as happens for incompressible data such as already-compressed blobs or random bytes - the provider stores that row verbatim with tag `None` instead. Enabling compression therefore never grows a row beyond its uncompressed size, even for binary values; the only cost in that case is the compression attempt's CPU, which the `CompressionMinPayloadBytes` threshold already keeps off the smallest rows.

### Observability: compression-savings metrics

Each append batch a compressing WAL provider commits emits two monotonic counters on the `orleans.lattice` meter, both in bytes and tagged by `tree`, so a dashboard can chart the realised savings per tree:

| Metric | Unit | Tags | Meaning |
|---|---|---|---|
| `orleans.lattice.storage.wal.uncompressed_bytes` | `By` | `tree` | Pre-compression encoded payload bytes the batch attempted to store. |
| `orleans.lattice.storage.wal.stored_bytes` | `By` | `tree` | Post-compression payload bytes the batch actually stored. |
| `orleans.lattice.storage.wal.compression_skipped` | `{row}` | `tree`, `reason` | Rows stored verbatim instead of compressed, attributed by `reason`. |

The **savings ratio** for a tree is `1 - stored_bytes / uncompressed_bytes` (a PromQL `rate()` ratio of the two counters). They are plain counters rather than an observable savings gauge so the totals need no staleness-horizon handling and survive activation churn; a row that skips compression counts its verbatim length into *both* byte totals, so a tree with no realised savings reports an equal pair rather than a gap.

The `reason` tag on `compression_skipped` is one of:

- `below_threshold` - the encoded payload was shorter than `CompressionMinPayloadBytes`, so the row was left uncompressed by design.
- `inflation_guard` - compressing did not shrink the payload (incompressible data), so the verbatim bytes were stored instead.
- `disabled` - compression is not enabled on the provider (`Compression = None`).

A persistently low savings ratio whose `compression_skipped` is dominated by `below_threshold` is the signal to lower `CompressionMinPayloadBytes`; one dominated by `inflation_guard` means the payloads are genuinely incompressible and lowering the threshold would only burn CPU.
### Choosing the `CompressionMinPayloadBytes` threshold

The default (`256`) was chosen empirically for JSON values, the dominant payload shape, by driving realistic JSON records through the real encode + Zstd-3 path and measuring stored-byte savings and per-row CPU across a payload-size sweep:

| Encoded payload (bytes) | Stored after Zstd-3 | Reduction |
|---:|---:|---:|
| 112 (empty value, metadata only) | 100 | 11% |
| 152 | ~140 | ~8% |
| 190 | ~162 | ~15% |
| 229 | 175 | 24% |
| 269 | 192 | 29% |
| 347 | 225 | 35% |
| 547 | 283 | 48% |
| 630 | 308 | 51% |
| 1177 | 436 | 63% |
| 2185 | 620 | 72% |
| 4226 | 1008 | 76% |

Two findings drive the default:

1. **JSON never inflates.** Compressed-plus-prefix output was smaller than the input at *every* size measured, down to a metadata-only 112-byte payload. So unlike incompressible binary, JSON has no break-even floor the threshold must protect against - the threshold's only job here is to avoid spending CPU for a saving too small to matter.
2. **The reduction crosses ~25% near 256 encoded bytes and climbs steeply above it**, while below ~190 bytes it falls under ~15% (tens of bytes) for the same roughly-fixed ~5 µs per-row compression cost. `256` sits at that knee: it captures the high-value range and skips only the smallest payloads where the fixed cost isn't repaid.

The previous `512` default (inherited from the replication framing-tail threshold, which guards *batched* tails, not per-row JSON) left a large band of 256-511-byte rows - which compress 25-48% - stored verbatim. Tune from the default as follows:

- **Footprint-bound workload** (Azure Table request size / account throughput is the bottleneck and CPU is cheap): set `CompressionMinPayloadBytes = 0` to compress every row, since JSON savings stay net-positive all the way down.
- **CPU-bound silo / incompressible binary values**: raise it so the per-row cost is only paid on rows large enough to yield a worthwhile absolute saving.

### Compression level: empirically level-independent for the threshold

A natural question is whether the optimal `CompressionMinPayloadBytes` should change with the Zstd level. An in-process sweep of eight levels - {1, 3, 6, 9, 12, 15, 19, 22} - against the same JSON payload-size range says **no**. For the sub-512-byte payloads a threshold can actually affect, the achievable compression ratio (and therefore the savings-based optimal threshold) is essentially level-independent: the smallest encoded size reaching a 20% reduction is ~229 bytes at *every* level from 1 to 22, and for a 25% target it moves by at most one sampling step (269 -> 229 bytes) only at level 15 and above. Levels diverge meaningfully only on large payloads (> 1 KB), which sit well above any sensible threshold and are always compressed regardless.

What *does* change dramatically with level is CPU: the same ~250-byte row costs ~6 µs to compress at level 3 but ~30-40 µs at levels 15-22 - a 5-6x increase for no extra small-payload saving. The provider therefore defaults to **Zstd level 3** (`LatticeAzureTableServiceCollectionExtensions.DefaultCompressionLevel`) - the cheapest level that already captures the full small-payload ratio - and keeps a single `CompressionMinPayloadBytes` default that is correct whatever level a host pins.

### Memory-allocation impact

The headline benefit is not only on-disk bytes; it is a large drop in per-append managed allocations for big payloads, measured by the `EncodeWalBatch_AzureTable_Zstd` microbenchmark:

- At **large values (~4 KB)**, encoding a full 99-entry batch allocates **~81% fewer bytes** with Zstd than without (~456 KB -> ~85 KB), and Gen0/Gen1 collections fall to ~0 - the compressed rows are small enough to avoid the gen-promotion and large-buffer paths the uncompressed arrays hit.
- At **small values (~128 B)**, allocations are roughly equal: the 256-byte threshold leaves these rows uncompressed, so there is no allocation (or CPU) penalty for enabling compression on small-mutation workloads.

Compression never *increased* allocations at any measured size. Together with the inflation guard and the size threshold, this is why the feature is safe to enable by default: large payloads get a substantial footprint-and-allocation win, while small or incompressible payloads fall through to the verbatim path at negligible cost.

### Registration and the level-coupling note

`AddAzureTableWalStorage` registers a Zstd fallback compressor (`ZstdLatticeCompressor` at `LatticeAzureTableServiceCollectionExtensions.DefaultCompressionLevel`, currently `3`) via `TryAddEnumerable`, so enabling `Compression = LatticeCompression.Zstd` works without any extra wiring. Because registration is additive and idempotent, **the first fallback factory to run wins the compression level**: if a host has already registered its own `ZstdLatticeCompressor` instance (e.g. for replication at a different level), `TryAddEnumerable` will not add the WAL default, and the WAL path reuses the host's instance and its level. To pin a specific WAL level, register the compressor instance explicitly before calling `AddAzureTableWalStorage`. Changing that level later is safe even after rows have been written - because the on-disk frame is self-describing and reads key on the per-row algorithm tag, the new level applies only to subsequently written rows while older rows continue to decode unchanged.

### Why a shared byte-tag registry, not a WAL-specific compressor contract

The WAL path deliberately reuses the same `IEnumerable<ILatticeCompressor>` DI registry and `LatticeCompression` byte tags as replication rather than introducing a parallel registry. Should a layer ever need WAL-specific compressor selection that must not be shared with replication, the intended evolution is to introduce a dedicated `IWalPayloadCompressor` marker contract over the same byte-tag space, rather than splitting or namespacing the existing tag registry.

## Reusing the seam in other layers

`ILatticeCompressor` lives in the core `Orleans.Lattice` package precisely so non-replication layers can reuse it. Planned consumers:

- **Azure Table WAL row payloads.** Shipped - see [Azure Table WAL row-payload compression](#azure-table-wal-row-payload-compression) above. Hosts that have already registered a compressor for replication get WAL compression for free.
- **Snapshots / cold-storage tiers.** Reserved for future work; the seam shape is byte-in / byte-out specifically so these layers do not need a fresh contract.

## Testing

The public registration surface is pinned by:

- `LatticeCompressionServiceCollectionExtensionsTests` (core unit tests) - null-guards, idempotency, instance vs type, side-by-side registration.
- `PublicApiContractTests.Compression` partial (core integration suite) - the supported public DI shape hosts depend on.
- `CompressedFramingRoundtripTests` (replication tests) - byte-keyed dispatch round-trip including a host-reserved tag in the `[0x80, 0xFF]` range.
- `LatticeReplicationOptionsValidatorTests` (replication tests) - host-reserved tags pass validation; core-range typos still fail.
- `AzureTableWalStorageProviderTests.Compression` and `AzureTableWalStorageOptionsTests` (Azure Table tests) - white-box encode tagging, threshold behaviour, constructor guards, and option defaults/validation.
- `CompressedAzureTableWalIntegrationTests` (Azure Table tests, emulator-gated) - end-to-end compress/decompress round-trip against Azurite, including raw-row tag verification and backwards-compatible reads of uncompressed rows.

Run the relevant suites with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Compression|FullyQualifiedName~PublicApiContractTests.Compression"
dotnet test test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj --filter "FullyQualifiedName~Compress"
```