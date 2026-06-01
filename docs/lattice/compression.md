# Compression

This document is the source of truth for everything related to compression in Orleans.Lattice: the public seam, the registration pattern, the on-wire tag space, the configuration knobs, and the worked examples for plugging in a custom algorithm. Sibling docs (`wire-format.md`, `configuration.md`, `api.md`) link here instead of repeating the material.

## TL;DR

- The compression seam is the public interface `ILatticeCompressor` in the `Orleans.Lattice` namespace.
- The on-wire tag is a single `byte` (`LatticeCompression`), carried in plaintext alongside the relevant layer's fixed header so receivers can dispatch before allocating an inflate buffer.
- Register compressors on the silo's DI container via `AddLatticeCompressor` - the encoder layer keys its dispatch on the **raw byte**, not the named enum, so hosts can ship custom algorithms without core enum churn.
- Out of the box, replication ships Zstandard (`LatticeCompression.Zstd`) registered automatically by `AddLatticeReplication`.

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
| `0x01` | Core | `LatticeCompression.Zstd` - RFC 8478 Zstandard. Only the core `ZstdLatticeCompressor` type may claim this tag. |
| `0x02` - `0x7F` | Core (reserved) | Reserved for future core-shipped algorithms. Hosts must not invent values in this range; the encoder rejects such registrations with `ArgumentException` at silo startup. |
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

The replication layer is the first in-tree consumer of the compression seam. Compression is opt-in per cluster via `LatticeReplicationOptions`:

```text
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.FramingCompression = LatticeCompression.Zstd;
    o.FramingCompressionLevel = 3;          // Zstd: 1 (fastest) - 22 (highest ratio), default 3
    o.FramingCompressionMinBatchBytes = 512; // skip compression below this uncompressed tail size
});
```

| Option | Type | Default | Meaning |
|---|---|---|---|
| `FramingCompression` | `LatticeCompression` | `None` | Algorithm tag stamped into the framing header. |
| `FramingCompressionLevel` | `int` | `3` | Zstd compression level. Validated to `[1, 22]` when the algorithm is `Zstd`; ignored otherwise. |
| `FramingCompressionMinBatchBytes` | `int` | `512` | Uncompressed-tail threshold below which the shipper stamps `Compression = None` for the batch (heartbeats / small-bursty traffic skip the per-batch fixed overhead). `0` disables the threshold. |

Compression is invisible to the apply pipeline - `ReceiverFlowControlContext`, mutation observers, and the WAL replay path see the same plaintext entries regardless of the on-wire tag.

The wire-format layout of the compressed tail (length prefixes, alignment with the fixed plaintext header, no-wire-version-bump guarantee) is documented in [`docs/lattice.replication/wire-format.md`](../lattice.replication/wire-format.md).

## Reusing the seam in other layers

`ILatticeCompressor` lives in the core `Orleans.Lattice` package precisely so non-replication layers can reuse it. Planned consumers:

- **Azure Table WAL row payloads.** A future per-row compression path on the Azure Table WAL provider (tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice)) will dispatch through the same DI registration, using the same `LatticeCompression` tag stored as Azure Table metadata on each row. Hosts that have already registered a compressor for replication get WAL compression for free.
- **Snapshots / cold-storage tiers.** Reserved for future work; the seam shape is byte-in / byte-out specifically so these layers do not need a fresh contract.

## Testing

The public registration surface is pinned by:

- `LatticeCompressionServiceCollectionExtensionsTests` (core unit tests) - null-guards, idempotency, instance vs type, side-by-side registration.
- `PublicApiContractTests.Compression` partial (core integration suite) - the supported public DI shape hosts depend on.
- `CompressedFramingRoundtripTests` (replication tests) - byte-keyed dispatch round-trip including a host-reserved tag in the `[0x80, 0xFF]` range.
- `LatticeReplicationOptionsValidatorTests` (replication tests) - host-reserved tags pass validation; core-range typos still fail.

Run the relevant suites with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Compression|FullyQualifiedName~PublicApiContractTests.Compression"
dotnet test test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj --filter "FullyQualifiedName~Compress"
```