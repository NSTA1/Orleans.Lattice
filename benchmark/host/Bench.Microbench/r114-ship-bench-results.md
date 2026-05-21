# R-114 ship-path A/B benchmark results

## Purpose

R-114 (one-encode migration) flips the WAL ship path from re-serializing
a typed `ReplicationBatchEnvelope` on every drain to framing the
already-encoded WAL segments straight off the storage provider. The
encode-only microbench (`EncodeWalBatch_AzureTable`) is allocation-
neutral across stages because the per-record encoder did not change;
the savings show up on the **composite ship path**, where the typed
envelope path re-encodes every entry's payload through the Orleans
serializer and the framing path simply length-prefixes the bytes that
were already written to storage.

`Ship_TypedEnvelope` and `Ship_FramingOnly` in
`LatticeMicroBenchmarks.cs` exercise the marshaller-only seam with
distinct per-entry payload buffers (each `WalRecord.Value` is its own
heap object, mirroring real producer behavior - see the comment block
in `BuildShipFramingFixture` for why this matters).

## Run command

```pwsh
dotnet build benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release
dotnet run -c Release --no-build --project benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj `
    -- --filter "*Ship*" --memory --warmupCount 3 --iterationCount 3 --launchCount 1
```

Wall-clock: ~4.5 minutes for the 24-row sweep on AMD Ryzen 7 PRO 7840U,
.NET 10.0.8, BenchmarkDotNet 0.15.8.

## Results - allocated bytes per call

| entries | payload | typed envelope (today) | framing only (R-114) | reduction |
|--------:|--------:|-----------------------:|---------------------:|----------:|
| 16      | 64      | 7.90 KB                | 7.90 KB              | 0%        |
| 16      | 1024    | 67.95 KB               | 42.61 KB             | -37%      |
| 16      | 16384   | 1,013.01 KB            | 507.63 KB            | -50%      |
| 64      | 64      | 35.66 KB               | 31.95 KB             | -10%      |
| 64      | 1024    | 277.40 KB              | 173.25 KB            | -38%      |
| 64      | 16384   | 4,102.69 KB            | 2,078.40 KB          | -49%      |
| 256     | 64      | 157.07 KB              | 127.99 KB            | -19%      |
| 256     | 1024    | 1,126.59 KB            | 695.67 KB            | -38%      |
| 256     | 16384   | 16,473.02 KB           | 8,361.47 KB          | -49%      |
| 1024    | 64      | 654.11 KB              | 512.04 KB            | -22%      |
| 1024    | 1024    | 4,533.73 KB            | 2,785.26 KB          | -39%      |
| 1024    | 16384   | 65,964.14 KB           | 33,493.41 KB         | -49%      |

## Results - mean wall-clock per call

| entries | payload | typed envelope | framing only | speedup |
|--------:|--------:|---------------:|-------------:|--------:|
| 16      | 64      | 4.39 us        | 1.33 us      | 3.30x   |
| 16      | 1024    | 10.84 us       | 5.04 us      | 2.15x   |
| 16      | 16384   | 197.37 us      | 91.74 us     | 2.15x   |
| 64      | 64      | 17.85 us       | 4.35 us      | 4.10x   |
| 64      | 1024    | 69.34 us       | 38.55 us     | 1.80x   |
| 64      | 16384   | 649.05 us      | 367.74 us    | 1.76x   |
| 256     | 64      | 73.61 us       | 15.17 us     | 4.85x   |
| 256     | 1024    | 329.11 us      | 160.57 us    | 2.05x   |
| 256     | 16384   | 2,676.57 us    | 1,683.57 us  | 1.59x   |
| 1024    | 64      | 356.23 us      | 124.57 us    | 2.86x   |
| 1024    | 1024    | 1,222.94 us    | 574.29 us    | 2.13x   |
| 1024    | 16384   | 11,286.81 us   | 5,750.24 us  | 1.96x   |

## Interpretation

- Stage-1 wire-shape rename is allocation-neutral on the encode-only
  path; that was the earlier `EncodeWalBatch_AzureTable` A/B result and
  is not in scope for this benchmark.
- The composite ship path shows a steady ~50% allocation reduction at
  payload-dominated batches (>= 1 KiB per entry) and a ~2x wall-clock
  speedup, materialising exactly where R-114's design predicted: the
  typed envelope path re-allocates every value byte[] through the
  Orleans serializer, while the framing path frames the bytes already
  produced by the per-entry WAL encoder.
- At small payloads (64 B) the WAL header overhead dominates and the
  savings shrink to 0-22%, but the framing path is never worse than
  the typed path.

## R-116 third column - allocated bytes per call

R-116 strips the redundant `WalRecord.TreeId` slot from the per-entry
encoded bytes; the tree id is restored at every read seam from the
surrounding partition / framing context. Re-running `Ship_FramingOnly`
against the post-R-116 encoder with the same `"ship-bench"` (10-byte
UTF-8) tree name produces the third column below.

| entries | payload | framing only (R-114) | framing only (R-116) | absolute saving | per-entry |
|--------:|--------:|---------------------:|---------------------:|----------------:|----------:|
| 16      | 64      | 7.90 KB              | 7.90 KB              | 0 KB            | ~0 B      |
| 16      | 1024    | 42.61 KB             | 42.30 KB             | 0.31 KB         | ~20 B     |
| 16      | 16384   | 507.63 KB            | 507.32 KB            | 0.31 KB         | ~20 B     |
| 64      | 64      | 31.95 KB             | 31.95 KB             | 0 KB            | ~0 B      |
| 64      | 1024    | 173.25 KB            | 172.00 KB            | 1.25 KB         | ~20 B     |
| 64      | 16384   | 2,078.40 KB          | 2,077.14 KB          | 1.26 KB         | ~20 B     |
| 256     | 64      | 127.99 KB            | 127.99 KB            | 0 KB            | ~0 B      |
| 256     | 1024    | 695.67 KB            | 690.67 KB            | 5.00 KB         | ~20 B     |
| 256     | 16384   | 8,361.47 KB          | 8,356.49 KB          | 4.98 KB         | ~20 B     |
| 1024    | 64      | 512.04 KB            | 512.04 KB            | 0 KB            | ~0 B      |
| 1024    | 1024    | 2,785.26 KB          | 2,765.25 KB          | 20.01 KB        | ~20 B     |
| 1024    | 16384   | 33,493.41 KB         | 33,473.51 KB         | 19.90 KB        | ~20 B     |

### Interpretation - per-entry, not percentage

The headline number is the **per-entry absolute saving**, not the
percentage of total batch allocation. The per-entry saving is
constant at ~20 B for the bench's 10-byte tree name (10 UTF-8 bytes +
Orleans field tag + wire-type byte + length prefix + alignment
overhead), and scales linearly with `entryCount`. Production tree
names like `"orders/eu-west-1/v3"` (19 UTF-8 bytes) push the per-entry
saving to ~29 B; longer namespaced names like
`"tenants/acme/orders/eu-west-1/v3"` (32 bytes) reach ~42 B.

What this means in steady-state operations:

- A shipper draining 1024-entry batches saves ~20 KB of gRPC
  bandwidth and ~20 KB of WAL on-disk bytes per batch (10-byte tree
  name) or ~30 KB per batch (19-byte tree name).
- At a sustained 1k batches/sec cluster-wide, that is ~20 MB/sec of
  replication bandwidth and on-disk growth eliminated for the
  bench's tree-name length, ~30 MB/sec for production-shaped names.
- The percentage shrinks at large payload corners only because the
  payload `byte[]` dominates the batch by 3 orders of magnitude
  (16,384 B per entry vs. ~20 B saved). The percentage is bounded
  above by `treeIdWireBytes / (treeIdWireBytes + payloadBytes)` at
  any given corner, which is not the figure of merit for a
  bandwidth / WAL-growth optimisation - the per-entry constant is.

The zero-saving rows at `payload = 64 B` are an artefact of the
underlying allocator's bucket rounding: at small payloads, every
entry rounds up to the same allocator bucket whether the tree-id
slot is present or not, so the elision crosses below the bucket
granularity. The wire-bytes (and on-disk-bytes) saving is still
present in those rows, just not visible in the managed-allocation
counter the benchmark exposes.

## Fixture caveat

An early version of `BuildShipFramingFixture` shared a single
`payload` byte[] across all entries in a batch. The Orleans serializer
performs session-based reference deduplication, so each entry after
the first emitted as a small back-reference and the typed envelope
path appeared to allocate ~10x less than reality. The current fixture
allocates a distinct `byte[]` per entry, matching producer behavior
where each `SetAsync` call hands the WAL its own buffer. Reviewers
considering similar A/B fixtures should ensure per-entry payload
identity for any batch-encoding microbench.
