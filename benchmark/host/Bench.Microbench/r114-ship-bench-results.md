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
