# Bench.WalAzureTable

Azurite-backed concurrency-sweep probe for the per-batch Azure Table WAL
partitioning design.

## What the tracked feature actually asks for

The replication feature on per-batch Azure Table partition keys and
manifest-driven reads carries this acceptance text:

> With WalMaxPendingBatches = 4 against the Azure Tables emulator, a
> 1024-entry burst spread across 8 shards shows throughput-uplift over
> today's path proportional to partition-server count;
> GetHighestOffsetAsync reports a strictly monotonic sequence across
> concurrent appends (no clobber).

That sentence was written before the redesign landed. It has two halves
that age differently:

1. **Strict monotonicity under concurrent appends.** This half is the
   correctness invariant - it survived the redesign unchanged and is
   independently verifiable. The probe asserts it directly.

2. **"Throughput uplift over today's path proportional to
   partition-server count."** "Today's path" referred to the
   pre-redesign single-partition layout that this branch has already
   removed. A literal A/B between old and new on the same binary is
   therefore no longer available. The probe addresses this half via a
   concurrency sweep on the redesigned provider (the operational proxy
   for partition-server parallelism) and a structural assertion that
   every batch lands in its own Azure Table partition (the schema-level
   precondition for that parallelism).

## Why the local probe cannot prove the uplift quantitatively

Azurite is a single-process emulator. Every partition it hosts is
served by the same write loop on the same machine, so partitioning the
data across N partitions does not buy real partition-server
parallelism on the emulator. The probe's concurrency sweep
(`c = 1, 2, 4, 8`) therefore tends to flatten near `1.0x` on Azurite
regardless of how well the schema scales on a real account. **The
local probe is not the right place to ship a "look at the speedup"
number.** It is the right place to ship two other things:

- A **strict structural assertion** that, after a 1024-entry burst
  across 8 shards, the table contains exactly `shards *
  batches_per_shard` distinct `_b_|...` batch-partition keys. If that
  number ever drops below expected, batches are sharing a partition
  and the parallelism precondition is broken regardless of what any
  speedup-number looks like.

- A **strict monotonicity assertion** on `GetHighestOffsetAsync`
  sampled across the burst, per shard, with zero violations.

Quantitative uplift over a single-partition baseline needs a real
Azure Tables account (or a multi-server emulator) and is out of scope
for the inner dev loop.

## Workload

For each concurrency level `c` in the sweep:

- 1024 entries are spread across 8 shards (128 per shard).
- Each shard pushes the entries as 16 batches of 8 entries.
- Up to `c` batches per shard are in flight simultaneously.
- All 8 shards run their bursts concurrently with each other.
- A sampler polls `GetHighestOffsetAsync` per shard every ~5 ms and
  the per-shard head sequence is checked for monotonicity post-burst.
- After the burst drains, the probe reads back every entry per shard
  and counts the distinct batch-partition keys in the underlying
  table.

A warm-up burst at `c = 4` against a throwaway table runs first to
absorb JIT, Azurite-first-touch, and table-create costs, and is
excluded from the scaling comparison.

## Output

Console summary: one line per sweep point, showing burst time,
entries/s, batches/s, scale-vs-c1, observed-vs-expected distinct
batch-partition counts, and the monotonicity sample count or
violation count.

A JSON report is printed after the summary with the same data per
sweep point plus a top-level `success` flag.

## Interpreting the numbers

| Column | What it tells you |
|---|---|
| `burst_ms` | Wall-clock for the full 1024-entry burst at that concurrency. Dominated by Azurite's write loop locally. |
| `entries_per_second` | Absolute throughput. Useful only in *relative* comparison across the sweep. |
| `scale_vs_c1` | Throughput at concurrency `c` divided by throughput at `c = 1`. On Azurite this hovers near `1.0x`; on a real Azure Tables account this should grow with concurrency until partition-server count or per-shard supply runs out. |
| `distinct batch-parts` | Observed-vs-expected count of `_b_|...` partition keys in the table after the burst. **Must equal `expected`** - if not, the schema-level precondition for partition-server parallelism is broken. |
| `monotonicity` | Either `STRICT (N samples)` or a violation count. Must be `STRICT`. |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Burst succeeded, monotonicity strict, distinct batch-partition count matches expected. |
| `2` | Azurite is not reachable on `UseDevelopmentStorage=true`. |
| `3` | At least one of: monotonicity violations, read-back mismatch, or distinct-batch-partition count mismatch. |

## Running it

Start Azurite first (Docker is fine; the repo's `azurite` script also
works). Then:

```powershell
dotnet run --project benchmark/host/Bench.WalAzureTable -c Release
```

The probe runs in a few seconds against a local emulator. It does not
depend on BenchmarkDotNet because BenchmarkDotNet's run model (many
small iterations of an isolated method) is the wrong shape for a
WAL-append burst that has to be observed in flight by a sampler.

## What "passing" actually proves

A passing run proves:

1. The redesigned provider stamps every batch into its own Azure
   Table partition - the schema-level precondition for partition-server
   parallelism on a real Azure Tables account.
2. `GetHighestOffsetAsync` reports a strictly monotonic sequence per
   shard during concurrent appends, so concurrent batches do not
   clobber the high-water mark.
3. The full burst round-trips through read-back with every shard's
   tail at the expected offset.

It does **not** prove a quantitative throughput uplift on Azurite.
That measurement requires a real Azure Tables endpoint and is
deliberately not promised by this probe.
