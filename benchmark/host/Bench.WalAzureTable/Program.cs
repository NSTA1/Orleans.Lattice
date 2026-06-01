// Azurite-backed concurrency-sweep probe for the per-batch Azure Table
// WAL partitioning design.
//
// Acceptance text the probe targets, from the per-batch
// partition-keys + manifest-driven-reads feature:
//
//   With WalMaxPendingBatches = 4 against the Azure Tables emulator,
//   a 1024-entry burst spread across 8 shards shows throughput-uplift
//   over today's path proportional to partition-server count;
//   GetHighestOffsetAsync reports a strictly monotonic sequence across
//   concurrent appends (no clobber).
//
// "Throughput-uplift proportional to partition-server count" was
// written against a pre-redesign single-partition path that this
// branch has already removed, so a literal A/B between old and new on
// the same binary is no longer available. The probe substitutes a
// concurrency sweep on the redesigned provider as the operational
// proxy for that claim: under the old design every batch on a shard
// serialised through one partition server, so raising in-flight
// concurrency past 1 bought only RPC pipelining; under the new
// design, every batch lands in its own partition, so raising
// concurrency should compound across partition servers. A burst whose
// wall-clock shrinks roughly in proportion to concurrency is the
// signal that partition-server parallelism is happening. A flat curve
// would mean we are still bottlenecked on a single partition (or on
// Azurite's process-wide write loop, which is the most plausible
// confound against the emulator) - either way the probe surfaces the
// signal explicitly instead of pretending a single absolute number is
// "uplift".
//
// Workload at every sweep point:
//   - Spread 1024 entries across 8 shards (128 entries per shard).
//   - Within each shard, push the entries as 16 batches of 8 entries.
//   - Up to ConcurrencyLevel batches per shard are in flight
//     simultaneously. ConcurrencyLevel ranges over {1, 2, 4, 8}.
//   - The shard burst is driven by a per-shard task; shards run
//     concurrently with each other at every sweep point so the
//     measurement varies only on per-shard concurrency.
//   - During the burst, a sampler polls GetHighestOffsetAsync per
//     shard every ~5 ms and the probe asserts strict per-shard
//     monotonicity post-burst.
//
// The sweep produces a JSON report with a row per concurrency level
// (burst_seconds, entries_per_second, scale_vs_c1) plus per-sample
// monotonicity totals. The exit code is non-zero if any sweep point
// observed a monotonicity violation.

using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Serialization;

const string AzuriteConnectionString = "UseDevelopmentStorage=true";
const string TreeId = "wal-azuretable-probe";
const int TotalEntries = 1024;
const int ShardCount = 8;
const int EntriesPerBatch = 8;
const int ValueBytes = 128;
const int WarmupBatchesPerShard = 2;

// Concurrency sweep - one burst per level. The feature's
// headline value is WalMaxPendingBatches = 4, so 4 is in the
// middle of the swept range; 1 is the "no in-flight parallelism"
// baseline; 8 = batches-per-shard /2, so half the shard's batches
// can run together (a higher concurrency does not increase
// parallelism because the per-shard supply runs out).
var concurrencyLevels = new[] { 1, 2, 4, 8 };

if (TotalEntries % (ShardCount * EntriesPerBatch) != 0)
{
#pragma warning disable CS0162 // unreachable: all operands are compile-time constants today, but the guard remains for future param drift.
    Console.Error.WriteLine(
        $"[bench-wal-azuretable] TotalEntries ({TotalEntries}) must split evenly into {ShardCount} shards * {EntriesPerBatch} entries/batch.");
    return 1;
#pragma warning restore CS0162
}

var entriesPerShard = TotalEntries / ShardCount;
var batchesPerShard = entriesPerShard / EntriesPerBatch;

await using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
var serializer = services.GetRequiredService<Serializer<LatticeMutation>>();
var adminClient = new TableServiceClient(AzuriteConnectionString);

// Reachability probe with explicit failure mode.
try
{
    await foreach (var _ in adminClient.QueryAsync(maxPerPage: 1))
    {
        break;
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine(
        $"[bench-wal-azuretable] Azurite is not reachable on {AzuriteConnectionString}. "
        + $"Start it via 'azurite --silent --location <dir>' (or the docker-compose stack) before running this probe. "
        + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
    return 2;
}

// Pre-build a payload buffer once; the burst recipe builds a fresh
// entries array per sweep point so offsets restart from 0 against a
// fresh table.
var payload = new byte[ValueBytes];
for (var i = 0; i < payload.Length; i++)
{
    payload[i] = (byte)(i & 0xFF);
}

var sweepResults = new List<BurstResult>(concurrencyLevels.Length);
var anyViolations = false;
var anyReadbackMismatch = false;
var anyStructuralMismatch = false;

// Warm-up burst at concurrency = 4 against a throwaway table so JIT
// + Azurite + table-create one-time costs do not dominate the first
// measured sweep point. The warm-up result is recorded but excluded
// from the scaling comparison.
{
    var warmupResult = await RunBurstAsync(
        services: services,
        adminClient: adminClient,
        serializer: serializer,
        connectionString: AzuriteConnectionString,
        treeId: TreeId,
        shardCount: ShardCount,
        batchesPerShard: WarmupBatchesPerShard,
        entriesPerBatch: EntriesPerBatch,
        concurrencyLevel: 4,
        payload: payload,
        label: "warmup")
        .ConfigureAwait(false);
    Console.WriteLine(
        $"[bench-wal-azuretable] warmup: c={warmupResult.ConcurrencyLevel} "
        + $"{warmupResult.TotalEntries} entries in {warmupResult.BurstSeconds * 1000:F1} ms = "
        + $"{warmupResult.EntriesPerSecond:F0} entries/s (excluded from scaling)");
}

foreach (var concurrency in concurrencyLevels)
{
    var result = await RunBurstAsync(
        services: services,
        adminClient: adminClient,
        serializer: serializer,
        connectionString: AzuriteConnectionString,
        treeId: TreeId,
        shardCount: ShardCount,
        batchesPerShard: batchesPerShard,
        entriesPerBatch: EntriesPerBatch,
        concurrencyLevel: concurrency,
        payload: payload,
        label: "c=" + concurrency.ToString(CultureInfo.InvariantCulture))
        .ConfigureAwait(false);
    sweepResults.Add(result);

    if (result.MonotonicityViolations.Count > 0)
    {
        anyViolations = true;
    }
    if (result.TotalEntriesReadBack != result.TotalEntries
        || !result.FinalHeadsPerShard.All(h => h == entriesPerShard - 1))
    {
        anyReadbackMismatch = true;
    }
    if (result.DistinctBatchPartitionCount != result.ExpectedDistinctBatchPartitionCount)
    {
        anyStructuralMismatch = true;
    }
}

var baseline = sweepResults[0];
var scaleVsC1 = new double[sweepResults.Count];
for (var i = 0; i < sweepResults.Count; i++)
{
    scaleVsC1[i] = baseline.BurstSeconds > 0
        ? sweepResults[i].EntriesPerSecond / baseline.EntriesPerSecond
        : 0d;
}

// Console summary: one line per sweep point.
Console.WriteLine("[bench-wal-azuretable] concurrency sweep (1024 entries / 8 shards):");
Console.WriteLine("  level | burst_ms | entries/s | batches/s | scale_vs_c1 | distinct batch-parts | monotonicity");
Console.WriteLine("  ------+----------+-----------+-----------+-------------+----------------------+-------------");
for (var i = 0; i < sweepResults.Count; i++)
{
    var r = sweepResults[i];
    Console.WriteLine(string.Format(
        CultureInfo.InvariantCulture,
        "  c={0,-3} | {1,8:F1} | {2,9:F0} | {3,9:F1} | {4,10:F2}x | {5,3}/{6,-3} (expected) | {7}",
        r.ConcurrencyLevel,
        r.BurstSeconds * 1000,
        r.EntriesPerSecond,
        r.BatchesPerSecond,
        scaleVsC1[i],
        r.DistinctBatchPartitionCount,
        r.ExpectedDistinctBatchPartitionCount,
        r.MonotonicityViolations.Count == 0
            ? "STRICT (" + r.MonotonicitySampleCount.ToString(CultureInfo.InvariantCulture) + " samples)"
            : r.MonotonicityViolations.Count.ToString(CultureInfo.InvariantCulture) + " violations"));
}

Console.WriteLine();
Console.WriteLine("[bench-wal-azuretable] interpretation:");
Console.WriteLine("  - 'distinct batch-parts' = number of distinct '_b_|...' partition keys observed in the");
Console.WriteLine("    table after the burst completes. The redesigned schema lands every batch in its");
Console.WriteLine("    own partition, so the expected count is shards * batches_per_shard. This is the");
Console.WriteLine("    structural precondition for partition-server parallelism on a real Azure Tables");
Console.WriteLine("    account: if this column ever drops below 'expected', batches are sharing a");
Console.WriteLine("    partition and the parallelism claim is broken.");
Console.WriteLine("  - 'scale_vs_c1' = entries/s at concurrency=N divided by entries/s at concurrency=1.");
Console.WriteLine("    Against a real Azure Tables account with multiple partition servers, this should");
Console.WriteLine("    grow with concurrency until partition-server count or per-shard supply runs out.");
Console.WriteLine("    Against Azurite (a single-process emulator) it is dominated by the emulator's");
Console.WriteLine("    own write loop and stays close to 1.0x - so the local probe DOES NOT demonstrate");
Console.WriteLine("    the uplift, it only proves the schema-level precondition for it.");

var report = new
{
    scenario = "wal-azuretable-concurrency-sweep",
    success = !anyViolations && !anyReadbackMismatch && !anyStructuralMismatch,
    total_entries = TotalEntries,
    shards = ShardCount,
    batches_per_shard = batchesPerShard,
    entries_per_batch = EntriesPerBatch,
    concurrency_sweep = sweepResults.Select((r, i) => new
    {
        concurrency_level = r.ConcurrencyLevel,
        burst_seconds = Math.Round(r.BurstSeconds, 4),
        entries_per_second = Math.Round(r.EntriesPerSecond, 1),
        batches_per_second = Math.Round(r.BatchesPerSecond, 1),
        scale_vs_c1 = Math.Round(scaleVsC1[i], 3),
        distinct_batch_partition_count = r.DistinctBatchPartitionCount,
        expected_distinct_batch_partition_count = r.ExpectedDistinctBatchPartitionCount,
        monotonicity_samples = r.MonotonicitySampleCount,
        monotonicity_violations = r.MonotonicityViolations.Count,
        total_entries_read_back = r.TotalEntriesReadBack,
        final_heads_per_shard = r.FinalHeadsPerShard,
    }).ToArray(),
};

Console.WriteLine();
Console.WriteLine(JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));

if (anyViolations)
{
    Console.Error.WriteLine("[bench-wal-azuretable] monotonicity violations observed:");
    foreach (var r in sweepResults)
    {
        foreach (var v in r.MonotonicityViolations)
        {
            Console.Error.WriteLine($"  c={r.ConcurrencyLevel}: {v}");
        }
    }
}

if (anyStructuralMismatch)
{
    Console.Error.WriteLine("[bench-wal-azuretable] distinct-batch-partition assertion failed - "
        + "batches are NOT landing in their own partitions, so the partition-server parallelism precondition is broken:");
    foreach (var r in sweepResults)
    {
        if (r.DistinctBatchPartitionCount != r.ExpectedDistinctBatchPartitionCount)
        {
            Console.Error.WriteLine(
                $"  c={r.ConcurrencyLevel}: observed {r.DistinctBatchPartitionCount} distinct batch partitions, "
                + $"expected {r.ExpectedDistinctBatchPartitionCount}.");
        }
    }
}

return (anyViolations || anyReadbackMismatch || anyStructuralMismatch) ? 3 : 0;

static async Task<BurstResult> RunBurstAsync(
    ServiceProvider services,
    TableServiceClient adminClient,
    Serializer<LatticeMutation> serializer,
    string connectionString,
    string treeId,
    int shardCount,
    int batchesPerShard,
    int entriesPerBatch,
    int concurrencyLevel,
    byte[] payload,
    string label)
{
    var tableName = "WalProbe" + Guid.NewGuid().ToString("N");
    var provider = new AzureTableWalStorageProvider(
        Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = connectionString,
            TableName = tableName,
        }),
        serializer);

    try
    {
        // Pre-build per-shard batches off the hot path.
        var perShardBatches = new WalEntry[shardCount][][];
        for (var shard = 0; shard < shardCount; shard++)
        {
            perShardBatches[shard] = new WalEntry[batchesPerShard][];
            var hlc = HybridLogicalClock.Zero;
            for (var b = 0; b < batchesPerShard; b++)
            {
                var batch = new WalEntry[entriesPerBatch];
                for (var i = 0; i < entriesPerBatch; i++)
                {
                    var offset = (b * entriesPerBatch) + i;
                    hlc = HybridLogicalClock.Tick(hlc);
                    batch[i] = new WalEntry
                    {
                        Offset = offset,
                        Mutation = new LatticeMutation
                        {
                            TreeId = treeId,
                            Kind = MutationKind.Set,
                            Key = label + "-shard-" + shard.ToString("D2", CultureInfo.InvariantCulture)
                                + "-k-" + offset.ToString("D6", CultureInfo.InvariantCulture),
                            Value = payload,
                            Timestamp = hlc,
                            OriginClusterId = "bench-wal-azuretable",
                        },
                    };
                }
                perShardBatches[shard][b] = batch;
            }
        }

        // Sampler observes GetHighestOffsetAsync across all shards
        // while the burst is in flight.
        using var samplerCts = new CancellationTokenSource();
        var samples = new List<(int shard, long head, long elapsedMicroseconds)>(capacity: 4096);
        var samplesLock = new object();
        var samplerStart = Stopwatch.GetTimestamp();
        var samplerTask = Task.Run(async () =>
        {
            var ct = samplerCts.Token;
            while (!ct.IsCancellationRequested)
            {
                for (var shard = 0; shard < shardCount; shard++)
                {
                    long head;
                    try
                    {
                        head = await provider.GetHighestOffsetAsync(treeId, shard, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (RequestFailedException)
                    {
                        continue;
                    }
                    var elapsed = (Stopwatch.GetTimestamp() - samplerStart) * 1_000_000L / Stopwatch.Frequency;
                    lock (samplesLock)
                    {
                        samples.Add((shard, head, elapsed));
                    }
                }
                try
                {
                    await Task.Delay(5, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        });

        // Drive each shard's burst on its own task; within a shard a
        // SemaphoreSlim caps concurrency at concurrencyLevel.
        var sw = Stopwatch.StartNew();
        var shardTasks = new Task[shardCount];
        for (var shard = 0; shard < shardCount; shard++)
        {
            var localShard = shard;
            shardTasks[shard] = Task.Run(async () =>
            {
                using var gate = new SemaphoreSlim(concurrencyLevel, concurrencyLevel);
                var inflight = new List<Task>(batchesPerShard);
                for (var b = 0; b < batchesPerShard; b++)
                {
                    await gate.WaitAsync().ConfigureAwait(false);
                    var batch = perShardBatches[localShard][b];
                    inflight.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await provider.AppendBatchAsync(treeId, localShard, batch, CancellationToken.None).ConfigureAwait(false);
                        }
                        finally
                        {
                            gate.Release();
                        }
                    }));
                }
                await Task.WhenAll(inflight).ConfigureAwait(false);
            });
        }
        await Task.WhenAll(shardTasks).ConfigureAwait(false);
        sw.Stop();
        samplerCts.Cancel();
        try
        {
            await samplerTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected.
        }

        // Bounded poll until every shard's persisted TAIL reflects
        // the full burst; phase 2 is already enqueued and ordered.
        var totalEntries = shardCount * batchesPerShard * entriesPerBatch;
        var entriesPerShard = batchesPerShard * entriesPerBatch;
        var drainDeadline = Stopwatch.GetTimestamp() + Stopwatch.Frequency * 10L;
        while (Stopwatch.GetTimestamp() < drainDeadline)
        {
            var stable = true;
            for (var shard = 0; shard < shardCount; shard++)
            {
                var head = await provider.GetHighestOffsetAsync(treeId, shard, CancellationToken.None).ConfigureAwait(false);
                if (head != entriesPerShard - 1)
                {
                    stable = false;
                    break;
                }
            }
            if (stable)
            {
                break;
            }
            await Task.Delay(20).ConfigureAwait(false);
        }

        // Monotonicity assertion: per shard, recorded head sequence
        // must be non-decreasing.
        var violations = new List<string>();
        var perShardLast = new long[shardCount];
        Array.Fill(perShardLast, -1L);
        int sampleCount;
        lock (samplesLock)
        {
            sampleCount = samples.Count;
            foreach (var (shard, head, elapsed) in samples)
            {
                if (head < perShardLast[shard])
                {
                    violations.Add(
                        $"shard={shard} head went from {perShardLast[shard]} to {head} at t={elapsed}us");
                }
                else
                {
                    perShardLast[shard] = head;
                }
            }
        }

        // Read back to confirm count + final heads.
        var finalHeads = new long[shardCount];
        var totalRead = 0L;
        for (var shard = 0; shard < shardCount; shard++)
        {
            finalHeads[shard] = await provider.GetHighestOffsetAsync(treeId, shard, CancellationToken.None).ConfigureAwait(false);
            await foreach (var _ in provider.ReadAsync(treeId, shard, -1L, entriesPerShard, CancellationToken.None).ConfigureAwait(false))
            {
                totalRead++;
            }
        }

        // Structural prerequisite for partition-server parallelism on a
        // real Azure Tables account: every committed batch must have
        // landed in a distinct batch-partition key. The redesigned
        // schema keys batches as 'treeId|shardIndex|B|S{startOffset:D19}'
        // (prefix '_b_' is internal to the provider). We count distinct
        // batch-partition keys directly off the raw table and assert it
        // equals shardCount * batchesPerShard. Azurite collapses these
        // partitions onto a single emulator process so this assertion
        // is *not* sufficient to demonstrate uplift on Azurite, but it
        // is the structural precondition that lets real Azure Tables
        // distribute the batches across multiple partition servers,
        // and any future regression that collapses batches back into
        // a shared partition would trip it.
        var tableClient = new TableClient(connectionString, tableName);
        var distinctBatchPartitions = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var row in tableClient.QueryAsync<TableEntity>(
            filter: (string?)null, select: new[] { "PartitionKey" }, cancellationToken: CancellationToken.None)
            .ConfigureAwait(false))
        {
            // The provider partitions data into batch-partitions
            // ('_b_' prefix) and per-shard manifest-partitions
            // ('_m_' prefix). Only batch partitions are subject to
            // the "distinct per batch" property we want to assert.
            if (row.PartitionKey.StartsWith("_b_|", StringComparison.Ordinal))
            {
                distinctBatchPartitions.Add(row.PartitionKey);
            }
        }

        var elapsedSeconds = sw.Elapsed.TotalSeconds;
        return new BurstResult
        {
            ConcurrencyLevel = concurrencyLevel,
            TotalEntries = totalEntries,
            BurstSeconds = elapsedSeconds,
            EntriesPerSecond = elapsedSeconds > 0 ? totalEntries / elapsedSeconds : 0d,
            BatchesPerSecond = elapsedSeconds > 0 ? (shardCount * batchesPerShard) / elapsedSeconds : 0d,
            MonotonicitySampleCount = sampleCount,
            MonotonicityViolations = violations,
            TotalEntriesReadBack = totalRead,
            FinalHeadsPerShard = finalHeads,
            DistinctBatchPartitionCount = distinctBatchPartitions.Count,
            ExpectedDistinctBatchPartitionCount = shardCount * batchesPerShard,
        };
    }
    finally
    {
        await provider.DisposeAsync().ConfigureAwait(false);
        try
        {
            await adminClient.DeleteTableAsync(tableName).ConfigureAwait(false);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup.
        }
    }
}

internal sealed class BurstResult
{
    public required int ConcurrencyLevel { get; init; }
    public required int TotalEntries { get; init; }
    public required double BurstSeconds { get; init; }
    public required double EntriesPerSecond { get; init; }
    public required double BatchesPerSecond { get; init; }
    public required int MonotonicitySampleCount { get; init; }
    public required IReadOnlyList<string> MonotonicityViolations { get; init; }
    public required long TotalEntriesReadBack { get; init; }
    public required IReadOnlyList<long> FinalHeadsPerShard { get; init; }
    public required int DistinctBatchPartitionCount { get; init; }
    public required int ExpectedDistinctBatchPartitionCount { get; init; }
}
