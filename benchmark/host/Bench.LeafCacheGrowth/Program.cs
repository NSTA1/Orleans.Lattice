// Bench.LeafCacheGrowth - unbounded LeafCacheGrain footprint probe (issue #387).
//
// Purpose
// -------
// The LeafCacheGrain._cache dictionary mirrors its primary leaf's live entry
// set 1:1 and grows monotonically over the activation's lifetime. Issue #387 asks
// whether that per-silo per-tree footprint should be bounded, and gates any
// eviction policy on preserving four correctness contracts. The investigation
// concluded that only a value-payload-only LRU (metadata retained) can clear
// the gate, at the cost of a per-key leaf RPC on the evicted fraction. Whether
// that tradeoff is worthwhile is an empirical question - this probe supplies
// the baseline (unbounded) side of that measurement so the ship/close decision
// rests on numbers rather than intuition.
//
// What it measures
// ----------------
// For each (entry_count x value_bytes) point it activates a REAL BPlusLeafGrain
// (primary leaf) seeded with entry_count entries of value_bytes each, activates
// a REAL LeafCacheGrain in front of it, warms the cache to a full mirror, then
// runs a uniform-random read workload for a fixed duration. During the workload
// it samples at 1-second cadence:
//   - Process.WorkingSet64 and GC.GetTotalMemory(forceFullCollection: false);
//   - the cache mirror's own EntryCount and summed value-payload bytes, read
//     through the LeafCacheGrain.DebugFootprint diagnostic seam;
// and records per-read latency (p50 / p99 via reservoir sampling).
//
// Honest attribution note
// -----------------------
// The probe runs the leaf and the cache in ONE process with no Orleans
// serialization boundary, so the cache's byte[] payloads ALIAS the leaf's
// source projection - Process.WorkingSet64 therefore reflects one copy of the
// payloads plus two envelope dictionaries, NOT the doubled footprint a real
// two-silo deployment pays. The authoritative, aliasing-independent figure for
// "how much the cache mirror holds" is cache_value_bytes from the DebugFootprint
// seam: in production that many payload bytes are resident on EVERY silo that
// activates a cache in front of the leaf. Treat cache_value_bytes as the issue #387
// headline; treat working-set / GC-total as a whole-process context signal.
//
// Configuration (env vars; issue defaults)
//   BENCH_LEAFCACHE_ENTRY_COUNTS      comma list, default "1000,10000,100000"
//   BENCH_LEAFCACHE_VALUE_BYTES       comma list, default "64,1024,65536"
//   BENCH_LEAFCACHE_DURATION_SECONDS  per-cell read duration, default 10
//   BENCH_LEAFCACHE_SEED_BATCH        entries per SetManyAsync, default 5000
//   BENCH_RESULTS_PATH                if set, the JSON report is written here
//
// WARNING: the full issue matrix includes 100000 x 65536 = ~6.4 GB of resident
// cache payload for that single cell. Size the host accordingly or narrow the
// matrix via the env vars above.

using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Benchmark.LeafCacheGrowth;
using Orleans.Runtime;

const string TreeId = "leafcache-growth-probe";

var entryCounts = ParseIntList(
    Environment.GetEnvironmentVariable("BENCH_LEAFCACHE_ENTRY_COUNTS"),
    new[] { 1_000, 10_000, 100_000 });
var valueSizes = ParseIntList(
    Environment.GetEnvironmentVariable("BENCH_LEAFCACHE_VALUE_BYTES"),
    new[] { 64, 1_024, 65_536 });
var durationSeconds = ReadIntEnv("BENCH_LEAFCACHE_DURATION_SECONDS", 10);
var seedBatch = ReadIntEnv("BENCH_LEAFCACHE_SEED_BATCH", 5_000);

Console.WriteLine(
    $"[bench-leafcache-growth] matrix: entry_counts=[{string.Join(",", entryCounts)}] "
    + $"value_bytes=[{string.Join(",", valueSizes)}] duration={durationSeconds}s/cell");

var cells = new List<CellResult>(entryCounts.Length * valueSizes.Length);
foreach (var entryCount in entryCounts)
{
    foreach (var valueBytes in valueSizes)
    {
        Console.WriteLine(
            $"[bench-leafcache-growth] running cell entry_count={entryCount} value_bytes={valueBytes} ...");
        var cell = await RunCellAsync(TreeId, entryCount, valueBytes, durationSeconds, seedBatch)
            .ConfigureAwait(false);
        cells.Add(cell);
        Console.WriteLine(
            $"  -> cache_entries={cell.CacheEntryCount} cache_value_bytes={cell.CacheValueBytes:N0} "
            + $"({FormatBytes(cell.CacheValueBytes)}) reads={cell.ReadCount:N0} "
            + $"p50={cell.ReadP50Micros:F2}us p99={cell.ReadP99Micros:F2}us");

        // Drop references and reclaim between cells so the next cell's
        // working-set baseline is not inflated by the previous cell's retained
        // mirror. Cells are independent measurements.
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    }
}

// Correctness gate: every cell must have mirrored the full entry set into the
// cache (EntryCount == seeded count) and summed the expected payload bytes.
// A mismatch means the delta full-snapshot delivery did not populate the
// mirror as expected, which would invalidate the footprint numbers.
var anyMirrorMismatch = cells.Any(c => c.CacheEntryCount != c.EntryCount
    || c.CacheValueBytes != (long)c.EntryCount * c.ValueBytes);

Console.WriteLine();
Console.WriteLine("[bench-leafcache-growth] baseline (unbounded cache) footprint sweep:");
Console.WriteLine("  entries | value_B | cache_entries | cache_value_bytes | ws_delta_bytes | gc_total_peak | p50_us | p99_us");
Console.WriteLine("  --------+---------+---------------+-------------------+----------------+---------------+--------+-------");
foreach (var c in cells)
{
    Console.WriteLine(string.Format(
        CultureInfo.InvariantCulture,
        "  {0,7} | {1,7} | {2,13} | {3,17} | {4,14} | {5,13} | {6,6:F2} | {7,6:F2}",
        c.EntryCount,
        c.ValueBytes,
        c.CacheEntryCount,
        c.CacheValueBytes,
        c.WorkingSetDeltaBytes,
        c.GcTotalPeakBytes,
        c.ReadP50Micros,
        c.ReadP99Micros));
}

Console.WriteLine();
Console.WriteLine("[bench-leafcache-growth] interpretation:");
Console.WriteLine("  - cache_value_bytes is the authoritative, aliasing-independent size of the cache");
Console.WriteLine("    mirror's value payloads (via the DebugFootprint seam). In production that many");
Console.WriteLine("    bytes are resident on EVERY silo that fronts the leaf with a cache activation -");
Console.WriteLine("    this is the per-silo footprint the eviction investigation targets.");
Console.WriteLine("  - ws_delta_bytes / gc_total_peak are whole-process figures. Because the probe runs");
Console.WriteLine("    the leaf and cache in one process, payloads are aliased, so these UNDERSTATE the");
Console.WriteLine("    doubled footprint a real two-silo deployment pays. Use cache_value_bytes for the");
Console.WriteLine("    per-silo cost and estimate the deployed cost as (silos_fronting_leaf x that value).");
Console.WriteLine("  - p50_us / p99_us are steady-state cache-hit read latencies (CacheTtl suppresses the");
Console.WriteLine("    leaf refresh after warmup), i.e. the baseline the value-payload-only LRU candidate");
Console.WriteLine("    would regress on the evicted fraction by taking the per-key leaf-delegation path.");

var report = new
{
    scenario = "leafcache-growth-baseline-unbounded",
    success = !anyMirrorMismatch,
    duration_seconds_per_cell = durationSeconds,
    cells = cells.Select(c => new
    {
        entry_count = c.EntryCount,
        value_bytes = c.ValueBytes,
        cache_entry_count = c.CacheEntryCount,
        cache_value_bytes = c.CacheValueBytes,
        expected_value_bytes = (long)c.EntryCount * c.ValueBytes,
        working_set_baseline_bytes = c.WorkingSetBaselineBytes,
        working_set_peak_bytes = c.WorkingSetPeakBytes,
        working_set_delta_bytes = c.WorkingSetDeltaBytes,
        gc_total_baseline_bytes = c.GcTotalBaselineBytes,
        gc_total_peak_bytes = c.GcTotalPeakBytes,
        read_count = c.ReadCount,
        read_p50_micros = Math.Round(c.ReadP50Micros, 3),
        read_p99_micros = Math.Round(c.ReadP99Micros, 3),
        samples = c.Samples.Select(s => new
        {
            elapsed_ms = s.ElapsedMs,
            working_set_bytes = s.WorkingSetBytes,
            gc_total_bytes = s.GcTotalBytes,
            cache_entry_count = s.CacheEntryCount,
            cache_value_bytes = s.CacheValueBytes,
        }).ToArray(),
    }).ToArray(),
};

var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
var resultsPath = Environment.GetEnvironmentVariable("BENCH_RESULTS_PATH");
if (!string.IsNullOrWhiteSpace(resultsPath))
{
    var dir = Path.GetDirectoryName(resultsPath);
    if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
    await File.WriteAllTextAsync(resultsPath, json).ConfigureAwait(false);
    Console.WriteLine($"[bench-leafcache-growth] report written to {resultsPath}");
}
else
{
    Console.WriteLine();
    Console.WriteLine(json);
}

if (anyMirrorMismatch)
{
    Console.Error.WriteLine(
        "[bench-leafcache-growth] cache mirror mismatch: a cell did not mirror the full seeded "
        + "entry set (cache_entry_count != entry_count or cache_value_bytes != expected). The "
        + "footprint numbers for that cell are not trustworthy.");
    return 3;
}

return 0;

// ---------------------------------------------------------------------------

static async Task<CellResult> RunCellAsync(
    string treeId,
    int entryCount,
    int valueBytes,
    int durationSeconds,
    int seedBatch)
{
    var leafGuid = Guid.NewGuid();
    var leafGrainId = GrainId.Create("leaf", leafGuid.ToString("N"));

    // Options: a large CacheTtl so that, after the warmup refresh, steady-state
    // reads short-circuit the leaf entirely (RefreshAsync's TTL gate) and the
    // measured read latency reflects only the cache's own hit path. MaxLeafKeys
    // is resolved from the registry stub below, not from options.
    var options = new LatticeOptions { CacheTtl = TimeSpan.FromHours(1) };
    var optionsMonitor = new FixedOptionsMonitor(options);

    // Registry stub pins a MaxLeafKeys comfortably above the seeded count so the
    // primary leaf never splits during seeding (a split would fan out into
    // sibling leaf grains this probe deliberately does not wire).
    var registry = Substitute.For<ILatticeRegistry>();
    registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
        new TreeRegistryEntry
        {
            MaxLeafKeys = (entryCount * 2) + 1_000,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = 1,
        }));

    IBPlusLeafGrain? leaf = null;
    var grainFactory = Substitute.For<IGrainFactory>();
    grainFactory.GetGrain<ILatticeRegistry>(Arg.Any<string>()).Returns(registry);
    grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(_ => leaf!);
    grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(_ => leaf!);
    // Auxiliary routes the leaf write path may touch; NSubstitute auto-mocks
    // every Task-returning member to a completed default, which is the no-op
    // behaviour the probe wants (no compaction, no hot-shard reporting).
    grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>())
        .Returns(Substitute.For<ITombstoneCompactionGrain>());
    grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>())
        .Returns(Substitute.For<IShardRootGrain>());

    var observers = new MutationObserverDispatcher(
        Array.Empty<IMutationObserver>(),
        NullLogger<MutationObserverDispatcher>.Instance);
    var optionsResolver = new LatticeOptionsResolver(grainFactory, optionsMonitor);
    var originResolver = new DefaultLatticeOriginClusterIdResolver();

    var leafContext = Substitute.For<IGrainContext>();
    leafContext.GrainId.Returns(leafGrainId);
    var leafState = new FakePersistentState<LeafNodeState>();
    var leafGrain = new BPlusLeafGrain(
        leafContext, leafState, grainFactory, optionsResolver, observers, originResolver);
    leaf = leafGrain;

    await leafGrain.SetTreeIdAsync(treeId).ConfigureAwait(false);

    // Seed the leaf with entry_count entries of value_bytes each. Distinct
    // payload buffers per key so the aggregate byte cost is real (not a single
    // shared buffer that would collapse to one allocation).
    var keys = new string[entryCount];
    for (var i = 0; i < entryCount; i++)
    {
        keys[i] = "k-" + i.ToString("D9", CultureInfo.InvariantCulture);
    }

    var batch = new List<KeyValuePair<string, byte[]>>(Math.Min(seedBatch, entryCount));
    for (var i = 0; i < entryCount; i++)
    {
        var payload = new byte[valueBytes];
        // A cheap non-constant fill so the buffer is materially resident and
        // the JIT / GC cannot fold it away.
        payload[0] = (byte)(i & 0xFF);
        if (valueBytes > 1) payload[valueBytes - 1] = (byte)((i >> 8) & 0xFF);
        batch.Add(new KeyValuePair<string, byte[]>(keys[i], payload));
        if (batch.Count >= seedBatch)
        {
            await leafGrain.SetManyAsync(batch).ConfigureAwait(false);
            batch.Clear();
        }
    }
    if (batch.Count > 0)
    {
        await leafGrain.SetManyAsync(batch).ConfigureAwait(false);
    }

    // Build the cache in front of the leaf. Its context key must be the leaf's
    // GrainId string so LeafCacheGrain.PrimaryLeafId parses back to the leaf.
    var cacheContext = Substitute.For<IGrainContext>();
    cacheContext.GrainId.Returns(GrainId.Create("leafcache", leafGrainId.ToString()));
    var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, originResolver);

    // Settle memory and take the pre-warmup baseline: the leaf's source
    // projection is resident, the cache mirror is not yet.
    GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    var wsBaseline = CurrentWorkingSet();
    var gcBaseline = GC.GetTotalMemory(forceFullCollection: false);

    // Warm the cache: the first read trips the epoch-mismatch full-snapshot
    // delivery, populating the mirror with every live entry.
    _ = await cache.GetAsync(keys[0]).ConfigureAwait(false);

    var warmFootprint = cache.DebugFootprint();

    // Uniform-random read workload. Latencies are reservoir-sampled to bound
    // memory; memory + footprint are sampled at 1-second cadence inline (the
    // grain is single-threaded, so no separate sampler thread).
    var rng = new Random(0x5EED ^ (entryCount * 31) ^ valueBytes);
    var reservoir = new LatencyReservoir(capacity: 200_000, seed: 12345);
    var samples = new List<MemorySample>();
    var wsPeak = wsBaseline;
    var gcPeak = gcBaseline;

    var runSw = Stopwatch.StartNew();
    var runDuration = TimeSpan.FromSeconds(durationSeconds);
    var opSw = new Stopwatch();
    long nextSampleMs = 0;
    long readCount = 0;

    while (runSw.Elapsed < runDuration)
    {
        var key = keys[rng.Next(entryCount)];
        opSw.Restart();
        _ = await cache.GetAsync(key).ConfigureAwait(false);
        opSw.Stop();
        reservoir.Add(opSw.ElapsedTicks);
        readCount++;

        var elapsedMs = runSw.ElapsedMilliseconds;
        if (elapsedMs >= nextSampleMs)
        {
            var ws = CurrentWorkingSet();
            var gcTotal = GC.GetTotalMemory(forceFullCollection: false);
            if (ws > wsPeak) wsPeak = ws;
            if (gcTotal > gcPeak) gcPeak = gcTotal;
            var fp = cache.DebugFootprint();
            samples.Add(new MemorySample(elapsedMs, ws, gcTotal, fp.EntryCount, fp.ValueBytes));
            nextSampleMs += 1_000;
        }
    }
    runSw.Stop();

    var ticksPerMicro = Stopwatch.Frequency / 1_000_000.0;
    var p50Ticks = reservoir.Percentile(0.50);
    var p99Ticks = reservoir.Percentile(0.99);

    return new CellResult
    {
        EntryCount = entryCount,
        ValueBytes = valueBytes,
        CacheEntryCount = warmFootprint.EntryCount,
        CacheValueBytes = warmFootprint.ValueBytes,
        WorkingSetBaselineBytes = wsBaseline,
        WorkingSetPeakBytes = wsPeak,
        WorkingSetDeltaBytes = Math.Max(0, wsPeak - wsBaseline),
        GcTotalBaselineBytes = gcBaseline,
        GcTotalPeakBytes = gcPeak,
        ReadCount = readCount,
        ReadP50Micros = p50Ticks / ticksPerMicro,
        ReadP99Micros = p99Ticks / ticksPerMicro,
        Samples = samples,
    };
}

static long CurrentWorkingSet()
{
    using var proc = Process.GetCurrentProcess();
    return proc.WorkingSet64;
}

static int[] ParseIntList(string? raw, int[] fallback)
{
    if (string.IsNullOrWhiteSpace(raw)) return fallback;
    var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    var result = new List<int>(parts.Length);
    foreach (var part in parts)
    {
        if (int.TryParse(part, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > 0)
            result.Add(v);
    }
    return result.Count > 0 ? result.ToArray() : fallback;
}

static int ReadIntEnv(string name, int fallback)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > 0
        ? v
        : fallback;
}

static string FormatBytes(long bytes)
{
    string[] units = { "B", "KB", "MB", "GB", "TB" };
    double value = bytes;
    var unit = 0;
    while (value >= 1024 && unit < units.Length - 1)
    {
        value /= 1024;
        unit++;
    }
    return $"{value:F2} {units[unit]}";
}
