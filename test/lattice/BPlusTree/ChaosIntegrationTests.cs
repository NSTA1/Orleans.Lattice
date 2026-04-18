using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// F-011 chaos stress test: runs a dense concurrent workload of point
/// reads, point writes, bulk reads, bulk writes, scans, and counts against
/// a tree while manually-triggered shard splits mutate the physical
/// topology. Verifies that Lattice's public API upholds its consistency
/// guarantees under arbitrary interleavings:
///
/// <list type="bullet">
///   <item><description><c>CountAsync</c> returns the exact size of the
///     pinned key universe regardless of concurrent splits and value
///     updates.</description></item>
///   <item><description><c>KeysAsync</c> never yields duplicates, never
///     yields unknown keys, and yields exactly the full universe when
///     the scan completes.</description></item>
///   <item><description><c>EntriesAsync</c> only yields values that were
///     actually written at some point (envelope check on the value).</description></item>
///   <item><description><c>GetManyAsync</c> results are drawn from the
///     known universe with well-formed values.</description></item>
///   <item><description>No operation on the public API throws an unhandled
///     exception. Stale routing is transparently retried by the framework;
///     stream-cursor deactivations are transient and tolerated.</description></item>
/// </list>
///
/// The universe is fixed during the chaos window so the exact-count
/// invariant is well-defined. Writers only rewrite existing keys with
/// monotonically-increasing values of the form
/// <c>v-{keyIndex}-{writerId}-{seq}</c>; any value matching that envelope
/// proves the byte array is internally consistent.
/// </summary>
[TestFixture]
[NonParallelizable]
public class ChaosIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int UniverseSize = 500;
    private const int WriterCount = 4;
    private const int BulkWriterCount = 2;
    private const int ReaderCount = 3;
    private const int BulkReaderCount = 2;
    private const int ScannerCount = 2;
    private const int CounterCount = 2;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(4);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(400);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    /// <summary>Builds the deterministic universe key for index <paramref name="i"/>.</summary>
    private static string KeyOf(int i) => $"chaos-{i:D5}";

    /// <summary>Writes the initial value for each universe key.</summary>
    private static async Task SeedAsync(ILattice tree)
    {
        for (int i = 0; i < UniverseSize; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));
    }

    /// <summary>
    /// Validates that <paramref name="value"/> matches the <c>v-{i}-*</c>
    /// envelope for universe key <paramref name="expectedIndex"/>.
    /// </summary>
    private static bool IsValidValueFor(int expectedIndex, byte[] value)
    {
        if (value is null || value.Length == 0) return false;
        var s = Encoding.UTF8.GetString(value);
        return s.StartsWith($"v-{expectedIndex}-", StringComparison.Ordinal);
    }

    /// <summary>Parses the universe index from a chaos key; -1 on miss.</summary>
    private static int IndexOfKey(string key)
        => key.StartsWith("chaos-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(6), out var idx)
            ? idx
            : -1;

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException"
            or "StaleShardRoutingException";

    [Test]
    public async Task Chaos_concurrent_reads_writes_scans_counts_and_splits_preserve_invariants()
    {
        var treeId = $"chaos-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree);

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k)
            => s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers: rewrite random universe keys with monotonic values.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 1);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var value = Encoding.UTF8.GetBytes($"v-{idx}-{writerId}-{++seq}");
                        await tree.SetAsync(KeyOf(idx), value);
                        Bump(stats, "point-writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-writes"); }
                    catch (Exception ex)
                    {
                        failures.Add($"writer{writerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Bulk writers: SetManyAsync with batches of 8 random keys.
        for (int w = 0; w < BulkWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 104729 + 3);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var batch = new List<KeyValuePair<string, byte[]>>(8);
                        var seen = new HashSet<int>();
                        while (batch.Count < 8)
                        {
                            var idx = rng.Next(UniverseSize);
                            if (!seen.Add(idx)) continue;
                            batch.Add(new(KeyOf(idx),
                                Encoding.UTF8.GetBytes($"v-{idx}-bulk{writerId}-{++seq}")));
                        }
                        await tree.SetManyAsync(batch);
                        Bump(stats, "bulk-writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-writes"); }
                    catch (Exception ex)
                    {
                        failures.Add($"bulk-writer{writerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Point readers: GetAsync a random key and validate the envelope.
        for (int r = 0; r < ReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 48611 + 5);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var v = await tree.GetAsync(KeyOf(idx));
                        if (v is null)
                            failures.Add($"reader{readerId}: GetAsync returned null for universe key {idx}");
                        else if (!IsValidValueFor(idx, v))
                            failures.Add($"reader{readerId}: GetAsync returned invalid value for key {idx}: " +
                                Encoding.UTF8.GetString(v));
                        Bump(stats, "point-reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-reads"); }
                    catch (Exception ex)
                    {
                        failures.Add($"reader{readerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Bulk readers: GetManyAsync for 16 random keys.
        for (int r = 0; r < BulkReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 15485863 + 7);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var keys = new List<string>(16);
                        for (int i = 0; i < 16; i++)
                            keys.Add(KeyOf(rng.Next(UniverseSize)));
                        var results = await tree.GetManyAsync(keys);
                        foreach (var kv in results)
                        {
                            var idx = IndexOfKey(kv.Key);
                            if (idx < 0)
                                failures.Add($"bulk-reader{readerId}: GetManyAsync yielded unknown key '{kv.Key}'");
                            else if (!IsValidValueFor(idx, kv.Value))
                                failures.Add($"bulk-reader{readerId}: GetManyAsync invalid value for key {idx}: " +
                                    Encoding.UTF8.GetString(kv.Value));
                        }
                        Bump(stats, "bulk-reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-reads"); }
                    catch (Exception ex)
                    {
                        failures.Add($"bulk-reader{readerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Scanners: alternate Keys / Entries / reverse / range scans.
        for (int s = 0; s < ScannerCount; s++)
        {
            var scannerId = s;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(scannerId * 2038074743 + 11);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var mode = rng.Next(4);
                        if (mode == 0)
                        {
                            var seen = new HashSet<string>();
                            await foreach (var k in tree.KeysAsync())
                            {
                                if (ct.IsCancellationRequested) break;
                                if (!seen.Add(k))
                                    failures.Add($"scanner{scannerId}: KeysAsync duplicate '{k}'");
                                if (IndexOfKey(k) < 0)
                                    failures.Add($"scanner{scannerId}: KeysAsync unknown '{k}'");
                            }
                            if (!ct.IsCancellationRequested && seen.Count != UniverseSize)
                                failures.Add($"scanner{scannerId}: KeysAsync yielded {seen.Count}, expected {UniverseSize}");
                            Bump(stats, "keys-scans");
                        }
                        else if (mode == 1)
                        {
                            var seen = new HashSet<string>();
                            await foreach (var kv in tree.EntriesAsync())
                            {
                                if (ct.IsCancellationRequested) break;
                                if (!seen.Add(kv.Key))
                                    failures.Add($"scanner{scannerId}: EntriesAsync duplicate '{kv.Key}'");
                                var idx = IndexOfKey(kv.Key);
                                if (idx < 0)
                                    failures.Add($"scanner{scannerId}: EntriesAsync unknown '{kv.Key}'");
                                else if (!IsValidValueFor(idx, kv.Value))
                                    failures.Add($"scanner{scannerId}: EntriesAsync bad value for '{kv.Key}'");
                            }
                            if (!ct.IsCancellationRequested && seen.Count != UniverseSize)
                                failures.Add($"scanner{scannerId}: EntriesAsync yielded {seen.Count}, expected {UniverseSize}");
                            Bump(stats, "entries-scans");
                        }
                        else if (mode == 2)
                        {
                            var seen = new HashSet<string>();
                            await foreach (var k in tree.KeysAsync(null, null, reverse: true))
                            {
                                if (ct.IsCancellationRequested) break;
                                if (!seen.Add(k))
                                    failures.Add($"scanner{scannerId}: KeysAsync(reverse) duplicate '{k}'");
                            }
                            if (!ct.IsCancellationRequested && seen.Count != UniverseSize)
                                failures.Add($"scanner{scannerId}: KeysAsync(reverse) yielded {seen.Count}, expected {UniverseSize}");
                            Bump(stats, "keys-scans-reverse");
                        }
                        else
                        {
                            var start = KeyOf(100);
                            var end = KeyOf(400);
                            const int expectedInRange = 300;
                            var seen = new HashSet<string>();
                            await foreach (var k in tree.KeysAsync(start, end))
                            {
                                if (ct.IsCancellationRequested) break;
                                if (!seen.Add(k))
                                    failures.Add($"scanner{scannerId}: KeysAsync(range) duplicate '{k}'");
                                var idx = IndexOfKey(k);
                                if (idx < 100 || idx >= 400)
                                    failures.Add($"scanner{scannerId}: KeysAsync(range) out-of-range '{k}'");
                            }
                            if (!ct.IsCancellationRequested && seen.Count != expectedInRange)
                                failures.Add($"scanner{scannerId}: KeysAsync(range) yielded {seen.Count}, expected {expectedInRange}");
                            Bump(stats, "keys-scans-range");
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-scans"); }
                    catch (Exception ex)
                    {
                        failures.Add($"scanner{scannerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Counters: CountAsync must always equal the pinned universe size.
        for (int c = 0; c < CounterCount; c++)
        {
            var counterId = c;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var count = await tree.CountAsync();
                        if (count != UniverseSize)
                            failures.Add($"counter{counterId}: CountAsync={count}, expected {UniverseSize}");
                        Bump(stats, "counts");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-counts"); }
                    catch (Exception ex)
                    {
                        failures.Add($"counter{counterId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Split coordinator: fires manual splits periodically to churn
        // the topology. Splits a rotating non-empty source shard each tick.
        workers.Add(Task.Run(async () =>
        {
            var rng = new Random(42);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(SplitInterval, ct);
                    var physical = await tree.CountPerShardAsync();
                    var candidates = Enumerable.Range(0, physical.Count)
                        .Where(i => physical[i] > 0).ToList();
                    if (candidates.Count == 0) continue;
                    var src = candidates[rng.Next(candidates.Count)];
                    var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{src}");
                    await split.SplitAsync(src);
                    await split.RunSplitPassAsync();
                    Bump(stats, "splits");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { }
                catch (Exception ex)
                {
                    failures.Add($"split-coordinator threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        await Task.WhenAll(workers);

        // After the chaos window closes, the universe must still be intact.
        var finalCount = await tree.CountAsync();
        var finalKeys = new HashSet<string>();
        await foreach (var k in tree.KeysAsync()) finalKeys.Add(k);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n  " +
                string.Join("\n  ", failures.Take(20)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-chaos CountAsync must match the pinned universe size.");
            Assert.That(finalKeys.Count, Is.EqualTo(UniverseSize),
                "Post-chaos KeysAsync must yield exactly the pinned universe.");

            foreach (var op in new[] { "point-writes", "bulk-writes", "point-reads",
                "bulk-reads", "keys-scans", "entries-scans", "counts", "splits" })
            {
                Assert.That(stats.GetValueOrDefault(op, 0), Is.GreaterThan(0),
                    $"Chaos workload category '{op}' must have performed at least one operation.");
            }
        });

        TestContext.Out.WriteLine("Chaos workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($"  {kv.Key,-22}{kv.Value}");
    }
}
