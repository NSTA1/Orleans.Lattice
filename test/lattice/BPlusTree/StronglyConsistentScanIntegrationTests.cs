using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// F-011 integration tests: <see cref="ILattice.CountAsync"/>,
/// <see cref="ILattice.KeysAsync"/>, and <see cref="ILattice.EntriesAsync"/>
/// must produce strongly-consistent results — exact key set, exact count —
/// even when adaptive shard splits are happening concurrently.
/// </summary>
[TestFixture]
public class StronglyConsistentScanIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

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

    private async Task<Dictionary<string, byte[]>> SeedAsync(string treeId, int keyCount)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = new Dictionary<string, byte[]>(keyCount);
        for (int i = 0; i < keyCount; i++)
        {
            var key = $"sck-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"v-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }
        return expected;
    }

    [Test]
    public async Task CountAsync_returns_exact_count_after_split_completes()
    {
        var treeId = $"sc-count-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 300);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsCompleteAsync(), Is.True);

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(expected.Count),
            "CountAsync must return the exact number of seeded keys after split.");
    }

    [Test]
    public async Task KeysAsync_returns_exact_set_after_split_completes()
    {
        var treeId = $"sc-keys-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 300);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();

        var actual = new HashSet<string>();
        await foreach (var k in tree.KeysAsync()) actual.Add(k);

        Assert.That(actual, Is.EquivalentTo(expected.Keys));
    }

    [Test]
    public async Task EntriesAsync_returns_exact_set_after_split_completes()
    {
        var treeId = $"sc-entries-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 300);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();

        var actual = new Dictionary<string, byte[]>();
        await foreach (var kv in tree.EntriesAsync()) actual[kv.Key] = kv.Value;

        Assert.That(actual.Keys, Is.EquivalentTo(expected.Keys));
        foreach (var (k, v) in expected)
            Assert.That(actual[k].AsSpan().SequenceEqual(v), Is.True, $"value mismatch for {k}");
    }

    /// <summary>
    /// Continuously scan via CountAsync / KeysAsync / EntriesAsync while a
    /// split runs concurrently. Every scan that completes must observe the
    /// exact seeded set — not under-count, not over-count, no duplicates.
    /// </summary>
    [Test]
    public async Task Concurrent_scans_during_split_observe_exact_seeded_set()
    {
        var treeId = $"sc-concurrent-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 400);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var failures = new ConcurrentBag<string>();
        var scansCompleted = 0;
        using var cts = new CancellationTokenSource();

        var scanWorkers = new[]
        {
            Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var c = await tree.CountAsync();
                        if (c != expected.Count)
                            failures.Add($"CountAsync returned {c}, expected {expected.Count}");
                        Interlocked.Increment(ref scansCompleted);
                    }
                    catch (Exception) when (cts.IsCancellationRequested) { }
                    catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { /* transient stream cursor deactivation; retry */ }
                    catch (Exception ex)
                    {
                        failures.Add($"CountAsync threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }),
            Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested
)
                {
                    try
                    {
                        var seen = new HashSet<string>();
                        await foreach (var k in tree.KeysAsync())
                        {
                            if (cts.IsCancellationRequested) break;
                            if (!seen.Add(k))
                                failures.Add($"KeysAsync yielded duplicate '{k}'");
                            if (!expected.ContainsKey(k))
                                failures.Add($"KeysAsync yielded unknown key '{k}'");
                        }
                        if (!cts.IsCancellationRequested)
                        {
                            if (seen.Count != expected.Count)
                                failures.Add($"KeysAsync yielded {seen.Count} keys, expected {expected.Count}");
                            Interlocked.Increment(ref scansCompleted);
                        }
                    }
                    catch (Exception) when (cts.IsCancellationRequested) { }
                    catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { /* transient stream cursor deactivation; retry */ }
                    catch (Exception ex)
                    {
                        failures.Add($"KeysAsync threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }),
            Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var seen = new HashSet<string>();
                        await foreach (var kv in tree.EntriesAsync())
                        {
                            if (cts.IsCancellationRequested) break;
                            if (!seen.Add(kv.Key))
                                failures.Add($"EntriesAsync yielded duplicate '{kv.Key}'");
                            if (!expected.TryGetValue(kv.Key, out var want))
                                failures.Add($"EntriesAsync yielded unknown key '{kv.Key}'");
                            else if (!kv.Value.AsSpan().SequenceEqual(want))
                                failures.Add($"EntriesAsync wrong value for '{kv.Key}'");
                        }
                        if (!cts.IsCancellationRequested)
                        {
                            if (seen.Count != expected.Count)
                                failures.Add($"EntriesAsync yielded {seen.Count} entries, expected {expected.Count}");
                            Interlocked.Increment(ref scansCompleted);
                        }
                    }
                    catch (Exception) when (cts.IsCancellationRequested) { }
                    catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { /* transient stream cursor deactivation; retry */ }
                    catch (Exception ex)
                    {
                        failures.Add($"EntriesAsync threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }),
        };

        // Let scanners warm up so a scan is in-flight when the split begins.
        await Task.Delay(150);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsCompleteAsync(), Is.True);

        // Allow scanners to also exercise the post-swap path.
        await Task.Delay(300);

        cts.Cancel();
        await Task.WhenAll(scanWorkers);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Strongly-consistent scans observed {failures.Count} failures during split:\n  {string.Join("\n  ", failures.Take(20))}");
            Assert.That(scansCompleted, Is.GreaterThan(0),
                "At least one scan must have completed during the test window.");
        });
    }

    /// <summary>
    /// Drive multiple concurrent splits (against different source shards)
    /// while continuously scanning. Verifies the per-slot reconciliation
    /// model handles N concurrent topology changes, not just one.
    /// </summary>
    [Test]
    public async Task Concurrent_scans_during_multiple_parallel_splits_observe_exact_set()
    {
        var treeId = $"sc-multi-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 500);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var failures = new ConcurrentBag<string>();
        var scansCompleted = 0;
        using var cts = new CancellationTokenSource();

        var scanWorkers = Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var seen = new HashSet<string>();
                    await foreach (var k in tree.KeysAsync())
                    {
                        if (cts.IsCancellationRequested) break;
                        if (!seen.Add(k)) failures.Add($"duplicate '{k}'");
                        if (!expected.ContainsKey(k)) failures.Add($"unknown '{k}'");
                    }
                    if (!cts.IsCancellationRequested)
                    {
                        if (seen.Count != expected.Count)
                            failures.Add($"under/over-count: got {seen.Count}, want {expected.Count}");
                        Interlocked.Increment(ref scansCompleted);
                    }
                }
                catch (Exception) when (cts.IsCancellationRequested) { }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { /* transient stream cursor deactivation; retry */ }
                catch (Exception ex)
                {
                    failures.Add($"KeysAsync threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        })).ToArray();

        await Task.Delay(150);

        // Drive splits of shards 0 and 1 in parallel.
        var split0 = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        var split1 = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/1");
        await Task.WhenAll(split0.SplitAsync(0), split1.SplitAsync(1));
        await Task.WhenAll(split0.RunSplitPassAsync(), split1.RunSplitPassAsync());
        Assert.That(await split0.IsCompleteAsync(), Is.True);
        Assert.That(await split1.IsCompleteAsync(), Is.True);

        await Task.Delay(300);

        cts.Cancel();
        await Task.WhenAll(scanWorkers);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Strongly-consistent scans observed {failures.Count} failures during parallel splits:\n  {string.Join("\n  ", failures.Take(20))}");
            Assert.That(scansCompleted, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task CountAsync_returns_exact_count_during_active_split()
    {
        var treeId = $"sc-count-mid-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 200);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var failures = new ConcurrentBag<string>();
        var iterations = 0;
        using var cts = new CancellationTokenSource();

        var counter = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var c = await tree.CountAsync();
                    if (c != expected.Count)
                        failures.Add($"CountAsync={c}, expected {expected.Count}");
                    Interlocked.Increment(ref iterations);
                }
                catch (Exception) when (cts.IsCancellationRequested) { }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { /* transient stream cursor deactivation; retry */ }
                catch (Exception ex)
                {
                    failures.Add($"threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        });

        await Task.Delay(100);
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();
        await Task.Delay(150);
        cts.Cancel();
        await counter;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty, $"Mid-split count failures:\n  {string.Join("\n  ", failures.Take(10))}");
            Assert.That(iterations, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task KeysAsync_reverse_returns_exact_set_after_split()
    {
        var treeId = $"sc-keys-rev-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 200);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var actual = new HashSet<string>();
        await foreach (var k in tree.KeysAsync(reverse: true)) actual.Add(k);

        Assert.That(actual, Is.EquivalentTo(expected.Keys));
    }

    [Test]
    public async Task KeysAsync_with_range_returns_exact_subset_after_split()
    {
        var treeId = $"sc-keys-range-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 300);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var start = "sck-00100";
        var end = "sck-00200";
        var expectedSubset = expected.Keys
            .Where(k => string.CompareOrdinal(k, start) >= 0 && string.CompareOrdinal(k, end) < 0)
            .ToHashSet();

        var actual = new HashSet<string>();
        await foreach (var k in tree.KeysAsync(start, end)) actual.Add(k);

        Assert.That(actual, Is.EquivalentTo(expectedSubset));
    }
}

