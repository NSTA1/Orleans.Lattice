using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// multi-page / range / prefetch ordering tests. The default
/// <see cref="FourShardClusterFixture"/> uses <c>KeysPageSize=512</c>,
/// which means a 400-key seed set fits in a single page per shard — the
/// in-line reconciliation path never fires on a non-first page. This
/// fixture forces <c>KeysPageSize=16</c> so moved-slot reports arrive
/// mid-scan, exercising the real reconciliation code path.
/// </summary>
[TestFixture]
public class MultiPageScanOrderingIntegrationTests
{
    private MultiPageFourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MultiPageFourShardClusterFixture();
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
            var key = $"mp-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"v-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }
        return expected;
    }

    // Runs a scanner in a tight loop; returns the outcome of at least one
    // completed scan that overlapped with the concurrent split.
    private async Task RunConcurrentSplitScanAsync(
        string treeId,
        int keyCount,
        Func<ILattice, IAsyncEnumerable<string>> scan,
        Func<string?, string, bool> orderViolation,
        int? expectedCount = null)
    {
        var expected = await SeedAsync(treeId, keyCount);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var failures = new ConcurrentBag<string>();
        var completedScans = 0;
        using var cts = new CancellationTokenSource();

        var scanner = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    string? prev = null;
                    var count = 0;
                    await foreach (var k in scan(tree))
                    {
                        if (cts.IsCancellationRequested) break;
                        if (prev is not null && orderViolation(prev, k))
                            failures.Add($"ordering break at idx {count}: '{prev}' -> '{k}'");
                        prev = k;
                        count++;
                    }
                    if (!cts.IsCancellationRequested)
                    {
                        if (expectedCount is int ec && count != ec)
                            failures.Add($"yielded {count}, expected {ec}");
                        Interlocked.Increment(ref completedScans);
                    }
                }
                catch (Exception ex) when (cts.IsCancellationRequested) { _ = ex; }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { }
                catch (InvalidOperationException ex) when (ex.Message.Contains("retries while topology kept changing"))
                {
                    // MaxScanRetries exhaustion is a valid outcome under
                    // tight retry caps — count as completed for liveness.
                    Interlocked.Increment(ref completedScans);
                }
                catch (Exception ex) { failures.Add($"threw {ex.GetType().Name}: {ex.Message}"); }
            }
        });

        await Task.Delay(150);
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);
        await Task.Delay(300);

        cts.Cancel();
        await scanner;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Ordering failures during concurrent split:\n {string.Join("\n ", failures.Take(20))}");
            Assert.That(completedScans, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task KeysAsync_multipage_during_concurrent_split_yields_strictly_ascending_order()
    {
        var treeId = $"mp-fwd-{Guid.NewGuid():N}";
        await RunConcurrentSplitScanAsync(
            treeId, keyCount: 400,
            scan: t => t.KeysAsync(),
            orderViolation: (prev, k) => string.CompareOrdinal(prev, k) >= 0,
            expectedCount: 400);
    }

    [Test]
    public async Task KeysAsync_multipage_reverse_during_concurrent_split_yields_strictly_descending_order()
    {
        var treeId = $"mp-rev-{Guid.NewGuid():N}";
        await RunConcurrentSplitScanAsync(
            treeId, keyCount: 400,
            scan: t => t.KeysAsync(null, null, reverse: true),
            orderViolation: (prev, k) => string.CompareOrdinal(prev, k) <= 0,
            expectedCount: 400);
    }

    [Test]
    public async Task EntriesAsync_multipage_during_concurrent_split_yields_strictly_ascending_key_order()
    {
        var treeId = $"mp-ent-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 400);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var failures = new ConcurrentBag<string>();
        var completedScans = 0;
        using var cts = new CancellationTokenSource();

        var scanner = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    string? prev = null;
                    var count = 0;
                    await foreach (var kv in tree.EntriesAsync())
                    {
                        if (cts.IsCancellationRequested) break;
                        if (prev is not null && string.CompareOrdinal(prev, kv.Key) >= 0)
                            failures.Add($"ordering break: '{prev}' -> '{kv.Key}'");
                        prev = kv.Key;
                        count++;
                    }
                    if (!cts.IsCancellationRequested)
                    {
                        if (count != 400) failures.Add($"yielded {count}, expected 400");
                        Interlocked.Increment(ref completedScans);
                    }
                }
                catch (Exception ex) when (cts.IsCancellationRequested) { _ = ex; }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { }
                catch (InvalidOperationException ex) when (ex.Message.Contains("retries while topology kept changing"))
                {
                    Interlocked.Increment(ref completedScans);
                }
                catch (Exception ex) { failures.Add($"threw {ex.GetType().Name}: {ex.Message}"); }
            }
        });

        await Task.Delay(150);
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();
        await Task.Delay(300);

        cts.Cancel();
        await scanner;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Ordering failures:\n {string.Join("\n ", failures.Take(20))}");
            Assert.That(completedScans, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task KeysAsync_range_bounded_during_concurrent_split_yields_strictly_ascending_in_range()
    {
        var treeId = $"mp-range-{Guid.NewGuid():N}";
        const int keyCount = 400;
        var start = $"mp-{100:D5}";
        var end = $"mp-{300:D5}";
        const int expectedInRange = 200;

        var expected = await SeedAsync(treeId, keyCount);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var failures = new ConcurrentBag<string>();
        var completedScans = 0;
        using var cts = new CancellationTokenSource();

        var scanner = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    string? prev = null;
                    var count = 0;
                    await foreach (var k in tree.KeysAsync(start, end))
                    {
                        if (cts.IsCancellationRequested) break;
                        if (string.CompareOrdinal(k, start) < 0 || string.CompareOrdinal(k, end) >= 0)
                            failures.Add($"out-of-range '{k}'");
                        if (prev is not null && string.CompareOrdinal(prev, k) >= 0)
                            failures.Add($"ordering break: '{prev}' -> '{k}'");
                        prev = k;
                        count++;
                    }
                    if (!cts.IsCancellationRequested)
                    {
                        if (count != expectedInRange) failures.Add($"yielded {count}, expected {expectedInRange}");
                        Interlocked.Increment(ref completedScans);
                    }
                }
                catch (Exception ex) when (cts.IsCancellationRequested) { _ = ex; }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { }
                catch (InvalidOperationException ex) when (ex.Message.Contains("retries while topology kept changing"))
                {
                    Interlocked.Increment(ref completedScans);
                }
                catch (Exception ex) { failures.Add($"threw {ex.GetType().Name}: {ex.Message}"); }
            }
        });

        await Task.Delay(150);
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();
        await Task.Delay(300);

        cts.Cancel();
        await scanner;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Range-scan ordering failures:\n {string.Join("\n ", failures.Take(20))}");
            Assert.That(completedScans, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task KeysAsync_prefetch_multipage_during_concurrent_split_yields_strictly_ascending_order()
    {
        var treeId = $"mp-pref-{Guid.NewGuid():N}";
        await RunConcurrentSplitScanAsync(
            treeId, keyCount: 400,
            scan: t => t.KeysAsync(null, null, reverse: false, prefetch: true),
            orderViolation: (prev, k) => string.CompareOrdinal(prev, k) >= 0,
            expectedCount: 400);
    }

    [Test]
    public async Task KeysAsync_multipage_after_split_completes_yields_strictly_ascending_order()
    {
        // Regression guard for the final-stability code path: seed, split
        // and drain fully, THEN scan. All live-cursor drains plus
        // final-stability reconcile must still produce strict order under
        // multi-page pagination.
        var treeId = $"mp-post-{Guid.NewGuid():N}";
        var expected = await SeedAsync(treeId, 400);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);

        var observed = new List<string>();
        await foreach (var k in tree.KeysAsync()) observed.Add(k);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Count, Is.EqualTo(expected.Count));
            for (int i = 1; i < observed.Count; i++)
                Assert.That(string.CompareOrdinal(observed[i - 1], observed[i]), Is.LessThan(0),
                    $"Not strictly ascending at index {i}: '{observed[i - 1]}' vs '{observed[i]}'");
        });
    }
}
