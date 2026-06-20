using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the server-side ranged count
/// <c>ILattice.CountAsync(startInclusive, endExclusive)</c>. The ranged count
/// reuses the whole-tree count machinery (per-slot routing against the
/// authoritative <c>ShardMap</c> plus the version / TxRegistry stability
/// checks) so it must be exact across multiple leaves, multiple shards, and a
/// concurrent adaptive split - the same guarantees the unbounded
/// <c>CountAsync()</c> carries. These tests run over the four-shard / four-keys
/// per-leaf fixture so any non-trivial key set spans many leaves and all four
/// shards.
/// </summary>
[TestFixture]
[Category("Integration")]
public class RangedCountAsyncIntegrationTests
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> SeedAsync(string treeId, int keyCount)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        return tree;
    }

    [Test]
    public async Task CountAsync_null_bounds_equals_whole_tree_count()
    {
        var tree = await SeedAsync($"ranged-null-{Guid.NewGuid():N}", 120);

        var unbounded = await tree.CountAsync();
        var ranged = await tree.CountAsync(null, null);

        Assert.That(ranged, Is.EqualTo(unbounded));
        Assert.That(ranged, Is.EqualTo(120));
    }

    [Test]
    public async Task CountAsync_range_is_inclusive_of_start_and_exclusive_of_end()
    {
        var tree = await SeedAsync($"ranged-bounds-{Guid.NewGuid():N}", 200);

        // [k-0050, k-0150) -> exactly 100 keys, spanning many leaves and all
        // four shards (keys hash uniformly across the slot space).
        var count = await tree.CountAsync("k-0050", "k-0150");
        Assert.That(count, Is.EqualTo(100));
    }

    [Test]
    public async Task CountAsync_start_only_counts_from_floor_to_end()
    {
        var tree = await SeedAsync($"ranged-start-{Guid.NewGuid():N}", 200);

        // [k-0150, null) -> k-0150 .. k-0199 == 50 keys.
        var count = await tree.CountAsync("k-0150", null);
        Assert.That(count, Is.EqualTo(50));
    }

    [Test]
    public async Task CountAsync_end_only_counts_up_to_bound()
    {
        var tree = await SeedAsync($"ranged-end-{Guid.NewGuid():N}", 200);

        // [null, k-0030) -> k-0000 .. k-0029 == 30 keys.
        var count = await tree.CountAsync(null, "k-0030");
        Assert.That(count, Is.EqualTo(30));
    }

    [Test]
    public async Task CountAsync_empty_range_returns_zero()
    {
        var tree = await SeedAsync($"ranged-empty-{Guid.NewGuid():N}", 100);

        // start == end is an empty half-open interval.
        Assert.That(await tree.CountAsync("k-0050", "k-0050"), Is.Zero);
        // A range entirely above every key.
        Assert.That(await tree.CountAsync("z", null), Is.Zero);
        // A range entirely below every key.
        Assert.That(await tree.CountAsync(null, "k-0000"), Is.Zero);
    }

    [Test]
    public async Task CountAsync_range_excludes_a_reserved_floor_prefix()
    {
        // Mirrors the aggregation-view usage: a reserved NUL-prefixed row must
        // be excluded by counting [\u0001, null) so it never inflates the
        // visible key count.
        var tree = await _fixture.CreateTreeAsync($"ranged-floor-{Guid.NewGuid():N}");
        await tree.SetAsync("\0reserved-a", Encoding.UTF8.GetBytes("r"));
        await tree.SetAsync("\0reserved-b", Encoding.UTF8.GetBytes("r"));
        for (int i = 0; i < 40; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var visible = await tree.CountAsync("\u0001", null);
        Assert.That(visible, Is.EqualTo(40));
        Assert.That(await tree.CountAsync(), Is.EqualTo(42));
    }

    [Test]
    public async Task CountAsync_ranged_matches_true_live_count_after_split()
    {
        var treeId = $"ranged-count-split-{Guid.NewGuid():N}";
        var tree = await SeedAsync(treeId, 400);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        // A range covering every key must reconcile per-slot ownership across
        // the split exactly as the unbounded count does.
        var count = await tree.CountAsync("k-0000", null);
        Assert.That(count, Is.EqualTo(400),
            "Ranged CountAsync must reconcile per-slot ownership across a mid-count split");
    }

    /// <summary>
    /// Deterministic reproducer for the mid-split counter race against the
    /// ranged count: the counter spins over a full-cover range while the split
    /// coordinator runs its phase machine. Any observation that double-counted
    /// a migrating slot would surface as <c>count != keyCount</c>.
    /// </summary>
    [Test]
    public async Task CountAsync_ranged_never_overcounts_during_concurrent_split()
    {
        var treeId = $"ranged-count-mid-split-{Guid.NewGuid():N}";
        var tree = await SeedAsync(treeId, 300);

        var failures = new ConcurrentBag<string>();
        var iterations = 0;
        using var cts = new CancellationTokenSource();

        var counter = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    // Full-cover range: result must always equal the pinned
                    // universe size throughout the split window.
                    var c = await tree.CountAsync("k-0000", null);
                    if (c != 300)
                        failures.Add($"CountAsync(range)={c}, expected 300");
                    Interlocked.Increment(ref iterations);
                }
                catch (Exception) when (cts.IsCancellationRequested) { }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { }
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
            Assert.That(failures, Is.Empty,
                "Concurrent ranged CountAsync observations must all equal the pinned universe:\n  "
                + string.Join("\n  ", failures.Take(10)));
            Assert.That(iterations, Is.GreaterThan(0),
                "Counter must have completed at least one iteration against the mid-split state.");
        });
    }

    /// <summary>
    /// A bounded sub-range count must also stay exact across a concurrent
    /// split: the migrating-slot double-count window is independent of the
    /// range bound, and the sub-range population is pinned by the deterministic
    /// key set.
    /// </summary>
    [Test]
    public async Task CountAsync_bounded_subrange_is_exact_during_concurrent_split()
    {
        var treeId = $"ranged-sub-mid-split-{Guid.NewGuid():N}";
        var tree = await SeedAsync(treeId, 300);

        var failures = new ConcurrentBag<string>();
        var iterations = 0;
        using var cts = new CancellationTokenSource();

        var counter = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    // [k-0100, k-0200) -> exactly 100 keys, regardless of how
                    // the slots backing those keys are split across shards.
                    var c = await tree.CountAsync("k-0100", "k-0200");
                    if (c != 100)
                        failures.Add($"CountAsync(subrange)={c}, expected 100");
                    Interlocked.Increment(ref iterations);
                }
                catch (Exception) when (cts.IsCancellationRequested) { }
                catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException") { }
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
            Assert.That(failures, Is.Empty,
                "Concurrent bounded ranged CountAsync observations must all equal the pinned sub-range size:\n  "
                + string.Join("\n  ", failures.Take(10)));
            Assert.That(iterations, Is.GreaterThan(0),
                "Counter must have completed at least one iteration against the mid-split state.");
        });
    }
}
