using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression: <c>CountAsync</c> and <c>CountPerShardAsync</c> must
/// return the exact live key count even when a shard split commits
/// concurrently. The v1 fix (shard-map version stability check on top of
/// <c>CountWithMovedAwayAsync</c>) was insufficient because the split
/// coordinator publishes the new <c>ShardMap</c> (including the target
/// shard) before advancing the source shard's persisted phase to
/// <c>Reject</c>. During that window the source does not filter
/// moved-slot keys while the target already holds them, and the version
/// check passes because the map is already at its new version. The v2 fix
/// routes each count through the authoritative <c>ShardMap</c> via
/// <c>CountForSlotsAsync</c> so every virtual slot is counted exactly
/// once, against the shard the map identifies as its current owner.
/// <para>
/// These tests drive <c>tree.CountAsync</c> / <c>tree.CountPerShardAsync</c>
/// in a tight concurrent loop across the full
/// <c>SplitAsync</c> + <c>RunSplitPassAsync</c> phase machine so every
/// count observation must see the pinned universe size. An observation
/// taken during the pre-v2 race window would double-count migrating
/// slots.
/// </para>
/// </summary>
[TestFixture]
public class CountAsyncSplitRegressionTests
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

    [Test]
    public async Task CountAsync_matches_true_live_count_after_split()
    {
        var treeId = $"count-split-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 400;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(keyCount),
            "CountAsync must reconcile per-slot ownership across a mid-count split");
    }

    [Test]
    public async Task CountPerShardAsync_matches_true_live_count_after_split()
    {
        var treeId = $"pershard-split-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 400;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard.Sum(), Is.EqualTo(keyCount),
            "CountPerShardAsync must reconcile per-slot ownership across a mid-count split");
    }

    /// <summary>
    /// Deterministic reproducer for the mid-split counter race: counter spins
    /// while the split coordinator runs its full phase machine. The
    /// pre-v2 implementation over-counted inside the window between
    /// <c>SwapAsync</c> publishing the new map and <c>EnterRejectAsync</c>
    /// flipping the source shard's phase to <c>Reject</c>. Any observation
    /// inside that window that sees <c>CountAsync != keyCount</c> fails
    /// the test.
    /// </summary>
    [Test]
    public async Task CountAsync_never_overcounts_during_concurrent_split()
    {
        var treeId = $"count-mid-split-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 300;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

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
                    if (c != keyCount)
                        failures.Add($"CountAsync={c}, expected {keyCount}");
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
        // Let the counter loop sample the fully-completed state as well.
        await Task.Delay(150);

        cts.Cancel();
        await counter;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Concurrent CountAsync observations must all equal the pinned universe:\n  "
                + string.Join("\n  ", failures.Take(10)));
            Assert.That(iterations, Is.GreaterThan(0),
                "Counter must have completed at least one iteration against the mid-split state.");
        });
    }

    /// <summary>
    /// Deterministic reproducer mirroring
    /// <see cref="CountAsync_never_overcounts_during_concurrent_split"/> for
    /// the per-shard variant: its summed result must also equal the pinned
    /// universe size throughout the split window.
    /// </summary>
    [Test]
    public async Task CountPerShardAsync_never_overcounts_during_concurrent_split()
    {
        var treeId = $"pershard-mid-split-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 300;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var failures = new ConcurrentBag<string>();
        var iterations = 0;
        using var cts = new CancellationTokenSource();

        var counter = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var per = await tree.CountPerShardAsync();
                    var sum = per.Sum();
                    if (sum != keyCount)
                        failures.Add($"CountPerShardAsync sum={sum}, expected {keyCount}");
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
                $"Concurrent CountPerShardAsync observations must all sum to the pinned universe:\n  "
                + string.Join("\n  ", failures.Take(10)));
            Assert.That(iterations, Is.GreaterThan(0),
                "Counter must have completed at least one iteration against the mid-split state.");
        });
    }
}
