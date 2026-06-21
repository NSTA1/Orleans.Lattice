using System.Text;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for the live metrics-observation facade
/// (<see cref="ILatticeStateMetricsObserver"/>). Asserts that a one-shot poll
/// matches a direct structural read, that the stream's first tick is a full
/// snapshot and later ticks are deltas (idle ticks empty, a mutation reflected
/// within a sample interval, only the changed tree present), that opt-in
/// per-shard hotness and view-lag rollups populate, that an active feed does
/// not back-pressure foreground writes, and that cancellation ends the stream.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeMetricsObservationIntegrationTests
{
    private MetricsObservationClusterFixture _fixture = null!;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MetricsObservationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task sample_matches_direct_structural_read()
    {
        var treeId = $"metrics-sample-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 12, shardCount: 2);

        var snapshot = await _fixture.Metrics.SampleAsync(new TreeMetricsRequest { TreeIds = new[] { treeId } });
        var direct = await _fixture.Query.GetTreeSummaryAsync(treeId, deep: true);

        Assert.That(snapshot.IsInitial, Is.True);
        var metrics = snapshot.Trees.Single(t => t.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(metrics.LiveKeys, Is.EqualTo(direct.Summary!.TotalLiveKeys));
            Assert.That(metrics.Tombstones, Is.EqualTo(direct.Summary!.TombstoneCount));
            Assert.That(metrics.ShardCount, Is.EqualTo(direct.Summary!.ShardCount));
            Assert.That(metrics.MinDepth, Is.EqualTo(direct.Summary!.MinDepth));
            Assert.That(metrics.MaxDepth, Is.EqualTo(direct.Summary!.MaxDepth));
            Assert.That(metrics.Lifecycle, Is.EqualTo(direct.Summary!.Lifecycle));
        });
    }

    [Test]
    public async Task sample_includes_shard_hotness_when_requested()
    {
        var treeId = $"metrics-hot-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 8, shardCount: 3);

        var snapshot = await _fixture.Metrics.SampleAsync(
            new TreeMetricsRequest { TreeIds = new[] { treeId }, IncludeShardHotness = true });

        var metrics = snapshot.Trees.Single(t => t.TreeId == treeId);
        Assert.That(metrics.ShardHotness, Has.Count.EqualTo(metrics.ShardCount));
        Assert.That(metrics.ShardHotness.Select(s => s.ShardIndex),
            Is.EquivalentTo(Enumerable.Range(0, metrics.ShardCount)));
    }

    [Test]
    public async Task sample_without_hotness_request_omits_shard_rows()
    {
        var treeId = $"metrics-nohot-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 2);

        var snapshot = await _fixture.Metrics.SampleAsync(new TreeMetricsRequest { TreeIds = new[] { treeId } });

        var metrics = snapshot.Trees.Single(t => t.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(metrics.ShardHotness, Is.Empty);
            Assert.That(metrics.ViewCount, Is.Null);
            Assert.That(metrics.ViewLagTotal, Is.Null);
        });
    }

    [Test]
    public async Task sample_with_view_lag_request_reports_zero_views_for_plain_tree()
    {
        var treeId = $"metrics-view-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 4, shardCount: 1);

        var snapshot = await _fixture.Metrics.SampleAsync(
            new TreeMetricsRequest { TreeIds = new[] { treeId }, IncludeViewLag = true });

        var metrics = snapshot.Trees.Single(t => t.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(metrics.ViewCount, Is.EqualTo(0));
            Assert.That(metrics.ViewLagTotal, Is.Null);
        });
    }

    [Test]
    public async Task observe_first_tick_is_full_initial_snapshot()
    {
        var treeId = $"metrics-initial-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 5, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeId } });

        var arrived = await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Count >= 1; } }, Timeout);

        await cts.CancelAsync();
        await pump;

        Assert.That(arrived, Is.True);
        Assert.That(snapshots[0].IsInitial, Is.True);
        Assert.That(snapshots[0].Trees.Select(t => t.TreeId), Does.Contain(treeId));
    }

    [Test]
    public async Task observe_idle_cluster_yields_empty_delta_ticks()
    {
        var treeId = $"metrics-idle-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 3, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeId } });

        // Wait for the initial tick plus at least one idle delta tick.
        var gotDelta = await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Any(s => !s.IsInitial); } }, Timeout);

        await cts.CancelAsync();
        await pump;

        Assert.That(gotDelta, Is.True);
        TreeMetricsSnapshot firstDelta;
        lock (snapshots)
        {
            firstDelta = snapshots.First(s => !s.IsInitial);
        }

        Assert.Multiple(() =>
        {
            Assert.That(firstDelta.Trees, Is.Empty, "An idle cluster must produce empty delta ticks.");
            Assert.That(firstDelta.RemovedTreeIds, Is.Empty);
        });
    }

    [Test]
    public async Task observe_reflects_mutation_within_a_sample_interval()
    {
        var treeId = $"metrics-mutate-{Guid.NewGuid():N}";
        var tree = await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 5, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeId } });

        await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Count >= 1; } }, Timeout);

        for (var i = 5; i < 12; i++)
        {
            await tree.SetAsync(MetricsObservationClusterFixture.KeyAt(i), Encoding.UTF8.GetBytes("v"));
        }

        var reflected = await MetricsObservationClusterFixture.WaitUntilAsync(
            () =>
            {
                lock (snapshots)
                {
                    return snapshots.Skip(1).Any(s => s.Trees.Any(t => t.TreeId == treeId && t.LiveKeys == 12));
                }
            },
            Timeout);

        await cts.CancelAsync();
        await pump;

        Assert.That(reflected, Is.True, "The metrics feed must reflect a mutation within a sample interval.");
    }

    [Test]
    public async Task observe_delta_contains_only_the_changed_tree()
    {
        var treeA = $"metrics-a-{Guid.NewGuid():N}";
        var treeB = $"metrics-b-{Guid.NewGuid():N}";
        var a = await _fixture.CreatePopulatedTreeAsync(treeA, keyCount: 4, shardCount: 1);
        await _fixture.CreatePopulatedTreeAsync(treeB, keyCount: 4, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeA, treeB } });

        await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Count >= 1; } }, Timeout);

        for (var i = 4; i < 10; i++)
        {
            await a.SetAsync(MetricsObservationClusterFixture.KeyAt(i), Encoding.UTF8.GetBytes("v"));
        }

        TreeMetricsSnapshot? changedTick = null;
        await MetricsObservationClusterFixture.WaitUntilAsync(
            () =>
            {
                lock (snapshots)
                {
                    changedTick = snapshots.Skip(1).FirstOrDefault(s => s.Trees.Any(t => t.TreeId == treeA && t.LiveKeys == 10));
                    return changedTick is not null;
                }
            },
            Timeout);

        await cts.CancelAsync();
        await pump;

        Assert.That(changedTick, Is.Not.Null);
        Assert.That(changedTick!.Trees.Select(t => t.TreeId), Is.EqualTo(new[] { treeA }),
            "A delta tick must carry only the tree whose aggregates changed.");
    }

    [Test]
    public async Task observe_active_feed_does_not_block_concurrent_writer()
    {
        var treeId = $"metrics-bp-{Guid.NewGuid():N}";
        var tree = await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 1, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeId }, IncludeShardHotness = true });

        await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Count >= 1; } }, Timeout);

        var start = DateTime.UtcNow;
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync(MetricsObservationClusterFixture.KeyAt(i), Encoding.UTF8.GetBytes("v"));
        }
        var elapsed = DateTime.UtcNow - start;

        await cts.CancelAsync();
        await pump;

        Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(8)),
            "An active metrics feed must not back-pressure foreground writes.");
    }

    [Test]
    public async Task observe_cancellation_completes_the_stream()
    {
        var treeId = $"metrics-cancel-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 2, shardCount: 1);

        var (pump, snapshots, cts) = _fixture.ObserveInBackground(
            new TreeMetricsRequest { TreeIds = new[] { treeId } });

        await MetricsObservationClusterFixture.WaitUntilAsync(
            () => { lock (snapshots) { return snapshots.Count >= 1; } }, Timeout);

        await cts.CancelAsync();
        await pump.WaitAsync(Timeout);

        Assert.That(pump.IsCompleted, Is.True);
    }
}
