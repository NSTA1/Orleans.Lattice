using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Cross-silo coverage for the metrics-observation surface. A sample and a live
/// subscription are served by a silo that did not originate the writes, and the
/// tree's shards are spread across the cluster, so the reported aggregates prove
/// the metrics observer reconciles cluster-wide structural state through real
/// cross-silo fan-out rather than only the shards local to the serving silo.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    [Test]
    public async Task SampleMetrics_served_by_non_originating_silo_reconciles_across_silos()
    {
        const string treeId = "multisilo-metrics-sample";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 80);

        var fromOther = await _fixture.MetricsFromOtherSilo().SampleAsync(
            new TreeMetricsRequest { TreeIds = new[] { treeId } });
        var direct = await _fixture.Query.GetTreeSummaryAsync(treeId, deep: true);

        var metrics = fromOther.Trees.Single(t => t.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(fromOther.IsInitial, Is.True);
            Assert.That(metrics.LiveKeys, Is.EqualTo(80),
                "the per-shard rollup must sum to the full key set even when shards are on different silos");
            Assert.That(metrics.LiveKeys, Is.EqualTo(direct.Summary!.TotalLiveKeys));
            Assert.That(metrics.ShardCount, Is.EqualTo(MultiSiloStateApiClusterFixture.ShardCount));
        });
    }

    [Test]
    public async Task ObserveMetrics_served_by_non_originating_silo_reflects_cross_silo_mutation()
    {
        const string treeId = "multisilo-metrics-observe";
        var tree = await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 20);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var snapshots = new List<TreeMetricsSnapshot>();
        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var snapshot in _fixture.MetricsFromOtherSilo().ObserveAsync(
                    new TreeMetricsRequest { TreeIds = new[] { treeId } }, cts.Token))
                {
                    lock (snapshots)
                    {
                        snapshots.Add(snapshot);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }, cts.Token);

        // Wait for the initial full snapshot, then drive a mutation through the
        // cluster client and assert the non-originating silo's feed reflects it.
        await WaitUntilAsync(() => { lock (snapshots) { return snapshots.Count >= 1; } }, cts.Token);

        for (var i = 20; i < 40; i++)
        {
            await tree.SetAsync($"key-{i:D5}", new byte[] { (byte)(i & 0xFF) });
        }

        var reflected = await WaitUntilAsync(
            () =>
            {
                lock (snapshots)
                {
                    return snapshots.Skip(1).Any(s => s.Trees.Any(t => t.TreeId == treeId && t.LiveKeys == 40));
                }
            },
            cts.Token);

        await cts.CancelAsync();
        try
        {
            await pump;
        }
        catch (OperationCanceledException)
        {
        }

        Assert.That(reflected, Is.True,
            "a metrics subscription on a non-originating silo must reflect a cross-silo mutation within a sample interval");
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> predicate, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (predicate())
            {
                return true;
            }

            try
            {
                await Task.Delay(25, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return predicate();
    }
}
