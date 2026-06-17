using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end integration tests for Phase 3 aggregation materialised views over a
/// live test cluster. A sum view and a min view grouped by a record field are
/// created over a source tree, the source is mutated (inserts, overwrites that
/// change the contributed value, deletes), and the materialised group values are
/// asserted to converge. A rebuild is then asserted to reproduce the same
/// aggregates from current source state. Convergence is driven through the
/// internal maintainer grain's <c>DrainAsync</c> so assertions are deterministic.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewAggregationIntegrationTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Record(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static string Team(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Team;

    private static double Score(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Score;

    private ILatticeView CreateView(string sourceTreeId, string viewName, AggregationKind kind)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new AggregationLatticeViewProjection(kind, Team, "v1", valueSelector: Score);
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task DrainToZeroAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
    }

    private static double? SumValue(byte[]? bytes) =>
        bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

    [Test]
    public async Task Sum_view_converges_across_inserts_overwrites_and_deletes()
    {
        const string tree = "agg-sum-src";
        const string view = "agg-sum-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, AggregationKind.Sum);
        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>($"view-{view}");

        await source.SetAsync("a", Record("red", 10));
        await source.SetAsync("b", Record("red", 5));
        await source.SetAsync("c", Record("blue", 7));
        await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(15));
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(7));
        });

        // Overwrite a's score (10 -> 2): red must drop to 7, not accumulate.
        await source.SetAsync("a", Record("red", 2));
        await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(7));

        // Delete b: red drops to 2.
        await source.DeleteAsync("b");
        await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(2));
    }

    [Test]
    public async Task Min_view_redrives_after_deleting_current_extremum()
    {
        const string tree = "agg-min-src";
        const string view = "agg-min-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, AggregationKind.Min);
        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>($"view-{view}");

        await source.SetAsync("a", Record("red", 5));
        await source.SetAsync("b", Record("red", 2));
        await source.SetAsync("c", Record("red", 9));
        await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(2));

        // Delete the current minimum: min must re-derive to the next survivor (5).
        await source.DeleteAsync("b");
        await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(5));
    }

    [Test]
    public async Task Committed_atomic_batch_folds_into_aggregation_groups_only_after_commit()
    {
        const string tree = "agg-atomic-src";
        const string view = "agg-atomic-view";
        const string origin = "remote-origin";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        _ = CreateView(tree, view, AggregationKind.Sum);
        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>($"view-{view}");
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var txId = Guid.NewGuid();

        HybridLogicalClock Hlc(long t) => new() { WallClockTicks = t };
        int ShardOf(string key) => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

        // Stage two prepared contributions to the same group, plus one to another.
        await apply.ApplyPreparedSetAsync("x", Record("red", 4), Hlc(100), origin, null, 0, txId, 3, 0);
        await apply.ApplyPreparedSetAsync("y", Record("red", 6), Hlc(101), origin, null, 0, txId, 3, 1);
        await apply.ApplyPreparedSetAsync("z", Record("blue", 9), Hlc(102), origin, null, 0, txId, 3, 2);

        // Pre-commit: no group accumulator should exist yet.
        await maintainer.DrainAsync();
        await maintainer.DrainAsync();
        Assert.That(await viewTree.GetAsync("red"), Is.Null, "An uncommitted atomic batch must not fold into the aggregation.");

        // Commit every touched shard, then drain: the whole batch folds in atomically.
        var shards = new[] { "x", "y", "z" }.Select(ShardOf).Distinct().ToArray();
        var ticks = 1000L;
        foreach (var shard in shards)
        {
            await apply.ApplyTxTerminalAsync(txId, true, shard, Hlc(ticks++), origin, atomicShardCount: shards.Length);
        }

        await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(10), "red = 4 + 6 folded atomically.");
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(9));
        });
    }
}
