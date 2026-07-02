using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

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

    private async Task<ILattice> DrainToZeroAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return await _fixture.ActiveViewTreeAsync(viewName);
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
        return await _fixture.ActiveViewTreeAsync(viewName);
    }

    private static double? SumValue(byte[]? bytes) =>
        bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

    [Test]
    public async Task Sum_view_converges_across_inserts_overwrites_and_deletes()
    {
        // White-box reads of a view's backing tree run under an authorised
        // ViewReadContext scope (as the maintainer and ILatticeView handle do);
        // the public read-guard otherwise rejects direct view-tree reads.
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "agg-sum-src";
        const string view = "agg-sum-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, AggregationKind.Sum);

        await source.SetAsync("a", Record("red", 10));
        await source.SetAsync("b", Record("red", 5));
        await source.SetAsync("c", Record("blue", 7));
        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(15));
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(7));
        });

        // Overwrite a's score (10 -> 2): red must drop to 7, not accumulate.
        await source.SetAsync("a", Record("red", 2));
        viewTree = await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(7));

        // Delete b: red drops to 2.
        await source.DeleteAsync("b");
        viewTree = await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(2));
    }

    [Test]
    public async Task Min_view_redrives_after_deleting_current_extremum()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "agg-min-src";
        const string view = "agg-min-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, AggregationKind.Min);

        await source.SetAsync("a", Record("red", 5));
        await source.SetAsync("b", Record("red", 2));
        await source.SetAsync("c", Record("red", 9));
        var viewTree = await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(2));

        // Delete the current minimum: min must re-derive to the next survivor (5).
        await source.DeleteAsync("b");
        viewTree = await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(5));
    }

    [Test]
    public async Task Committed_atomic_batch_folds_into_aggregation_groups_only_after_commit()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "agg-atomic-src";
        const string view = "agg-atomic-view";
        const string origin = "remote-origin";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        _ = CreateView(tree, view, AggregationKind.Sum);
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
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("red"), Is.Null, "An uncommitted atomic batch must not fold into the aggregation.");

        // Commit every touched shard, then drain: the whole batch folds in atomically.
        var shards = new[] { "x", "y", "z" }.Select(ShardOf).Distinct().ToArray();
        var ticks = 1000L;
        foreach (var shard in shards)
        {
            await apply.ApplyTxTerminalAsync(txId, true, shard, Hlc(ticks++), origin, atomicShardCount: shards.Length);
        }

        viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(10), "red = 4 + 6 folded atomically.");
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(9));
        });
    }

    [Test]
    public async Task Single_tree_atomic_batch_folds_onto_an_existing_group_atomically()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "agg-atomic-onto-existing-src";
        const string view = "agg-atomic-onto-existing-view";
        const string origin = "remote-origin";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        _ = CreateView(tree, view, AggregationKind.Sum);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        HybridLogicalClock Hlc(long t) => new() { WallClockTicks = t };
        int ShardOf(string key) => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

        // Seed an existing "red" group accumulator with an ordinary contribution
        // (lower HLC than the atomic batch, so it is never superseded).
        await apply.ApplySetAsync("p1", Record("red", 3), Hlc(50), origin, null, 0);
        await DrainToZeroAsync(view);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(3), "the existing group folds the ordinary contribution.");

        // A single-tree atomic batch adds two more contributions to the same group.
        var txId = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync("x", Record("red", 4), Hlc(100), origin, null, 0, txId, 2, 0);
        await apply.ApplyPreparedSetAsync("y", Record("red", 6), Hlc(101), origin, null, 0, txId, 2, 1);

        // Pre-commit: the staged batch must not move the existing accumulator.
        await maintainer.DrainAsync();
        await maintainer.DrainAsync();
        viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(3),
            "an uncommitted atomic batch must not fold onto the existing group.");

        // Commit: the batch flips the accumulator atomically (3 -> 13), read back
        // through the same captured-then-atomic flip the single-tree path uses.
        var shards = new[] { "x", "y" }.Select(ShardOf).Distinct().ToArray();
        var ticks = 1000L;
        foreach (var shard in shards)
        {
            await apply.ApplyTxTerminalAsync(txId, true, shard, Hlc(ticks++), origin, atomicShardCount: shards.Length);
        }

        viewTree = await DrainToZeroAsync(view);
        Assert.That(SumValue(await viewTree.GetAsync("red")), Is.EqualTo(13), "red = 3 + 4 + 6 after the atomic batch folds in.");
    }

    [Test]
    public async Task Aggregation_view_CountAsync_returns_distinct_group_count_excluding_reserved_rows()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "agg-count-src";
        const string view = "agg-count-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var handle = CreateView(tree, view, AggregationKind.Sum);

        // Four source keys across three distinct groups. The aggregation
        // maintainer also writes reserved accumulator / inverse / membership
        // rows under the NUL prefix, so the view's backing tree holds strictly
        // more than three rows.
        await source.SetAsync("a", Record("red", 10));
        await source.SetAsync("b", Record("red", 5));
        await source.SetAsync("c", Record("blue", 7));
        await source.SetAsync("d", Record("green", 3));
        var viewTree = await DrainToZeroAsync(view);

        // The view-facing CountAsync must report only the materialised group
        // values (red, blue, green) and never the reserved internal rows.
        var groupCount = await handle.CountAsync();
        Assert.That(groupCount, Is.EqualTo(3),
            "Aggregation view CountAsync must count only the distinct group values.");

        // The raw whole-tree count includes the reserved NUL-prefixed rows, so
        // it must be strictly greater than the group count - proving those rows
        // exist and that the ranged count genuinely excludes them rather than
        // there being no reserved rows to begin with.
        var rawCount = await viewTree.CountAsync();
        Assert.That(rawCount, Is.GreaterThan(groupCount),
            "The backing tree must hold reserved rows beyond the group values.");

        // A delete that empties the green group drops the visible count to 2.
        await source.DeleteAsync("d");
        await DrainToZeroAsync(view);
        var afterDelete = await handle.CountAsync();
        Assert.That(afterDelete, Is.EqualTo(2),
            "Removing the only contribution to a group drops the group count.");
    }

    // --- custom-reducer (folded) aggregation view ---

    private sealed record Fact(string Part, string Kind);

    // A non-commutative, HLC-ordered compliance state machine (the issue-1039
    // motivation): "Flag" demotes to FlaggedForReview, "UseAsIs" promotes back to
    // Nominal, "Scrap" is terminal. The materialised value is the final state
    // string, so ordering and retraction are directly observable.
    private static ILatticeView CreateComplianceView(MaterialisedViewClusterFixture fixture, string sourceTreeId, string viewName)
    {
        var factory = fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = LatticeFoldProjection.Create<Fact, string>(
            groupKeySelector: f => f.Part,
            initial: () => "Nominal",
            apply: (state, _, fact, _) => state switch
            {
                "Scrap" => "Scrap",
                _ => fact.Kind switch
                {
                    "Flag" => "FlaggedForReview",
                    "UseAsIs" => "Nominal",
                    "Scrap" => "Scrap",
                    _ => state,
                },
            },
            foldVersion: "compliance-v1");
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private static byte[] FactValue(string part, string kind) =>
        JsonLatticeSerializer<Fact>.Default.Serialize(new Fact(part, kind));

    private static string? StateOf(byte[]? bytes) =>
        bytes is null ? null : JsonLatticeSerializer<string>.Default.Deserialize(bytes);

    [Test]
    public async Task Folded_view_applies_non_commutative_fold_in_hlc_order()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "fold-compliance-src";
        const string view = "fold-compliance-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateComplianceView(_fixture, tree, view);

        // Part p1: Flag then UseAsIs -> back to Nominal (later fact demotes state).
        await source.SetAsync("p1:f1", FactValue("p1", "Flag"));
        await source.SetAsync("p1:f2", FactValue("p1", "UseAsIs"));
        // Part p2: Flag then Scrap -> terminal Scrap.
        await source.SetAsync("p2:f1", FactValue("p2", "Flag"));
        await source.SetAsync("p2:f2", FactValue("p2", "Scrap"));

        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(StateOf(await viewTree.GetAsync("p1")), Is.EqualTo("Nominal"));
            Assert.That(StateOf(await viewTree.GetAsync("p2")), Is.EqualTo("Scrap"));
        });
    }

    [Test]
    public async Task Folded_view_refolds_over_survivors_on_retraction()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "fold-retract-src";
        const string view = "fold-retract-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateComplianceView(_fixture, tree, view);

        await source.SetAsync("p1:f1", FactValue("p1", "Flag"));
        await source.SetAsync("p1:f2", FactValue("p1", "UseAsIs"));
        var viewTree = await DrainToZeroAsync(view);
        Assert.That(StateOf(await viewTree.GetAsync("p1")), Is.EqualTo("Nominal"));

        // Delete the UseAsIs fact: the group re-folds over the surviving Flag fact
        // (a plain reducer could not un-apply this), so the state reverts.
        await source.DeleteAsync("p1:f2");
        viewTree = await DrainToZeroAsync(view);
        Assert.That(StateOf(await viewTree.GetAsync("p1")), Is.EqualTo("FlaggedForReview"));
    }

    [Test]
    public async Task Folded_view_rebuild_reproduces_state_from_source()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "fold-rebuild-src";
        const string view = "fold-rebuild-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateComplianceView(_fixture, tree, view);

        await source.SetAsync("p1:f1", FactValue("p1", "Flag"));
        await source.SetAsync("p1:f2", FactValue("p1", "UseAsIs"));
        await source.SetAsync("p2:f1", FactValue("p2", "Scrap"));
        await DrainToZeroAsync(view);

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.RebuildAsync();
        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(StateOf(await viewTree.GetAsync("p1")), Is.EqualTo("Nominal"));
            Assert.That(StateOf(await viewTree.GetAsync("p2")), Is.EqualTo("Scrap"));
        });
    }

    [Test]
    public async Task Folded_view_digest_is_stable_across_rebuild()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "fold-digest-src";
        const string view = "fold-digest-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var handle = CreateComplianceView(_fixture, tree, view);

        await source.SetAsync("p1:f1", FactValue("p1", "Flag"));
        await source.SetAsync("p1:f2", FactValue("p1", "UseAsIs"));
        await source.SetAsync("p2:f1", FactValue("p2", "Flag"));
        await source.SetAsync("p2:f2", FactValue("p2", "Scrap"));
        await DrainToZeroAsync(view);

        // The materialised value is an opaque fold accumulator (arbitrary bytes),
        // so this asserts ComputeDigestAsync / anti-entropy fingerprints custom
        // accumulators correctly: a rebuild that re-derives the same member set
        // must reproduce a byte-identical digest, so two clusters that converge on
        // the same source never see a spurious drift.
        var beforeRebuild = await handle.ComputeDigestAsync();

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.RebuildAsync();
        await DrainToZeroAsync(view);

        var afterRebuild = await handle.ComputeDigestAsync();
        Assert.That(afterRebuild.ContentEquals(beforeRebuild), Is.True,
            "A folded view's opaque-accumulator digest must be rebuild-stable.");
    }
}
