using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for a staged prepare covered by a newer range delete. The source
/// tombstones every live key a range delete reaches (or exactly its recorded
/// matched keys), so a prepare that committed later but prepared earlier loses
/// to the tombstone by last-writer-wins. The view records an ordinary write as
/// superseding a staged prepare by its key; a range delete must supersede every
/// key it tombstoned, not only its start key.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingRangeSupersessionTests
{
    private const string Origin = "remote-origin";
    private const string OtherOrigin = "remote-origin-b";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ViewPerson(int Age, string Tag);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private async Task<(ILattice Source, IViewMaintainerGrain Maintainer)> CreateViewAsync(string tree, string view)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        _ = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        return (source, maintainer);
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("View did not catch up to the source head.");
    }

    [TestCase(true, false, TestName = "Matched_range_delete_supersedes_an_interior_staged_prepare_live")]
    [TestCase(false, false, TestName = "Unconstrained_range_delete_supersedes_an_interior_staged_prepare_live")]
    [TestCase(true, true, TestName = "Matched_range_delete_supersedes_an_interior_staged_prepare_across_rebuild")]
    [TestCase(false, true, TestName = "Unconstrained_range_delete_supersedes_an_interior_staged_prepare_across_rebuild")]
    public async Task A_newer_range_delete_supersedes_an_older_staged_prepare_on_an_interior_key(bool matched, bool rebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = $"{(matched ? "m" : "u")}{(rebuild ? "r" : "l")}";
        var tree = $"mv-range-supersede-{suffix}-src";
        var view = $"mv-range-supersede-{suffix}-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var start = $"range-{suffix}-a";
        var key = $"range-{suffix}-m";
        var end = $"range-{suffix}-z";
        var tx = Guid.NewGuid();

        // The key is live; a batch prepares a newer value for it and is staged.
        await apply.ApplySetAsync(key, Person(20, "base"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "prepared"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();

        // A newer range delete, starting below the key, tombstones it. A
        // predicate-filtered delete records the keys it matched; an ordinary one
        // records only its bounds.
        if (matched)
        {
            await source.DeleteRangeWherePredicateAsync(
                LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18), start, end);
        }
        else
        {
            await apply.ApplyDeleteRangeAsync(start, end, Hlc(200), Origin, sourceVectorClock: null);
        }
        if (rebuild)
        {
            await maintainer.RebuildAsync();
        }
        else
        {
            await maintainer.DrainAsync();
        }

        // The batch commits late; its older prepare loses to the tombstone.
        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.Null, "The source keeps the newer range tombstone over the older prepare.");
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "A newer range delete must supersede an older staged prepare on every key it tombstoned.");
        });
    }

    [TestCase(false, TestName = "Unconstrained_range_delete_does_not_supersede_a_prepare_on_a_key_it_did_not_tombstone_live")]
    [TestCase(true, TestName = "Unconstrained_range_delete_does_not_supersede_a_prepare_on_a_key_it_did_not_tombstone_across_rebuild")]
    public async Task A_range_delete_that_tombstoned_nothing_at_the_key_does_not_supersede_its_prepare(bool rebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = rebuild ? "r" : "l";
        var tree = $"mv-range-nolive-{suffix}-src";
        var view = $"mv-range-nolive-{suffix}-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var start = $"nolive-{suffix}-a";
        var key = $"nolive-{suffix}-m";
        var other = $"nolive-{suffix}-n";
        var end = $"nolive-{suffix}-z";
        var tx = Guid.NewGuid();

        // The key is not live, so a range delete over it tombstones only the
        // other, live key; the later-committed prepare then installs.
        await apply.ApplySetAsync(other, Person(20, "other"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "prepared"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();
        await apply.ApplyDeleteRangeAsync(start, end, Hlc(200), Origin, sourceVectorClock: null);
        if (rebuild)
        {
            await maintainer.RebuildAsync();
        }
        else
        {
            await maintainer.DrainAsync();
        }

        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(30, "prepared")), "A key the range delete did not tombstone keeps the prepare.");
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "A range delete must not supersede a prepare on a key it did not tombstone.");
        });
    }

    [TestCase(false, TestName = "Matched_range_delete_does_not_supersede_a_prepare_on_its_non_live_start_key_live")]
    [TestCase(true, TestName = "Matched_range_delete_does_not_supersede_a_prepare_on_its_non_live_start_key_across_rebuild")]
    public async Task A_matched_range_delete_does_not_supersede_a_prepare_on_its_start_key(bool rebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = rebuild ? "r" : "l";
        var tree = $"mv-range-startkey-{suffix}-src";
        var view = $"mv-range-startkey-{suffix}-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var start = $"startkey-{suffix}-a";
        var other = $"startkey-{suffix}-m";
        var end = $"startkey-{suffix}-z";
        var tx = Guid.NewGuid();

        // The prepare sits on the range's start key, which is not live, so the
        // predicate-filtered delete tombstones (and records) only the other key.
        await apply.ApplySetAsync(other, Person(20, "other"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplyPreparedSetAsync(
            start, Person(30, "prepared"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();
        await source.DeleteRangeWherePredicateAsync(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18), start, end);
        if (rebuild)
        {
            await maintainer.RebuildAsync();
        }
        else
        {
            await maintainer.DrainAsync();
        }

        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(start), Hlc(1000), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(start);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(30, "prepared")), "The range delete tombstoned nothing at its start key.");
            Assert.That(await viewTree.GetAsync(start), Is.EqualTo(sourceValue),
                "A range delete's start key must not supersede a prepare there unless the delete tombstoned it.");
            Assert.That(await viewTree.GetAsync(other), Is.Null, "The matched key is tombstoned in the view.");
        });
    }

    [TestCase(false, TestName = "Ordinary_write_logged_before_an_older_prepare_supersedes_it_live")]
    [TestCase(true, TestName = "Ordinary_write_logged_before_an_older_prepare_supersedes_it_across_rebuild")]
    public async Task A_newer_ordinary_write_logged_before_an_older_prepare_supersedes_it(bool rebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = rebuild ? "r" : "l";
        var tree = $"mv-inverted-{suffix}-src";
        var view = $"mv-inverted-{suffix}-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var key = $"inverted-{suffix}-k";
        var tx = Guid.NewGuid();

        // Replication logs entries in arrival order, not HLC order: a newer
        // ordinary write arrives (and is applied by the view) before an older
        // prepare for the same key, whose commit then loses to it by LWW.
        await apply.ApplySetAsync(key, Person(40, "newer"), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
        await maintainer.DrainAsync();
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "prepared"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
        if (rebuild)
        {
            await maintainer.RebuildAsync();
        }
        else
        {
            await maintainer.DrainAsync();
        }

        await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")), "The source keeps the newer ordinary write over the older prepare.");
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "An older prepare logged after a newer ordinary write must not overwrite it in the view.");
        });
    }
}
