using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Overlapping atomic batches whose prepared HLCs are ordered against their
/// arrival in the commit log: the newer batch (higher prepared HLC) is logged
/// first and the older one second. The source resolves the shared key by
/// last-writer-wins on the prepared HLC whatever the arrival or commit order, so
/// the view must hold the projection of that survivor. Covers set, delete and
/// filter-exit outcomes, every commit order including an abort, and the
/// interactions with a re-keyed projection, a mid-flight rebuild, an ordinary
/// range delete logged between the prepares and their terminals, and
/// aggregation membership and retraction.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingReversedOverlapTests
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

    public enum NewerOutcome
    {
        Set,
        Delete,
        FilterExit,
    }

    public enum OlderOutcome
    {
        Set,
        Delete,
    }

    public enum CommitOrder
    {
        NewerFirst,
        OlderFirst,
        SameDrain,
        OlderAborted,
    }

    public enum Interaction
    {
        None,
        Rekey,
        Rebuild,
        RangeBetween,
        NewerSetLoggedFirst,
        NewerDeleteLoggedFirst,
        NewerRangeLoggedFirst,
    }

    private sealed record ViewPerson(int Age, string Tag);

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static byte[] Score(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private async Task DrainToZeroAsync(string view)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{view}' did not catch up to the source head.");
    }

    private Task DrainOnceAsync(string view) =>
        _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).DrainAsync();

    private async Task CreateAsync(ILattice source, string view, LatticeViewDefinition definition)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        await _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();
    }

    private static int Shards(params string[] keys) => keys.Select(ShardOf).Distinct().Count();

    private static async Task CommitAsync(IReplicationApplyGrain apply, Guid tx, bool committed, long hlc, string origin, params string[] keys)
    {
        var shardCount = Shards(keys);
        foreach (var shard in keys.Select(ShardOf).Distinct())
        {
            await apply.ApplyTxTerminalAsync(tx, committed, shard, Hlc(hlc), origin, atomicShardCount: shardCount);
        }
    }

    private async Task TerminateAsync(
        string view,
        CommitOrder order,
        Func<Task> newerTerminal,
        Func<Task> olderTerminal,
        Func<Task>? midway)
    {
        switch (order)
        {
            case CommitOrder.NewerFirst:
                await newerTerminal();
                await DrainOnceAsync(view);
                if (midway is not null)
                {
                    await midway();
                }

                await olderTerminal();
                break;
            case CommitOrder.OlderFirst:
            case CommitOrder.OlderAborted:
                await olderTerminal();
                await DrainOnceAsync(view);
                if (midway is not null)
                {
                    await midway();
                }

                await newerTerminal();
                break;
            case CommitOrder.SameDrain:
                if (midway is not null)
                {
                    await midway();
                }

                await newerTerminal();
                await olderTerminal();
                break;
        }

        await DrainToZeroAsync(view);
    }

    [Test]
    public async Task Reversed_overlapping_batches_leave_the_view_on_the_source_survivor(
        [Values] NewerOutcome newer,
        [Values] OlderOutcome older,
        [Values] CommitOrder order,
        [Values] Interaction interaction)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = $"{(int)newer}{(int)older}{(int)order}{(int)interaction}";
        var tree = $"mv-rov-{suffix}-src";
        var view = $"mv-rov-{suffix}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var rekey = interaction == Interaction.Rekey;
        Func<string, string> viewKey = rekey ? static k => "v/" + k : static k => k;
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18),
            keySelector: rekey ? viewKey : null,
            keySelectorVersion: rekey ? "v1" : null);
        await CreateAsync(source, view, new LatticeViewDefinition(view, projection));

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var key = $"rov-{suffix}-k";
        var olderOther = $"rov-{suffix}-m";
        var newerOther = $"rov-{suffix}-n";
        var sibling = $"rov-{suffix}-l";
        var rangeEnd = $"rov-{suffix}-z";

        // A prior value under the shared key so a delete outcome is visible, and
        // an older sibling a range delete supersedes.
        await apply.ApplySetAsync(key, Person(60, "prior"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplySetAsync(sibling, Person(50, "sibling"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        // An ordinary write newer than both batches, logged before either
        // prepare: the source keeps it whatever the batches do, and neither batch
        // may replace it in the view even though both are staged over one key.
        switch (interaction)
        {
            case Interaction.NewerSetLoggedFirst:
                await apply.ApplySetAsync(key, Person(70, "ordinary"), Hlc(400), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
                break;
            case Interaction.NewerDeleteLoggedFirst:
                await apply.ApplyDeleteAsync(key, Hlc(400), OtherOrigin, sourceVectorClock: null);
                break;
            case Interaction.NewerRangeLoggedFirst:
                await apply.ApplyDeleteRangeAsync(key, rangeEnd, Hlc(400), OtherOrigin, sourceVectorClock: null);
                break;
        }

        await DrainToZeroAsync(view);

        var newerTx = Guid.NewGuid();
        var olderTx = Guid.NewGuid();

        // The newer batch (prepared HLC 300) is logged first.
        switch (newer)
        {
            case NewerOutcome.Delete:
                await apply.ApplyPreparedDeleteAsync(key, Hlc(300), OtherOrigin, sourceVectorClock: null, newerTx, atomicBatchSize: 2, atomicBatchIndex: 0);
                break;
            default:
                await apply.ApplyPreparedSetAsync(
                    key, newer == NewerOutcome.Set ? Person(40, "newer") : Person(10, "minor"), Hlc(300), OtherOrigin,
                    sourceVectorClock: null, expiresAtTicks: 0, newerTx, atomicBatchSize: 2, atomicBatchIndex: 0);
                break;
        }

        await apply.ApplyPreparedSetAsync(
            newerOther, Person(41, "newer-other"), Hlc(300), OtherOrigin,
            sourceVectorClock: null, expiresAtTicks: 0, newerTx, atomicBatchSize: 2, atomicBatchIndex: 1);

        // The older batch (prepared HLC 100) is logged second.
        if (older == OlderOutcome.Delete)
        {
            await apply.ApplyPreparedDeleteAsync(key, Hlc(100), Origin, sourceVectorClock: null, olderTx, atomicBatchSize: 2, atomicBatchIndex: 0);
        }
        else
        {
            await apply.ApplyPreparedSetAsync(
                key, Person(30, "older"), Hlc(100), Origin,
                sourceVectorClock: null, expiresAtTicks: 0, olderTx, atomicBatchSize: 2, atomicBatchIndex: 0);
        }

        await apply.ApplyPreparedSetAsync(
            olderOther, Person(31, "older-other"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, olderTx, atomicBatchSize: 2, atomicBatchIndex: 1);
        await DrainOnceAsync(view);

        if (interaction == Interaction.RangeBetween)
        {
            // An ordinary range delete between the two prepared HLCs, logged
            // after both prepares and before either terminal.
            await apply.ApplyDeleteRangeAsync(key, rangeEnd, Hlc(200), Origin, sourceVectorClock: null);
            await DrainOnceAsync(view);
        }

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await TerminateAsync(
            view,
            order,
            () => CommitAsync(apply, newerTx, committed: true, 1000, OtherOrigin, key, newerOther),
            () => CommitAsync(apply, olderTx, committed: order != CommitOrder.OlderAborted, 1001, Origin, key, olderOther),
            interaction == Interaction.Rebuild ? () => maintainer.RebuildAsync() : null);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        var sourceKey = await source.GetAsync(key);
        var expectedSourceKey = interaction switch
        {
            Interaction.NewerSetLoggedFirst => Person(70, "ordinary"),
            Interaction.NewerDeleteLoggedFirst or Interaction.NewerRangeLoggedFirst => null,
            _ => newer switch
            {
                NewerOutcome.Set => Person(40, "newer"),
                NewerOutcome.FilterExit => Person(10, "minor"),
                _ => null,
            },
        };
        var expectedViewKey = expectedSourceKey is not null
            && JsonLatticeSerializer<ViewPerson>.Default.Deserialize(expectedSourceKey)!.Age >= 18
                ? expectedSourceKey
                : null;
        // An ordinary range delete does not tombstone a key that is only prepared
        // when the range applies: the source installs the older batch's other key
        // when it commits, even though the range carries the higher HLC. The view
        // must agree with that source outcome.
        var olderOtherSurvives = order != CommitOrder.OlderAborted;
        var siblingSurvives = interaction is not (Interaction.RangeBetween or Interaction.NewerRangeLoggedFirst);
        var sourceOlderOther = await source.GetAsync(olderOther);
        var sourceSibling = await source.GetAsync(sibling);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceKey, Is.EqualTo(expectedSourceKey),
                "The source resolves the shared key by last-writer-wins on the prepared HLC.");
            Assert.That(await viewTree.GetAsync(viewKey(key)), Is.EqualTo(expectedViewKey),
                "The view must hold the projection of the source survivor, not the batch logged or committed last.");
            Assert.That(await viewTree.GetAsync(viewKey(newerOther)), Is.EqualTo(Person(41, "newer-other")),
                "The newer batch's other key is committed.");
            Assert.That(sourceOlderOther is not null, Is.EqualTo(olderOtherSurvives), "Source state of the older batch's other key.");
            Assert.That(await viewTree.GetAsync(viewKey(olderOther)) is not null, Is.EqualTo(olderOtherSurvives),
                "The view agrees with the source on the older batch's other key.");
            Assert.That(sourceSibling is not null, Is.EqualTo(siblingSurvives), "Source state of the older sibling.");
            Assert.That(await viewTree.GetAsync(viewKey(sibling)) is not null, Is.EqualTo(siblingSurvives),
                "The view agrees with the source on the older sibling.");
        });
    }

    [Test]
    public async Task Reversed_overlapping_batches_leave_aggregation_membership_on_the_source_survivor(
        [Values] OlderOutcome older,
        [Values] CommitOrder order,
        [Values] bool rebuild,
        [Values] bool newerOrdinaryLoggedFirst)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = $"{(int)older}{(int)order}{(rebuild ? "b" : "n")}{(newerOrdinaryLoggedFirst ? "o" : "p")}";
        var tree = $"mv-rov-agg-{suffix}-src";
        var view = $"mv-rov-agg-{suffix}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new AggregationLatticeViewProjection(
            AggregationKind.Sum,
            static v => JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(v)!.Team,
            "v1",
            valueSelector: static v => JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(v)!.Score);
        await CreateAsync(source, view, new LatticeViewDefinition(view, projection));

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var key = $"rov-agg-{suffix}-k";
        await apply.ApplySetAsync(key, Score("green", 3), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);
        if (newerOrdinaryLoggedFirst)
        {
            // An ordinary write newer than both batches, logged before either
            // prepare: it keeps the source key whatever the batches do.
            await apply.ApplySetAsync(key, Score("yellow", 9), Hlc(400), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
            await DrainToZeroAsync(view);
        }

        var newerTx = Guid.NewGuid();
        var olderTx = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync(
            key, Score("blue", 7), Hlc(300), OtherOrigin,
            sourceVectorClock: null, expiresAtTicks: 0, newerTx, atomicBatchSize: 1, atomicBatchIndex: 0);
        if (older == OlderOutcome.Delete)
        {
            await apply.ApplyPreparedDeleteAsync(key, Hlc(100), Origin, sourceVectorClock: null, olderTx, atomicBatchSize: 1, atomicBatchIndex: 0);
        }
        else
        {
            await apply.ApplyPreparedSetAsync(
                key, Score("red", 5), Hlc(100), Origin,
                sourceVectorClock: null, expiresAtTicks: 0, olderTx, atomicBatchSize: 1, atomicBatchIndex: 0);
        }

        await DrainOnceAsync(view);

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await TerminateAsync(
            view,
            order,
            () => CommitAsync(apply, newerTx, committed: true, 1000, OtherOrigin, key),
            () => CommitAsync(apply, olderTx, committed: order != CommitOrder.OlderAborted, 1001, Origin, key),
            rebuild ? () => maintainer.RebuildAsync() : null);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        static double? Decode(byte[]? bytes) => bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);
        var blue = Decode(await viewTree.GetAsync("blue"));
        var red = Decode(await viewTree.GetAsync("red"));
        var green = Decode(await viewTree.GetAsync("green"));
        var yellow = Decode(await viewTree.GetAsync("yellow"));
        var sourceValue = await source.GetAsync(key);
        await Assert.MultipleAsync(() =>
        {
            Assert.That(sourceValue, Is.EqualTo(newerOrdinaryLoggedFirst ? Score("yellow", 9) : Score("blue", 7)),
                "The source keeps the last writer.");
            Assert.That(yellow ?? 0, Is.EqualTo(newerOrdinaryLoggedFirst ? 9 : 0), "The newer ordinary write's membership is kept.");
            Assert.That(blue ?? 0, Is.EqualTo(newerOrdinaryLoggedFirst ? 0 : 7), "The newer batch's membership is counted exactly once.");
            Assert.That(red ?? 0, Is.EqualTo(0), "The older batch must not add a stale membership.");
            Assert.That(green ?? 0, Is.EqualTo(0), "The prior membership is retracted exactly once.");
            return Task.CompletedTask;
        });
    }
}