using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for overlapping atomic batches on one source key feeding an
/// aggregation view. The source resolves two batches that both prepared the
/// same key by last-writer-wins on the prepared HLC, whichever commits first.
/// The aggregation flush must agree: a batch that prepared earlier but
/// completes later must not re-group the key, or the group accumulators stay
/// permanently diverged from the source.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewAggregationStagingOverlapOrderTests
{
    private const string Origin = "remote-origin";

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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static double? SumValue(byte[]? bytes) =>
        bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

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

    [Test]
    public async Task An_older_batch_completing_after_a_newer_batch_does_not_regroup_the_source_key()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-agg-overlap-src";
        const string view = "mv-agg-overlap-view";
        const string key = "agg-overlap-k";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new AggregationLatticeViewProjection(AggregationKind.Sum, Team, "v1", valueSelector: Score);
        var definition = new LatticeViewDefinition(view, projection);
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        await source.SetAsync("anchor", Record("blue", 1));
        await DrainToZeroAsync(maintainer);

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync(
            key, Record("red", 10), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Record("blue", 20), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, shard, Hlc(1000), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        await apply.ApplyTxTerminalAsync(older, committed: true, shard, Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Record("blue", 20)),
                "The source resolves overlapping prepares by last-writer-wins on the prepared HLC.");
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(21),
                "The surviving source value must be aggregated into its group.");
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.Null,
                "The batch that completed last but prepared earlier must not re-group the key.");
        });
    }
}