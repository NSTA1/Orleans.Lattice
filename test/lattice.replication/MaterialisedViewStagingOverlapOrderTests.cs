using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for overlapping atomic batches on one key. The source tree
/// resolves two batches that both prepared the same key by last-writer-wins
/// on the prepared entry's HLC, whichever commits first. The view must agree:
/// flushing completed batches in completion-discovery order, stamped with the
/// view tree's own clock, would let the older batch overwrite the newer one
/// and leave the view permanently diverged from its source.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingOverlapOrderTests
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

    [Test]
    public async Task An_older_batch_completing_after_a_newer_batch_on_the_same_key_does_not_overwrite_it()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-overlap-src";
        const string view = "mv-atomic-overlap-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "overlap-k";
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        // Both batches prepare the same key; the newer one carries the higher
        // prepared HLC and commits first.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        // The older batch commits later, in a later drain pass.
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")),
                "The source resolves overlapping prepares by last-writer-wins on the prepared HLC.");
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "The view must hold the source's surviving value, not the batch that completed last.");
        });
    }

    [Test]
    public async Task Overlapping_batches_completing_in_one_drain_pass_resolve_by_prepared_hlc()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-overlap-pass-src";
        const string view = "mv-atomic-overlap-pass-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "overlap-pass-k";
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "Batches completing in one pass must resolve by prepared HLC, not dictionary order.");
        });
    }
}
