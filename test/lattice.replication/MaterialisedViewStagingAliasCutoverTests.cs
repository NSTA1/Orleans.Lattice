using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Atomic staging is bound to the physical WAL the maintainer reads. When the
/// source's alias is cut over to a new physical tree, a batch still staged
/// against the retired log can never complete there: the rebind must discard it
/// (the rebuild from the new physical source is authoritative), release the
/// blocked-floor pin and the cursor on the retired log, and never surface the
/// orphaned prepare.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingAliasCutoverTests
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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public async Task A_batch_staged_on_the_retired_log_is_discarded_and_unpinned_by_an_alias_cutover()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string logical = "mv-stage-cutover-src";
        const string shadow = "mv-stage-cutover-restored";
        const string view = "mv-stage-cutover-view";
        const string consumerId = "view:" + view;

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        var latticeView = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var cursors = _fixture.SiloServices.GetRequiredService<IWalCursorRegistry>();

        await source.SetAsync("plain", Person(30, "orig"));
        await _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(logical).ApplyPreparedSetAsync(
            "staged", Person(40, "orphan"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0,
            Guid.NewGuid(), atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();
        Assert.That(await cursors.GetBlockedFloorAsync(logical), Is.Not.Null,
            "precondition: the staged prepare pins the retired log");

        var shadowTree = _fixture.Cluster.Client.GetGrain<ILattice>(shadow);
        await shadowTree.SetAsync("plain", Person(31, "restored"));
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(logical, shadow);

        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0
                && await latticeView.GetAsync("plain") is { } v && v.SequenceEqual(Person(31, "restored")))
            {
                break;
            }

            await Task.Delay(20);
        }

        // A write on the new log forces a full drain pass against it, which
        // would report and hold back on any batch still staged from the
        // retired log.
        await shadowTree.SetAsync("after", Person(50, "postcutover"));
        for (var attempt = 0; attempt < 50 && (await maintainer.GetLagAsync() > 0 || await latticeView.GetAsync("after") is null); attempt++)
        {
            await maintainer.DrainAsync();
            await Task.Delay(20);
        }

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("plain"), Is.EqualTo(Person(31, "restored")),
                "The view must converge on the new physical source.");
            Assert.That(await latticeView.GetAsync("staged"), Is.Null,
                "A prepare staged on the retired log must never surface after the cutover.");
            Assert.That(await cursors.GetBlockedFloorAsync(logical), Is.Null,
                "The cutover must release the staging pin on the retired log.");
            Assert.That((await cursors.SnapshotAsync(logical)).Any(c => c.ConsumerId == consumerId), Is.False,
                "No cursor may remain on the retired log.");
            Assert.That(await cursors.GetBlockedFloorAsync(shadow), Is.Null,
                "Nothing is staged against the new log.");
            Assert.That(await latticeView.GetAsync("after"), Is.EqualTo(Person(50, "postcutover")),
                "Writes on the new log must reach the view.");
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0));
        });
    }
}
