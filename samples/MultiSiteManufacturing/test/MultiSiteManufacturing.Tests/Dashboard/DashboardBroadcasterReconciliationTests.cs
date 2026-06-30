using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Tests.Federation;
using NUnit.Framework;
using Orleans.Runtime;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Dashboard;

/// <summary>
/// Covers the background view-vs-tree reconciliation added to
/// <see cref="DashboardBroadcaster"/> (issue #1048): parts written directly to
/// the fact tree - bypassing <see cref="FederationRouter"/>, so no
/// <c>FactRouted</c> event ever marks them dirty - must still converge into the
/// materialised <see cref="MultiSiteManufacturing.Host.Lattice.PartSummaryView"/>
/// that the dashboard snapshot reads, at a bounded rate, and only while a
/// dashboard is attached.
/// </summary>
[TestFixture]
public sealed class DashboardBroadcasterReconciliationTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public Task SetUp() => (_fixture = new FederationTestClusterFixture()).InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private DashboardBroadcaster NewBroadcaster(
        FederationRouter router,
        TimeSpan? rebuildInterval = null,
        int? reconcileBudget = null)
    {
        var streamId = StreamId.Create(
            DashboardBroadcaster.StreamNamespace,
            $"broadcast-{Guid.NewGuid():N}");
        // A long rebuild interval keeps the background loop from racing the
        // explicit ReconcileViewWithTreeForTestAsync drive; reconciliation is
        // exercised deterministically through the test seam.
        var broadcaster = new DashboardBroadcaster(
            router,
            _fixture.Cluster.Client,
            _fixture.NewPartCrdtStore(),
            NullLogger<DashboardBroadcaster>.Instance,
            streamId,
            partRebuildInterval: rebuildInterval ?? TimeSpan.FromMinutes(10),
            snapshotCacheTtl: TimeSpan.Zero,
            reconcileBudget: reconcileBudget);
        broadcaster.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return broadcaster;
    }

    [Test]
    public async Task Reconcile_folds_directly_written_part_into_the_view()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92000");

        // Write straight to the tree, exactly as the seed tool does - no router,
        // so no FactRouted fires and nothing marks the serial dirty.
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);

        // The view (and therefore the snapshot) has never heard of the part.
        var before = await broadcaster.GetInitialPartsAsync();
        Assert.That(before.Any(p => p.Serial == serial), Is.False,
            "a directly-written part must be absent from the view until reconciliation runs");

        // One reconciliation pass discovers it from the tree and folds it in.
        var discovered = await broadcaster.ReconcileViewWithTreeForTestAsync();
        Assert.That(discovered, Is.EqualTo(1));

        var after = await broadcaster.GetInitialPartsAsync();
        Assert.That(after.Any(p => p.Serial == serial), Is.True,
            "reconciliation must converge the view to tree truth for non-routed writes");
    }

    [Test]
    public async Task Reconcile_ignores_parts_already_materialised_in_the_view()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92100");

        // Fold the part into the view the normal (stream) way first.
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(serial);
        await broadcaster.DrainDirtyForTestAsync();

        // A reconciliation pass should find nothing new to do.
        var discovered = await broadcaster.ReconcileViewWithTreeForTestAsync();
        Assert.That(discovered, Is.EqualTo(0),
            "reconciliation must not re-queue parts already present in the view");
    }

    [Test]
    public async Task Reconcile_budget_caps_parts_discovered_per_pass_and_converges()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router, reconcileBudget: 2);

        // Five distinct parts written directly to the tree.
        var serials = new[]
        {
            new PartSerialNumber("HPT-BLD-S1-2028-92200"),
            new PartSerialNumber("HPT-BLD-S1-2028-92201"),
            new PartSerialNumber("HPT-BLD-S1-2028-92202"),
            new PartSerialNumber("HPT-BLD-S1-2028-92203"),
            new PartSerialNumber("HPT-BLD-S1-2028-92204"),
        };
        foreach (var serial in serials)
        {
            await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        }

        // Each pass discovers at most the budget (2), so the backfill spreads
        // across passes instead of folding all five at once.
        Assert.That(await broadcaster.ReconcileViewWithTreeForTestAsync(), Is.EqualTo(2));
        Assert.That(await broadcaster.ReconcileViewWithTreeForTestAsync(), Is.EqualTo(2));
        Assert.That(await broadcaster.ReconcileViewWithTreeForTestAsync(), Is.EqualTo(1));

        // Now fully converged: nothing left to discover and every part visible.
        Assert.That(await broadcaster.ReconcileViewWithTreeForTestAsync(), Is.EqualTo(0));

        var snapshot = await broadcaster.GetInitialPartsAsync();
        foreach (var serial in serials)
        {
            Assert.That(snapshot.Any(p => p.Serial == serial), Is.True,
                $"every seeded part must be visible once reconciliation converges ({serial.Value})");
        }
    }
}
