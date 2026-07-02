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
/// Covers the background fact-tree reconciliation retained on
/// <see cref="DashboardBroadcaster"/> (issue #1048): parts written directly to
/// the fact tree - bypassing <see cref="FederationRouter"/>, so no
/// <c>FactRouted</c> event ever marks them dirty - must still be fanned out live
/// to an attached dashboard, at a bounded rate, and only while a dashboard is
/// attached. The sample no longer owns a summary read model, so reconciliation
/// now diffs the tree against the set of already-fanned-out serials rather than a
/// materialised view.
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
            _fixture.ViewFactory,
            NullLogger<DashboardBroadcaster>.Instance,
            streamId,
            partRebuildInterval: rebuildInterval ?? TimeSpan.FromMinutes(10),
            snapshotCacheTtl: TimeSpan.Zero,
            reconcileBudget: reconcileBudget);
        broadcaster.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return broadcaster;
    }

    /// <summary>
    /// Attaches and primes a part subscriber so <c>HasPartWatchers</c> is true -
    /// reconciliation's dirty mark is gated on an attached dashboard. Returns the
    /// live enumerator plus its cancellation source so the caller can tear it
    /// down; the caller must keep both alive for the duration of the test.
    /// </summary>
    private static async Task<(IAsyncEnumerator<PartSummaryUpdate> Enumerator, Task<bool> Move, CancellationTokenSource Cts)>
        AttachPartWatcherAsync(DashboardBroadcaster broadcaster)
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var enumerator = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);
        var move = enumerator.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);
        return (enumerator, move, cts);
    }

    private static async Task DetachWatcherAsync(
        (IAsyncEnumerator<PartSummaryUpdate> Enumerator, Task<bool> Move, CancellationTokenSource Cts) watcher)
    {
        watcher.Cts.Cancel();
        try
        {
            await watcher.Move;
        }
        catch (OperationCanceledException)
        {
            // Expected once the subscription token is cancelled.
        }
        await watcher.Enumerator.DisposeAsync();
        watcher.Cts.Dispose();
    }

    [Test]
    public async Task Reconcile_fans_out_a_directly_written_part_live()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92000");
        var watcher = await AttachPartWatcherAsync(broadcaster);

        // Write straight to the tree, exactly as the seed tool does - no router,
        // so no FactRouted fires and nothing marks the serial dirty.
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);

        // The broadcaster has never fanned the part out.
        Assert.That(broadcaster.FannedOutPartsForTest, Does.Not.Contain(serial),
            "a directly-written part must be absent from the fanned-out set until reconciliation runs");

        // One reconciliation pass discovers it from the tree and fans it out.
        var discovered = await broadcaster.ReconcileViewWithTreeForTestAsync();
        Assert.That(discovered, Is.EqualTo(1));

        Assert.That(broadcaster.FannedOutPartsForTest, Does.Contain(serial),
            "reconciliation must fan out non-routed writes to the live dashboard");

        await DetachWatcherAsync(watcher);
    }

    [Test]
    public async Task Reconcile_ignores_parts_already_fanned_out()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92100");
        var watcher = await AttachPartWatcherAsync(broadcaster);

        // Fan the part out the normal (stream) way first.
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(serial);
        await broadcaster.DrainDirtyForTestAsync();
        Assert.That(broadcaster.FannedOutPartsForTest, Does.Contain(serial));

        // A reconciliation pass should find nothing new to do.
        var discovered = await broadcaster.ReconcileViewWithTreeForTestAsync();
        Assert.That(discovered, Is.EqualTo(0),
            "reconciliation must not re-queue parts already fanned out live");

        await DetachWatcherAsync(watcher);
    }

    [Test]
    public async Task Reconcile_marks_nothing_without_an_attached_watcher()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92150");

        // A part exists in the tree, but no dashboard is attached - the mark is
        // gated, so reconciliation queues nothing (there is no sample tree to
        // keep warm).
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);

        var discovered = await broadcaster.ReconcileViewWithTreeForTestAsync();

        Assert.That(discovered, Is.EqualTo(0),
            "with no watcher attached, reconciliation must not queue any rebuild");
        Assert.That(broadcaster.FannedOutPartsForTest, Does.Not.Contain(serial));
    }

    [Test]
    public async Task Reconcile_budget_caps_parts_discovered_per_pass_and_converges()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router, reconcileBudget: 2);
        var watcher = await AttachPartWatcherAsync(broadcaster);

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

        // Now fully converged: nothing left to discover and every part fanned out.
        Assert.That(await broadcaster.ReconcileViewWithTreeForTestAsync(), Is.EqualTo(0));

        foreach (var serial in serials)
        {
            Assert.That(broadcaster.FannedOutPartsForTest, Does.Contain(serial),
                $"every seeded part must be fanned out once reconciliation converges ({serial.Value})");
        }

        await DetachWatcherAsync(watcher);
    }
}
