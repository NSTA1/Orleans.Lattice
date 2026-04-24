using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using NUnit.Framework;
using Orleans.Runtime;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Dashboard;

/// <summary>
///
/// Tests the in-process pub/sub hub that bridges
/// <see cref="FederationRouter"/> events to Blazor Server components.
/// </summary>
[TestFixture]
public sealed class DashboardBroadcasterTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public Task SetUp() => (_fixture = new FederationTestClusterFixture()).InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private DashboardBroadcaster NewBroadcaster(FederationRouter router)
    {
        // Unique stream id per broadcaster isolates stream traffic
        // from other tests sharing this [OneTimeSetUp] cluster, so
        // stale pub/sub events from a prior test can't leak into this
        // one. Production code path uses DefaultBroadcastStreamId.
        var streamId = StreamId.Create(
            DashboardBroadcaster.StreamNamespace,
            $"broadcast-{Guid.NewGuid():N}");
        var broadcaster = new DashboardBroadcaster(
            router,
            _fixture.Cluster.Client,
            NullLogger<DashboardBroadcaster>.Instance,
            streamId);
        broadcaster.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return broadcaster;
    }

    [Test]
    public async Task GetInitialPartsAsync_returns_empty_for_fresh_router()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);

        var initial = await broadcaster.GetInitialPartsAsync();

        Assert.That(initial, Is.Empty);
    }

    [Test]
    public async Task FactRouted_pushes_PartSummaryUpdate_to_subscriber()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90001");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);

        // Start MoveNextAsync first so the iterator body runs and registers
        // the channel before we emit. Without this prime, the fact can fan
        // out before the subscription exists and the update is lost.
        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        await router.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge));
        var moved = await moveTask;

        Assert.That(moved, Is.True);
        var update = subscriber.Current;
        Assert.That(update.Serial, Is.EqualTo(serial));
        Assert.That(update.LatestStage, Is.EqualTo(ProcessStage.Forge));
        Assert.That(update.FactCount, Is.EqualTo(1));
        Assert.That(update.Diverges, Is.False);

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task ChaosConfigChanged_pushes_overview_to_subscriber()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribeChaosChanges(cts.Token).GetAsyncEnumerator(cts.Token);

        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        await router.ConfigureSiteAsync(
            ProcessSite.OhioForge,
            new SiteConfig { IsPaused = true, DelayMs = 0, ReorderEnabled = false });
        var moved = await moveTask;

        Assert.That(moved, Is.True);
        Assert.That(subscriber.Current.PausedSites, Is.EqualTo(1));
        Assert.That(subscriber.Current.Any, Is.True);

        await subscriber.DisposeAsync();

        // Clean up so later tests in this fixture see nominal chaos state.
        await router.ConfigureSiteAsync(
            ProcessSite.OhioForge,
            new SiteConfig { IsPaused = false, DelayMs = 0, ReorderEnabled = false });
    }

    [Test]
    public async Task Multiple_subscribers_all_receive_same_update()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90002");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sub1 = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);
        var sub2 = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);

        var move1 = sub1.MoveNextAsync().AsTask();
        var move2 = sub2.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        await router.EmitAsync(Step(serial, tick: 2, ProcessStage.Forge, ProcessSite.OhioForge));
        var moved1 = await move1;
        var moved2 = await move2;

        Assert.That(moved1, Is.True);
        Assert.That(moved2, Is.True);
        Assert.That(sub1.Current.Serial, Is.EqualTo(serial));
        Assert.That(sub2.Current.Serial, Is.EqualTo(serial));

        await sub1.DisposeAsync();
        await sub2.DisposeAsync();
    }

    [Test]
    public async Task Cancelled_subscriber_completes_without_hanging()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);

        using var cts = new CancellationTokenSource();
        var loop = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in broadcaster.SubscribePartUpdates(cts.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        });

        cts.Cancel();

        var completed = await Task.WhenAny(loop, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.That(completed, Is.SameAs(loop), "Subscriber loop did not complete after cancellation.");
    }

    [Test]
    public async Task SubscribeDivergence_emits_event_when_part_first_diverges()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90100");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribeDivergence(cts.Token).GetAsyncEnumerator(cts.Token);
        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        // Sneak a Critical NC into the lattice backend only — baseline stays
        // empty. Then trigger a FactRouted via the router so the broadcaster
        // re-reads both backends, spots the mismatch, and emits a divergence
        // event.
        await lattice.EmitAsync(Nc(serial, tick: 1, ncNumber: "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab), cts.Token);
        await router.EmitAsync(Step(serial, tick: 2, ProcessStage.Forge, ProcessSite.OhioForge));

        var moved = await moveTask;

        Assert.That(moved, Is.True);
        var evt = subscriber.Current;
        Assert.That(evt.Serial, Is.EqualTo(serial));
        Assert.That(evt.LatticeState, Is.EqualTo(ComplianceState.Scrap));
        Assert.That(evt.BaselineState, Is.EqualTo(ComplianceState.Nominal));
        Assert.That(evt.Resolved, Is.False);

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task SubscribeDivergence_emits_resolved_when_backends_reconverge()
    {
        var (router, baseline, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90101");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var subscriber = broadcaster.SubscribeDivergence(cts.Token).GetAsyncEnumerator(cts.Token);

        // Step 1 — drive the part into a divergent state.
        await lattice.EmitAsync(Nc(serial, tick: 1, "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab), cts.Token);
        var move1 = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);
        await router.EmitAsync(Step(serial, tick: 2, ProcessStage.Forge, ProcessSite.OhioForge));
        Assert.That(await move1, Is.True);
        Assert.That(subscriber.Current.Resolved, Is.False);

        // Step 2 — replay the missing NC into the baseline so both backends
        // agree again, then trigger another FactRouted.
        await baseline.EmitAsync(Nc(serial, tick: 1, "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab), cts.Token);
        var move2 = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);
        await router.EmitAsync(Step(serial, tick: 3, ProcessStage.Forge, ProcessSite.OhioForge));
        Assert.That(await move2, Is.True);

        var evt = subscriber.Current;
        Assert.That(evt.Serial, Is.EqualTo(serial));
        Assert.That(evt.Resolved, Is.True);
        Assert.That(evt.BaselineState, Is.EqualTo(evt.LatticeState));

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task GetInitialDivergenceAsync_returns_currently_divergent_parts()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90102");

        // Bury a Critical NC in the lattice backend only.
        await lattice.EmitAsync(Nc(serial, tick: 1, "NC-2", NcSeverity.Critical, ProcessSite.ToulouseNdtLab), CancellationToken.None);

        var initial = await broadcaster.GetInitialDivergenceAsync();

        Assert.That(initial, Has.Count.GreaterThanOrEqualTo(1));
        var row = initial.First(r => r.Serial == serial);
        Assert.That(row.LatticeState, Is.EqualTo(ComplianceState.Scrap));
        Assert.That(row.BaselineState, Is.EqualTo(ComplianceState.Nominal));
        Assert.That(row.Resolved, Is.False);
    }

    [Test]
    public async Task FactReplicated_pushes_PartSummaryUpdate_to_subscriber()
    {
        // The inbound replication endpoint raises FactReplicated after a
        // peer-originated fact has been applied + replayed into the local
        // baseline. The broadcaster must wire that event alongside
        // FactRouted so the peer cluster's dashboard refreshes without
        // polling. BuildSummaryAsync reads facts from the lattice, so
        // for the update to be non-empty we seed the lattice as the
        // real inbound path would (the minimal-API handler SetAsyncs
        // the replicated bytes into the local lattice before raising
        // FactReplicated).
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90200");
        var fact = Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge);

        await lattice.EmitAsync(fact, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);
        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        router.RaiseFactReplicated(fact);
        var moved = await moveTask;

        Assert.That(moved, Is.True);
        var update = subscriber.Current;
        Assert.Multiple(() =>
        {
            Assert.That(update.Serial, Is.EqualTo(serial));
            Assert.That(update.FactCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(update.LatestStage, Is.EqualTo(ProcessStage.Forge));
        });

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task FactRouted_pushes_SiteActivityIndexEntry_to_subscriber()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90300");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribeSiteActivity(cts.Token).GetAsyncEnumerator(cts.Token);

        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        var fact = Step(serial, tick: 10, ProcessStage.Forge, ProcessSite.OhioForge);
        await router.EmitAsync(fact);

        var moved = await moveTask;

        Assert.That(moved, Is.True);
        var entry = subscriber.Current;
        Assert.Multiple(() =>
        {
            Assert.That(entry.Serial, Is.EqualTo(serial));
            Assert.That(entry.Site, Is.EqualTo(ProcessSite.OhioForge));
            Assert.That(entry.Hlc, Is.EqualTo(fact.Hlc));
            Assert.That(entry.Activity, Is.EqualTo($"Step: {ProcessStage.Forge}"));
        });

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task FactReplicated_pushes_SiteActivityIndexEntry_to_subscriber()
    {
        // A peer-originated fact must appear on the activity feed so a
        // user watching a site sub-tab on the receiving cluster sees
        // replicated activity immediately — the whole point of wiring
        // both FactRouted and FactReplicated into the activity fan-out.
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90301");
        var fact = Step(serial, tick: 11, ProcessStage.Forge, ProcessSite.OhioForge);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribeSiteActivity(cts.Token).GetAsyncEnumerator(cts.Token);
        var moveTask = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        router.RaiseFactReplicated(fact);
        var moved = await moveTask;

        Assert.That(moved, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(subscriber.Current.Serial, Is.EqualTo(serial));
            Assert.That(subscriber.Current.Site, Is.EqualTo(ProcessSite.OhioForge));
            Assert.That(subscriber.Current.Activity, Is.EqualTo($"Step: {ProcessStage.Forge}"));
        });

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task SubscribeSiteActivity_fans_out_to_multiple_subscribers()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90302");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sub1 = broadcaster.SubscribeSiteActivity(cts.Token).GetAsyncEnumerator(cts.Token);
        var sub2 = broadcaster.SubscribeSiteActivity(cts.Token).GetAsyncEnumerator(cts.Token);

        var move1 = sub1.MoveNextAsync().AsTask();
        var move2 = sub2.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        await router.EmitAsync(Step(serial, tick: 12, ProcessStage.Forge, ProcessSite.OhioForge));

        Assert.That(await move1, Is.True);
        Assert.That(await move2, Is.True);
        Assert.That(sub1.Current.Serial, Is.EqualTo(serial));
        Assert.That(sub2.Current.Serial, Is.EqualTo(serial));

        await sub1.DisposeAsync();
        await sub2.DisposeAsync();
    }

    [Test]
    public async Task SubscribeSiteActivity_cancellation_completes_loop()
    {
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router);

        using var cts = new CancellationTokenSource();
        var loop = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in broadcaster.SubscribeSiteActivity(cts.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        });

        cts.Cancel();

        var completed = await Task.WhenAny(loop, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.That(completed, Is.SameAs(loop), "Site-activity subscriber loop did not complete after cancellation.");
    }

    /// <summary>
    /// Cross-silo fan-out: a fact routed through broadcaster A's
    /// router must reach a subscriber attached to broadcaster B when
    /// both broadcasters share the same stream id. This is the actual
    /// mechanism that lets a Blazor circuit pinned to silo B see a
    /// fact that landed on silo A — broadcaster A publishes to the
    /// cluster-wide stream, broadcaster B (subscribed to the same
    /// stream) fans it out into its local channels.
    /// </summary>
    [Test]
    public async Task Stream_publish_on_one_broadcaster_fans_out_on_another()
    {
        var sharedStreamId = Orleans.Runtime.StreamId.Create(
            DashboardBroadcaster.StreamNamespace,
            $"broadcast-shared-{Guid.NewGuid():N}");

        var (routerA, _, _) = _fixture.NewRouter();
        var (routerB, _, _) = _fixture.NewRouter();

        await using var broadcasterA = new DashboardBroadcaster(
            routerA, _fixture.Cluster.Client, NullLogger<DashboardBroadcaster>.Instance, sharedStreamId);
        await broadcasterA.StartAsync(CancellationToken.None);

        await using var broadcasterB = new DashboardBroadcaster(
            routerB, _fixture.Cluster.Client, NullLogger<DashboardBroadcaster>.Instance, sharedStreamId);
        await broadcasterB.StartAsync(CancellationToken.None);

        var serial = new PartSerialNumber("HPT-BLD-XSILO-91001");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        // Subscriber attached ONLY to broadcaster B.
        var subscriberB = broadcasterB.SubscribeSiteActivity(cts.Token).GetAsyncEnumerator(cts.Token);
        var moveTask = subscriberB.MoveNextAsync().AsTask();
        await Task.Delay(100, cts.Token);

        // Fact routes through routerA → broadcasterA publishes to the
        // stream → broadcasterB receives and fans out to its local
        // activity subscribers. The subscriber attached to B must see it.
        await routerA.EmitAsync(Step(serial, tick: 13, ProcessStage.Forge, ProcessSite.OhioForge));

        var moved = await moveTask;

        Assert.That(moved, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(subscriberB.Current.Serial, Is.EqualTo(serial));
            Assert.That(subscriberB.Current.Site, Is.EqualTo(ProcessSite.OhioForge));
            Assert.That(subscriberB.Current.Activity, Is.EqualTo($"Step: {ProcessStage.Forge}"));
        });

        await subscriberB.DisposeAsync();
    }
}
