using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Tests.Federation;
using NUnit.Framework;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Dashboard;

/// <summary>
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
        var broadcaster = new DashboardBroadcaster(router, NullLogger<DashboardBroadcaster>.Instance);
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
}
