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
/// Covers the scan-storm mitigations retained on <see cref="DashboardBroadcaster"/>
/// (issue #1038): per-part rebuild coalescing, the watcher-gated dirty mark, and
/// the short-TTL initial-snapshot cache that backs the snapshot's part directory
/// (now sourced from the library-maintained folded compliance view).
/// </summary>
[TestFixture]
public sealed class DashboardBroadcasterCoalescingTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public Task SetUp() => (_fixture = new FederationTestClusterFixture()).InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private DashboardBroadcaster NewBroadcaster(
        FederationRouter router,
        TimeSpan rebuildInterval,
        TimeSpan? snapshotCacheTtl = null)
    {
        var streamId = StreamId.Create(
            DashboardBroadcaster.StreamNamespace,
            $"broadcast-{Guid.NewGuid():N}");
        var broadcaster = new DashboardBroadcaster(
            router,
            _fixture.Cluster.Client,
            _fixture.NewPartCrdtStore(),
            _fixture.ViewFactory,
            NullLogger<DashboardBroadcaster>.Instance,
            streamId,
            partRebuildInterval: rebuildInterval,
            snapshotCacheTtl: snapshotCacheTtl);
        broadcaster.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return broadcaster;
    }

    [Test]
    public async Task Burst_of_facts_for_same_part_coalesces_into_fewer_rebuilds()
    {
        var window = TimeSpan.FromMilliseconds(200);
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router, window);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91000");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var subscriber = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);

        // Prime the iterator so its channel is registered before we emit.
        var move = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        // Emit a burst of five facts for the same part well within one window.
        for (var tick = 1; tick <= 5; tick++)
        {
            await router.EmitAsync(Step(serial, tick, ProcessStage.Forge, ProcessSite.OhioForge));
        }

        var updates = new List<PartSummaryUpdate>();
        Assert.That(await move, Is.True);
        updates.Add(subscriber.Current);

        // Drain any further coalesced rebuilds until the feed goes quiet.
        while (true)
        {
            var next = subscriber.MoveNextAsync().AsTask();
            var winner = await Task.WhenAny(next, Task.Delay(window * 2, cts.Token));
            if (winner != next)
            {
                // Release the in-flight MoveNextAsync before disposing - an
                // async enumerator cannot be disposed while a move is pending.
                cts.Cancel();
                try
                {
                    await next;
                }
                catch (OperationCanceledException)
                {
                    // Expected once the subscription token is cancelled.
                }
                break;
            }
            Assert.That(await next, Is.True);
            updates.Add(subscriber.Current);
        }

        Assert.Multiple(() =>
        {
            // Five facts collapse into far fewer than five rebuilds.
            Assert.That(updates, Has.Count.LessThan(5));
            // The final coalesced summary reflects every fact in the burst.
            Assert.That(updates[^1].Serial, Is.EqualTo(serial));
            Assert.That(updates[^1].FactCount, Is.EqualTo(5));
        });

        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task MarkPartDirty_is_gated_on_watchers_and_coalesces_per_serial()
    {
        // A very long window guarantees the background loop never drains during
        // the test, so PendingRebuildCount reflects exactly what marking did.
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router, TimeSpan.FromMinutes(10));
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91100");

        // No subscriber attached: the mark is a no-op. The sample no longer owns
        // a durable summary tree to keep warm - the library view is maintained
        // off the WAL - so there is nothing to fold when no circuit is watching.
        broadcaster.MarkPartDirtyForTest(serial);
        Assert.That(broadcaster.PendingRebuildCount, Is.EqualTo(0),
            "with no watcher attached, marking must not queue a rebuild");

        // Attach + prime a part subscriber so HasPartWatchers is true.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriber = broadcaster.SubscribePartUpdates(cts.Token).GetAsyncEnumerator(cts.Token);
        var move = subscriber.MoveNextAsync().AsTask();
        await Task.Delay(50, cts.Token);

        broadcaster.MarkPartDirtyForTest(serial);
        Assert.That(broadcaster.PendingRebuildCount, Is.EqualTo(1),
            "with a watcher attached, a mark queues a rebuild");

        // Repeated marks for the same serial coalesce to a single queued entry.
        broadcaster.MarkPartDirtyForTest(serial);
        broadcaster.MarkPartDirtyForTest(serial);
        Assert.That(broadcaster.PendingRebuildCount, Is.EqualTo(1),
            "repeated marks for one serial coalesce to a single rebuild");

        cts.Cancel();
        try
        {
            await move;
        }
        catch (OperationCanceledException)
        {
            // Expected once the subscription token is cancelled.
        }
        await subscriber.DisposeAsync();
    }

    [Test]
    public async Task GetInitialPartsAsync_is_memoised_within_ttl()
    {
        // A long TTL makes the memoisation deterministic regardless of build
        // latency: the second call must reuse the first build.
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(
            router, TimeSpan.FromMinutes(10), snapshotCacheTtl: TimeSpan.FromMinutes(10));
        var first = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(80000, 84999)}");
        var second = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(85000, 89999)}");

        // The snapshot reads the library-maintained folded view over the default
        // tree, so seed parts there and wait for the maintainer to catch up.
        var defaultTree = _fixture.NewLatticeBackendOverDefaultTree();
        var view = await _fixture.ViewFactory.GetAsync(ComplianceFoldProjection.ViewName);
        Assert.That(view, Is.Not.Null);

        await defaultTree.EmitAsync(Step(first, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        await view!.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        // Prime the cache with a snapshot that predates the second part.
        var seeded = await broadcaster.GetInitialPartsAsync();
        Assert.That(seeded.Any(p => p.Serial == first), Is.True);

        // Materialise the second part into the view after the cache was primed.
        await defaultTree.EmitAsync(Step(second, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        await view.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        // Within the TTL the memoised snapshot is returned, so the just-added
        // part is not yet visible.
        var cached = await broadcaster.GetInitialPartsAsync();
        Assert.That(cached.Any(p => p.Serial == second), Is.False,
            "snapshot within the TTL window should be served from the memoised build");
    }

    [Test]
    public async Task GetInitialPartsAsync_rebuilds_after_ttl_expires()
    {
        // A zero TTL forces every call to rebuild, so the snapshot always
        // reflects the latest materialised-view state.
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(
            router, TimeSpan.FromMinutes(10), snapshotCacheTtl: TimeSpan.Zero);
        var first = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(75000, 77499)}");
        var second = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(77500, 79999)}");

        var defaultTree = _fixture.NewLatticeBackendOverDefaultTree();
        var view = await _fixture.ViewFactory.GetAsync(ComplianceFoldProjection.ViewName);
        Assert.That(view, Is.Not.Null);

        await defaultTree.EmitAsync(Step(first, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        await view!.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));
        var seeded = await broadcaster.GetInitialPartsAsync();
        Assert.That(seeded.Any(p => p.Serial == first), Is.True);

        await defaultTree.EmitAsync(Step(second, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        await view.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        var refreshed = await broadcaster.GetInitialPartsAsync();
        Assert.That(refreshed.Any(p => p.Serial == second), Is.True,
            "a rebuilt snapshot should include parts materialised since the previous build");
    }
}
