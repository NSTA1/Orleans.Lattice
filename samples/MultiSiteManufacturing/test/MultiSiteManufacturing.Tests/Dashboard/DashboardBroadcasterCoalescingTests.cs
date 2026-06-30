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
/// Covers the scan-storm mitigations added to <see cref="DashboardBroadcaster"/>
/// (issue #1038): per-part rebuild coalescing, the no-subscriber skip, and the
/// short-TTL initial-snapshot cache that backs the snapshot's part directory.
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
    public async Task MarkPartDirty_queues_regardless_of_subscribers_and_coalesces_per_serial()
    {
        // A very long window guarantees the background loop never drains during
        // the test, so PendingRebuildCount reflects exactly what marking did.
        var (router, _, _) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(router, TimeSpan.FromMinutes(10));
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91100");

        // No subscriber yet: the mark is still queued, because the rebuild loop
        // maintains the materialised summary view from the fact stream so a
        // later dashboard open reads a current snapshot. (The old behaviour -
        // skipping the mark with no subscriber - left the view stale.)
        broadcaster.MarkPartDirtyForTest(serial);
        Assert.That(broadcaster.PendingRebuildCount, Is.EqualTo(1),
            "a fact must queue a rebuild even with no subscriber, to keep the view current");

        // Repeated marks for the same serial coalesce to a single queued entry.
        broadcaster.MarkPartDirtyForTest(serial);
        broadcaster.MarkPartDirtyForTest(serial);
        Assert.That(broadcaster.PendingRebuildCount, Is.EqualTo(1),
            "repeated marks for one serial coalesce to a single rebuild");
    }

    [Test]
    public async Task View_is_maintained_without_subscribers_so_snapshot_is_current()
    {
        // A long rebuild window keeps the background loop from racing the test;
        // a zero snapshot TTL forces every GetInitialPartsAsync to re-read the
        // view rather than serve a memoised result.
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(
            router, TimeSpan.FromMinutes(10), snapshotCacheTtl: TimeSpan.Zero);
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91400");

        // A fact lands in the tree the broadcaster folds from, with no circuit
        // attached. Drive the rebuild deterministically (no stream timing).
        await lattice.EmitAsync(Step(serial, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(serial);
        await broadcaster.DrainDirtyForTestAsync();

        // The snapshot reads the materialised view (not a fresh per-part fold)
        // and sees the part, proving the view was maintained with no subscriber.
        var snapshot = await broadcaster.GetInitialPartsAsync();
        Assert.That(snapshot.Any(p => p.Serial == serial), Is.True,
            "the materialised view must reflect facts folded with no subscriber attached");
    }

    [Test]
    public async Task GetInitialPartsAsync_is_memoised_within_ttl()
    {
        // A long TTL makes the memoisation deterministic regardless of build
        // latency: the second call must reuse the first build.
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(
            router, TimeSpan.FromMinutes(10), snapshotCacheTtl: TimeSpan.FromMinutes(10));
        var first = new PartSerialNumber("HPT-BLD-S1-2028-91200");
        var second = new PartSerialNumber("HPT-BLD-S1-2028-91201");

        // Materialise the first part into the view before priming the cache.
        await lattice.EmitAsync(Step(first, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(first);
        await broadcaster.DrainDirtyForTestAsync();

        // Prime the cache with a snapshot that predates the second part.
        var seeded = await broadcaster.GetInitialPartsAsync();
        Assert.That(seeded.Any(p => p.Serial == first), Is.True);

        // Materialise the second part into the view after the cache was primed.
        await lattice.EmitAsync(Step(second, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(second);
        await broadcaster.DrainDirtyForTestAsync();

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
        var (router, _, lattice) = _fixture.NewRouter();
        await using var broadcaster = NewBroadcaster(
            router, TimeSpan.FromMinutes(10), snapshotCacheTtl: TimeSpan.Zero);
        var first = new PartSerialNumber("HPT-BLD-S1-2028-91300");
        var second = new PartSerialNumber("HPT-BLD-S1-2028-91301");

        await lattice.EmitAsync(Step(first, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(first);
        await broadcaster.DrainDirtyForTestAsync();
        var seeded = await broadcaster.GetInitialPartsAsync();
        Assert.That(seeded.Any(p => p.Serial == first), Is.True);

        await lattice.EmitAsync(Step(second, tick: 1, ProcessStage.Forge, ProcessSite.OhioForge), CancellationToken.None);
        broadcaster.MarkPartDirtyForTest(second);
        await broadcaster.DrainDirtyForTestAsync();

        var refreshed = await broadcaster.GetInitialPartsAsync();
        Assert.That(refreshed.Any(p => p.Serial == second), Is.True,
            "a rebuilt snapshot should include parts materialised since the previous build");
    }
}
