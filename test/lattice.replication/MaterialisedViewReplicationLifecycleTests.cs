using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end integration tests for the Phase 7 view replication lifecycle:
/// <see cref="LatticeViewOptions.MaxLagBudget"/> eviction (the WAL-GC contract for a
/// chronically-lagging or dead view) and
/// <see cref="LatticeViewReplicationMode.ShipView"/> producer-designation /
/// maintainer suppression. Views are registered directly into the
/// <see cref="IViewCatalog"/> (bypassing the factory's background activation) so
/// convergence is driven deterministically through explicit
/// <c>DrainAsync</c> / <c>EnsureActiveAsync</c> calls.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewReplicationLifecycleTests
{
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

    private static byte[] Person(int age) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, "t"));

    private void RegisterView(string viewName, string sourceTreeId)
    {
        var catalog = _fixture.SiloServices.GetRequiredService<IViewCatalog>();
        var filter = LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);
        catalog.Register(new ViewRegistration(viewName, sourceTreeId, new PredicateLatticeViewProjection(filter)));
    }

    private async Task<int> ViewKeyCountAsync(string viewName)
    {
        var tree = await _fixture.ActiveViewTreeAsync(viewName);
        var count = 0;
        await foreach (var _ in tree.KeysAsync())
        {
            count++;
        }

        return count;
    }

    private async Task<bool> HasCursorPinAsync(string sourceTreeId, string viewName)
    {
        var registry = _fixture.SiloServices.GetRequiredService<IWalCursorRegistry>();
        var snapshot = await registry.SnapshotAsync(sourceTreeId);
        return snapshot.Any(s => s.ConsumerId == $"view:{viewName}");
    }

    [Test]
    public async Task Drain_evicts_and_rebuilds_when_lag_exceeds_budget()
    {
        const string tree = "mv-lag-evict-src";
        var view = MaterialisedViewClusterFixture.LagBudgetEvictionViewName;
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        RegisterView(view, tree);

        // A backlog far larger than the budget (3) and the one-entry batch size, so a
        // single incremental pass could never catch up - only an eviction rebuild does.
        const int count = 40;
        for (var i = 0; i < count; i++)
        {
            await source.SetAsync($"k{i:D2}", Person(30));
        }

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        // One drain trips the lag budget, unpins, and rebuilds from current source
        // state: the whole backlog materialises at once and the view catches up.
        await maintainer.DrainAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await maintainer.GetLagAsync(), Is.Zero, "eviction rebuild should catch the view up to the source head");
            Assert.That(await ViewKeyCountAsync(view), Is.EqualTo(count), "eviction rebuild should materialise the whole backlog");
            Assert.That(await HasCursorPinAsync(tree, view), Is.True, "the rebuild should re-pin the source WAL cursor at the rebuilt head");
        });
    }

    [Test]
    public async Task Drain_does_not_re_evict_within_the_lag_eviction_cooldown()
    {
        const string tree = "mv-lag-cooldown-src";
        var view = MaterialisedViewClusterFixture.LagEvictionCooldownViewName;
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        RegisterView(view, tree);

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        // First over-budget backlog: one drain evicts and rebuilds the whole backlog.
        const int firstBatch = 40;
        for (var i = 0; i < firstBatch; i++)
        {
            await source.SetAsync($"a{i:D2}", Person(30));
        }

        await maintainer.DrainAsync();
        Assert.That(await maintainer.GetLagAsync(), Is.Zero, "the first over-budget drain should evict and catch the view up");

        // A second over-budget backlog within the (default 30s) cooldown must NOT
        // trigger another eviction rebuild - it drains only one entry (BatchSize=1)
        // and stays behind, proving the cooldown prevents rebuild thrashing.
        const int secondBatch = 40;
        for (var i = 0; i < secondBatch; i++)
        {
            await source.SetAsync($"b{i:D2}", Person(30));
        }

        await maintainer.DrainAsync();

        Assert.That(
            await maintainer.GetLagAsync(),
            Is.GreaterThan(0),
            "within the lag-eviction cooldown a one-entry-batch drain cannot catch up to a fresh over-budget backlog, so the view must stay behind rather than re-evicting");
    }


    [Test]
    public async Task Drain_does_not_evict_when_budget_disabled()
    {
        const string tree = "mv-lag-noevict-src";
        var view = MaterialisedViewClusterFixture.LagBudgetDisabledViewName;
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        RegisterView(view, tree);

        // More entries than partitions * batch size (8 * 1), so a single incremental
        // drain provably cannot catch up - and with the budget disabled there is no
        // eviction rebuild to short-circuit it.
        const int count = 40;
        for (var i = 0; i < count; i++)
        {
            await source.SetAsync($"k{i:D2}", Person(30));
        }

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.DrainAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "without eviction a one-entry-batch drain cannot catch up to a 40-entry backlog");
            Assert.That(await ViewKeyCountAsync(view), Is.LessThan(count), "without eviction the view should not be fully materialised after one pass");
        });
    }

    [Test]
    public async Task ShipView_producer_runs_and_derives_into_the_stable_view_tree()
    {
        const string tree = "mv-shipview-producer-src";
        var view = MaterialisedViewClusterFixture.ShipViewProducerViewName;
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        RegisterView(view, tree);

        // Source is present locally before activation -> this cluster is the producer.
        await source.SetAsync("a", Person(30));
        await source.SetAsync("b", Person(40));
        await source.SetAsync("c", Person(10)); // filtered out

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        await maintainer.DrainAsync();

        await Assert.MultipleAsync(async () =>
        {
            // ShipView pins the stable generation-0 tree id (no shadow-swap cycling).
            Assert.That(await maintainer.GetActiveTreeIdAsync(), Is.EqualTo($"view-{view}"));
            Assert.That(await ViewKeyCountAsync(view), Is.EqualTo(2), "the producer maintainer should derive the filtered view");
            Assert.That(await HasCursorPinAsync(tree, view), Is.True, "the producer maintainer should pin the source WAL cursor");
        });
    }

    [Test]
    public async Task ShipView_consumer_suppresses_the_maintainer_when_source_absent()
    {
        const string tree = "mv-shipview-consumer-src-never-written";
        var view = MaterialisedViewClusterFixture.ShipViewConsumerViewName;
        RegisterView(view, tree);

        // The source WAL is never written on this cluster, so the ShipView maintainer
        // is suppressed at activation: it receives the view via replication instead.
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        var applied = await maintainer.DrainAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(applied, Is.Zero, "a suppressed consumer maintainer's drain is a no-op");
            Assert.That(await HasCursorPinAsync(tree, view), Is.False, "a suppressed consumer maintainer must not pin the source WAL");

            // A suppressed consumer cannot run source-digest reconcile (producer-only);
            // drift is repaired via replication anti-entropy, so it reports no repair.
            Assert.That(await maintainer.ReconcileAsync(), Is.False, "reconcile is producer-only and a no-op on a suppressed consumer");
        });
    }

    [Test]
    public async Task ShipView_producer_unsuppresses_when_the_source_becomes_readable_later()
    {
        const string tree = "mv-shipview-late-source-src";
        var view = MaterialisedViewClusterFixture.ShipViewLateSourceViewName;
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        RegisterView(view, tree);

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        // Activate over a still-empty source: the producer cannot yet tell it is the
        // producer (no locally-readable source WAL), so it suppresses itself.
        await maintainer.EnsureActiveAsync();
        Assert.That(
            await HasCursorPinAsync(tree, view),
            Is.False,
            "a ShipView maintainer over a still-empty source should be suppressed and pin nothing");

        // The source is written later. The keepalive re-routes a suppressed maintainer
        // through EnsureActiveAsync, which re-probes readability and un-suppresses.
        await source.SetAsync("a", Person(30));
        await source.SetAsync("b", Person(40));
        await source.SetAsync("c", Person(10)); // filtered out

        await maintainer.EnsureActiveAsync();
        await maintainer.DrainAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await HasCursorPinAsync(tree, view), Is.True, "once the source becomes readable the producer must un-suppress and pin the source WAL");
            Assert.That(await ViewKeyCountAsync(view), Is.EqualTo(2), "the un-suppressed producer should derive the filtered view");
        });
    }
}
