using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Phase 2 integration tests over the live <see cref="MaterialisedViewClusterFixture"/>:
/// re-keyed views (inserts / updates / deletes / range deletes), the re-key
/// collision fallback, and the <c>WaitForSourceHlcAsync</c> /
/// <c>WaitForSourceHeadAsync</c> read-your-writes barrier.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewPhase2IntegrationTests
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

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private ILatticeView CreateView(string sourceTreeId, string viewName, PredicateLatticeViewProjection projection)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private static PredicateLatticeViewProjection PrefixRekey() =>
        new(keySelector: src => $"v:{src}", keySelectorVersion: "prefix-v1");

    private async Task<ILattice> DrainToZeroAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return await _fixture.ActiveViewTreeAsync(viewName);
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
        return await _fixture.ActiveViewTreeAsync(viewName);
    }

    [Test]
    public async Task Rekeyed_view_reflects_inserts_updates_and_deletes()
    {
        // White-box reads of a view's backing tree run under an authorised
        // ViewReadContext scope (as the maintainer and ILatticeView handle do);
        // the public read-guard otherwise rejects direct view-tree reads.
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-rekey-src";
        const string view = "mv2-rekey-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, PrefixRekey());

        await source.SetAsync("a", Bytes("a1"));
        await source.SetAsync("b", Bytes("b1"));
        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("v:a"), Is.EqualTo(Bytes("a1")));
            Assert.That(await viewTree.GetAsync("v:b"), Is.EqualTo(Bytes("b1")));
            // The source key itself is never a view key under the re-map.
            Assert.That(await viewTree.GetAsync("a"), Is.Null);
        });

        // Update: the re-keyed entry tracks the new value.
        await source.SetAsync("a", Bytes("a2"));
        viewTree = await DrainToZeroAsync(view);
        Assert.That(await viewTree.GetAsync("v:a"), Is.EqualTo(Bytes("a2")));

        // Delete: the view key is recomputed from the source key and removed.
        await source.DeleteAsync("a");
        viewTree = await DrainToZeroAsync(view);
        Assert.That(await viewTree.GetAsync("v:a"), Is.Null);
    }

    [Test]
    public async Task Rekeyed_view_range_delete_via_matched_keys_retracts_each_key()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-rekey-range-src";
        const string view = "mv2-rekey-range-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, PrefixRekey());

        await source.SetAsync("a", Bytes("a1"));
        await source.SetAsync("b", Bytes("b1"));
        await source.SetAsync("c", Bytes("c1"));
        var viewTree = await DrainToZeroAsync(view);
        Assert.That(await viewTree.CountAsync(), Is.EqualTo(3));

        // Unfiltered range delete populates no MatchedKeys, but a re-keyed view
        // resolves it through a reconcile (rebuild) and converges all the same.
        await source.DeleteRangeAsync("a", "c");
        viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("v:a"), Is.Null);
            Assert.That(await viewTree.GetAsync("v:b"), Is.Null);
            // "c" is outside [a, c) and survives.
            Assert.That(await viewTree.GetAsync("v:c"), Is.EqualTo(Bytes("c1")));
        });
    }

    [Test]
    public async Task Key_preserving_view_range_delete_removes_view_range()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-kp-range-src";
        const string view = "mv2-kp-range-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, new PredicateLatticeViewProjection());

        await source.SetAsync("a", Bytes("a1"));
        await source.SetAsync("b", Bytes("b1"));
        await source.SetAsync("c", Bytes("c1"));
        await DrainToZeroAsync(view);

        await source.DeleteRangeAsync("a", "c");
        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("a"), Is.Null);
            Assert.That(await viewTree.GetAsync("b"), Is.Null);
            Assert.That(await viewTree.GetAsync("c"), Is.EqualTo(Bytes("c1")));
        });
    }

    [Test]
    public async Task Rekey_collision_emits_metric_and_falls_back_to_lww()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-collision-src";
        const string view = "mv2-collision-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // A deliberately non-injective re-map: every source key folds to one view
        // key. This is a configuration error; the maintainer surfaces it via the
        // collision counter and resolves by source-HLC last-writer-wins.
        var projection = new PredicateLatticeViewProjection(
            keySelector: _ => "folded",
            keySelectorVersion: "fold-v1");

        using var collisions = new MeterCollector<long>(
            LatticeMetrics.MeterName, "orleans.lattice.view.key_collisions");

        // Commit both source keys BEFORE the view exists so the maintainer's
        // first drain reads them in a single batch - the two distinct source
        // keys folding to one view key are then guaranteed to collide within the
        // batch deterministically rather than being split across drain passes.
        await source.SetAsync("a", Bytes("a1"));
        await source.SetAsync("b", Bytes("b1"));

        _ = CreateView(tree, view, projection);

        var viewTree = await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            // The view stays well-defined (one folded key with a winning value).
            Assert.That(await viewTree.GetAsync("folded"), Is.Not.Null);
            Assert.That(await viewTree.CountAsync(), Is.EqualTo(1));
        });

        Assert.That(collisions.Measurements.Sum(m => m.Value), Is.GreaterThan(0),
            "A non-injective re-map must record at least one collision.");
    }

    [Test]
    public async Task Rekey_source_write_never_exposes_old_and_new_view_keys_together()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-rekey-atomic-src";
        const string view = "mv2-rekey-atomic-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateView(tree, view, PrefixRekey());

        // Seed the row at source key "a" -> view key "v:a".
        await source.SetAsync("a", Bytes("payload"));
        var viewTree = await DrainToZeroAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("v:a"), Is.EqualTo(Bytes("payload")));
            Assert.That(await viewTree.GetAsync("v:b"), Is.Null);
        });

        // Re-key the row from source "a" to source "b" inside ONE atomic source
        // batch (upsert "b", delete "a"). The completed batch flushes to the view
        // as a mixed upsert(v:b) + delete(v:a) carried inside a single atomic
        // view-tree op, so the old view key is retracted in the same visibility
        // flip that publishes the new one.
        await source.SetManyAtomicAsync(
            new List<KeyValuePair<string, byte[]>> { new("b", Bytes("payload")) },
            new[] { "a" },
            $"rekey-{Guid.NewGuid():N}");

        // Drain one step at a time; the view must never expose BOTH the old and
        // the new view key at once - before the flip only v:a is present, after
        // it only v:b, never both.
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var converged = false;
        for (var attempt = 0; attempt < 50; attempt++)
        {
            var active = await _fixture.ActiveViewTreeAsync(view);
            var hasOld = await active.GetAsync("v:a") is not null;
            var hasNew = await active.GetAsync("v:b") is not null;
            Assert.That(hasOld && hasNew, Is.False,
                "the re-key flush exposed both the old and the new view key simultaneously");

            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                converged = true;
                break;
            }

            await Task.Delay(20);
        }

        Assert.That(converged, Is.True, $"View '{view}' did not catch up to the source head.");

        viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("v:b"), Is.EqualTo(Bytes("payload")));
            Assert.That(await viewTree.GetAsync("v:a"), Is.Null);
        });
    }

    [Test]
    public async Task WaitForSourceHlcAsync_completes_after_view_catches_up()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv2-barrier-src";
        const string view = "mv2-barrier-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateView(tree, view, new PredicateLatticeViewProjection());
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        await source.SetAsync("k", Bytes("v1"));
        var head = await maintainer.CaptureSourceHeadHlcAsync();

        await latticeView.WaitForSourceHlcAsync(head, TimeSpan.FromSeconds(10));

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.EqualTo(Bytes("v1")));
    }

    [Test]
    public async Task WaitForSourceHeadAsync_gives_read_your_writes()
    {
        const string tree = "mv2-head-src";
        const string view = "mv2-head-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateView(tree, view, new PredicateLatticeViewProjection());
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        await source.SetAsync("k", Bytes("written"));

        // Write-then-wait: the convenience captures the source head and blocks
        // until the view has applied it, so the read below is guaranteed to see
        // the write.
        await latticeView.WaitForSourceHeadAsync(TimeSpan.FromSeconds(10));

        Assert.That(await latticeView.GetAsync("k"), Is.EqualTo(Bytes("written")));
    }

    [Test]
    public async Task WaitForSourceHlcAsync_times_out_for_unreachable_target()
    {
        const string tree = "mv2-timeout-src";
        const string view = "mv2-timeout-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateView(tree, view, new PredicateLatticeViewProjection());
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        await source.SetAsync("k", Bytes("v1"));
        await DrainToZeroAsync(view);

        // A target far beyond any committed source HLC can never be reached, so
        // the barrier must time out rather than block forever.
        var unreachable = new HybridLogicalClock { WallClockTicks = long.MaxValue, Counter = 0 };

        Assert.That(
            async () => await latticeView.WaitForSourceHlcAsync(unreachable, TimeSpan.FromMilliseconds(200)),
            Throws.TypeOf<TimeoutException>());
    }
}
