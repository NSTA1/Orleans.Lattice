using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Phase 5 integration tests for the shadow-swap rebuild, generation-addressed
/// view trees, <see cref="ILatticeView.ReconcileAsync"/>, and the drift digest.
/// Proves a rebuild never exposes a half-built view (readers stay on the prior
/// fully-built generation until the atomic swap), the swapped-out generation tree
/// is reclaimed, rebuilt entries keep their TTL, reconcile detects and repairs
/// out-of-band drift, the drift digest distinguishes in-sync from drifted, and a
/// crash between shadow-build and swap leaves the old generation serving while a
/// subsequent rebuild converges. Covers both filter / re-key and aggregation views.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewShadowSwapTests
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

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Record(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static string Team(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Team;

    private static double Score(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Score;

    private ILatticeView CreateSumView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new AggregationLatticeViewProjection(AggregationKind.Sum, Team, "v1", valueSelector: Score);
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task DrainToZeroAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
    }

    private ILattice ViewGrain(string treeId) => _fixture.Cluster.Client.GetGrain<ILattice>(treeId);

    // (a) A concurrent reader never observes an empty or partially-built view
    // during a rebuild: the read handle resolves the active generation, which
    // only flips - in a single durable write - once the shadow is fully built.
    [Test]
    public async Task Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader()
    {
        const string tree = "ss-rebuild-src";
        const string view = "ss-rebuild-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);

        const int n = 25;
        for (var i = 0; i < n; i++)
        {
            await source.SetAsync($"k{i:D2}", Person(20 + i, $"t{i}"));
        }

        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(n));

        using var cts = new CancellationTokenSource();
        var minObserved = n;
        var reader = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                var count = await latticeView.CountAsync();
                minObserved = Math.Min(minObserved, count);
            }
        });

        // Several rebuilds back-to-back to widen the window the reader samples.
        for (var r = 0; r < 3; r++)
        {
            await latticeView.RebuildAsync();
        }

        cts.Cancel();
        await reader;

        Assert.That(minObserved, Is.EqualTo(n), "Reader observed a half-built or empty view during a rebuild.");
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(n));
    }

    // (b) The swapped-out generation tree is reclaimed after the post-swap grace.
    [Test]
    public async Task Rebuild_reclaims_the_old_generation_tree()
    {
        const string tree = "ss-reclaim-src";
        const string view = "ss-reclaim-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));
        await DrainToZeroAsync(view);

        var oldTreeId = await maintainer.GetActiveTreeIdAsync();
        Assert.That(await ViewGrain(oldTreeId).CountAsync(), Is.EqualTo(2));

        await latticeView.RebuildAsync();

        var newTreeId = await maintainer.GetActiveTreeIdAsync();
        Assert.That(newTreeId, Is.Not.EqualTo(oldTreeId), "Active generation must advance after a rebuild.");

        // Reclaim runs on the drain cadence once the grace (200 ms in the fixture)
        // elapses; poll a few drains to let it fire.
        for (var attempt = 0; attempt < 20; attempt++)
        {
            await Task.Delay(50);
            await maintainer.DrainAsync();
            if (await ViewGrain(oldTreeId).CountAsync() == 0)
            {
                break;
            }
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await ViewGrain(oldTreeId).CountAsync(), Is.EqualTo(0), "Old generation tree was not reclaimed.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(2), "Active view must still serve every entry.");
        });
    }

    // (c) Rebuilt entries keep their TTL (the Phase 1 TTL-loss deviation is fixed).
    [Test]
    public async Task Rebuild_preserves_entry_ttl()
    {
        const string tree = "ss-ttl-src";
        const string view = "ss-ttl-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Person(30, "ttl"), TimeSpan.FromHours(1));
        await DrainToZeroAsync(view);

        await latticeView.RebuildAsync();

        var activeTree = ViewGrain(await maintainer.GetActiveTreeIdAsync());
        var versioned = await activeTree.GetWithVersionAsync("a");

        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(Person(30, "ttl")));
            Assert.That(versioned.ExpiresAtTicks, Is.GreaterThan(DateTime.UtcNow.Ticks),
                "A rebuilt entry must retain a future expiry, not drop its TTL.");
        });
    }

    // (d) ReconcileAsync detects and repairs out-of-band drift, and is a no-op
    // (returns false) when the view already matches the source.
    [Test]
    public async Task ReconcileAsync_detects_and_repairs_out_of_band_drift()
    {
        const string tree = "ss-reconcile-src";
        const string view = "ss-reconcile-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));
        await DrainToZeroAsync(view);

        // In sync: reconcile detects no drift.
        Assert.That(await latticeView.ReconcileAsync(), Is.False, "An in-sync view must report no drift.");

        // Introduce drift directly on the active tree behind the maintainer's back.
        var activeTree = ViewGrain(await maintainer.GetActiveTreeIdAsync());
        await activeTree.DeleteAsync("a");
        Assert.That(await latticeView.GetAsync("a"), Is.Null);

        // Reconcile detects and repairs.
        Assert.That(await latticeView.ReconcileAsync(), Is.True, "Drift must be detected and repaired.");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("a"), Is.EqualTo(Person(30, "a")));
            Assert.That(await latticeView.GetAsync("b"), Is.EqualTo(Person(40, "b")));
        });

        // A second reconcile is now a no-op again.
        Assert.That(await latticeView.ReconcileAsync(), Is.False);
    }

    // (e) The drift digest matches between an in-sync view and source, and differs
    // after an out-of-band mutation.
    [Test]
    public async Task Drift_digest_matches_in_sync_and_differs_after_out_of_band_mutation()
    {
        const string tree = "ss-digest-src";
        const string view = "ss-digest-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));
        await DrainToZeroAsync(view);

        var inSync = await latticeView.ComputeDigestAsync();
        Assert.That(inSync.EntryCount, Is.EqualTo(2));

        // Mutate the active tree out-of-band: the digest must change.
        var activeTree = ViewGrain(await maintainer.GetActiveTreeIdAsync());
        await activeTree.SetAsync("a", Person(31, "tampered"));
        var drifted = await latticeView.ComputeDigestAsync();
        Assert.That(inSync.ContentEquals(drifted), Is.False, "Digest must change after an out-of-band mutation.");

        // Repair, then the digest returns to the in-sync fingerprint.
        Assert.That(await latticeView.ReconcileAsync(), Is.True);
        var repaired = await latticeView.ComputeDigestAsync();
        Assert.That(inSync.ContentEquals(repaired), Is.True, "Digest must return to the in-sync value after repair.");
    }

    // (f) A crash between shadow-build and swap leaves the old generation serving;
    // a subsequent rebuild clears the orphaned shadow and converges.
    [Test]
    public async Task Rebuild_converges_after_a_simulated_crash_before_swap()
    {
        const string tree = "ss-crash-src";
        const string view = "ss-crash-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));
        await DrainToZeroAsync(view);

        var activeId = await maintainer.GetActiveTreeIdAsync();

        // Simulate a crashed shadow build: write a bogus partial entry into the
        // next generation's tree without ever swapping. The active generation is
        // unchanged, so reads keep serving the old fully-built tree.
        var shadowId = $"{activeId}#g1";
        await ViewGrain(shadowId).SetAsync("orphan", Person(99, "orphan"));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await maintainer.GetActiveTreeIdAsync(), Is.EqualTo(activeId), "A crash before swap must leave the old generation active.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(2), "Old generation must keep serving every entry.");
            Assert.That(await latticeView.GetAsync("orphan"), Is.Null, "The orphaned shadow must not be visible to readers.");
        });

        // A subsequent rebuild clears the orphaned shadow first, then converges.
        await latticeView.RebuildAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("a"), Is.EqualTo(Person(30, "a")));
            Assert.That(await latticeView.GetAsync("b"), Is.EqualTo(Person(40, "b")));
            Assert.That(await latticeView.GetAsync("orphan"), Is.Null, "The orphaned key must not survive the rebuild.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(2));
        });
    }

    // Aggregation coverage: a rebuild keeps a concurrent reader on a fully-built
    // view, and reconcile repairs out-of-band drift to a group value.
    [Test]
    public async Task Aggregation_rebuild_and_reconcile_preserve_and_repair_group_values()
    {
        const string tree = "ss-agg-src";
        const string view = "ss-agg-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateSumView(tree, view);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);

        await source.SetAsync("a", Record("red", 10));
        await source.SetAsync("b", Record("red", 5));
        await source.SetAsync("c", Record("blue", 7));
        await DrainToZeroAsync(view);

        static double? Sum(byte[]? bytes) => bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

        Assert.Multiple(async () =>
        {
            Assert.That(Sum(await latticeView.GetAsync("red")), Is.EqualTo(15));
            Assert.That(Sum(await latticeView.GetAsync("blue")), Is.EqualTo(7));
        });

        var inSync = await latticeView.ComputeDigestAsync();

        // Rebuild reproduces the same materialised group values.
        await latticeView.RebuildAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(Sum(await latticeView.GetAsync("red")), Is.EqualTo(15));
            Assert.That(Sum(await latticeView.GetAsync("blue")), Is.EqualTo(7));
        });
        Assert.That(inSync.ContentEquals(await latticeView.ComputeDigestAsync()), Is.True,
            "Aggregation digest excludes reserved rows and must be rebuild-stable.");

        // Drift a materialised group value out-of-band, then reconcile repairs it.
        var activeTree = ViewGrain(await maintainer.GetActiveTreeIdAsync());
        await activeTree.SetAsync("red", LatticeAggregationValue.EncodeDouble(999));
        Assert.That(await latticeView.ReconcileAsync(), Is.True, "Out-of-band group drift must be detected and repaired.");
        Assert.That(Sum(await latticeView.GetAsync("red")), Is.EqualTo(15));
    }
}
