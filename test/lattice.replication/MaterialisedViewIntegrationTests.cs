using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end integration tests for Phase 1 materialised views over a live test
/// cluster: a predicate filter view is created over a source tree, the source is
/// mutated, and the view is asserted to converge (inserts surface, updates win by
/// HLC, predicate-exit and source deletes retract). Convergence is driven through
/// the internal maintainer grain's <c>DrainAsync</c> so assertions are
/// deterministic rather than timer-dependent.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewIntegrationTests
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
    public async Task View_reflects_inserts_filtered_by_predicate()
    {
        const string tree = "mv-insert-src";
        const string view = "mv-insert-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateAdultView(tree, view);

        await source.SetAsync("a", Person(30, "a1"));
        await source.SetAsync("b", Person(40, "b1"));
        await source.SetAsync("m", Person(10, "m1"));

        await DrainToZeroAsync(view);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("a"), Is.EqualTo(Person(30, "a1")));
            Assert.That(await viewTree.GetAsync("b"), Is.EqualTo(Person(40, "b1")));
            Assert.That(await viewTree.GetAsync("m"), Is.Null);
        });
    }

    [Test]
    public async Task View_update_wins_by_hlc()
    {
        const string tree = "mv-update-src";
        const string view = "mv-update-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateAdultView(tree, view);

        await source.SetAsync("k", Person(30, "old"));
        await DrainToZeroAsync(view);

        await source.SetAsync("k", Person(31, "new"));
        await DrainToZeroAsync(view);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.EqualTo(Person(31, "new")));
    }

    [Test]
    public async Task View_retracts_key_that_updates_out_of_predicate()
    {
        const string tree = "mv-retract-src";
        const string view = "mv-retract-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateAdultView(tree, view);

        await source.SetAsync("k", Person(30, "adult"));
        await DrainToZeroAsync(view);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.Not.Null);

        await source.SetAsync("k", Person(5, "minor"));
        await DrainToZeroAsync(view);

        viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task View_removes_deleted_source_key()
    {
        const string tree = "mv-delete-src";
        const string view = "mv-delete-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateAdultView(tree, view);

        await source.SetAsync("k", Person(30, "v"));
        await DrainToZeroAsync(view);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.Not.Null);

        await source.DeleteAsync("k");
        await DrainToZeroAsync(view);

        viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task GetLagAsync_reports_backlog_then_zero_after_drain()
    {
        const string tree = "mv-lag-src";
        const string view = "mv-lag-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);

        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));

        Assert.That(await latticeView.GetLagAsync(), Is.GreaterThan(0));

        await DrainToZeroAsync(view);

        Assert.That(await latticeView.GetLagAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task RebuildAsync_reconverges_after_view_drift()
    {
        const string tree = "mv-rebuild-src";
        const string view = "mv-rebuild-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var latticeView = CreateAdultView(tree, view);

        await source.SetAsync("a", Person(30, "a"));
        await source.SetAsync("b", Person(40, "b"));
        await DrainToZeroAsync(view);

        // Simulate drift: delete an entry directly from the active view tree behind
        // the maintainer's back, then rebuild from current source state. After a
        // shadow-swap rebuild the active generation advances, so assertions read
        // through the handle (which resolves the active generation) rather than a
        // fixed gen-0 tree id.
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        var activeTree = _fixture.Cluster.Client.GetGrain<ILattice>(await maintainer.GetActiveTreeIdAsync());
        await activeTree.DeleteAsync("a");
        Assert.That(await activeTree.GetAsync("a"), Is.Null);

        await latticeView.RebuildAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("a"), Is.EqualTo(Person(30, "a")));
            Assert.That(await latticeView.GetAsync("b"), Is.EqualTo(Person(40, "b")));
        });
    }
}
