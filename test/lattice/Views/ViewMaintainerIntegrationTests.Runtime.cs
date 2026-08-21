namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Integration tests for the fold-view path and the runtime view lifecycle
/// (<see cref="ILatticeViewFactory.Create"/> / <see cref="ILatticeViewFactory.GetAsync"/> /
/// <see cref="ILatticeViewFactory.DeleteAsync"/>), reusing the shared cluster fixture.
/// </summary>
public partial class ViewMaintainerIntegrationTests
{
    [Test]
    public async Task Fold_view_materialises_member_count_accumulator()
    {
        var src = _fixture.Source(ViewClusterFixture.FoldSource);
        await src.SetAsync("f1", ViewClusterFixture.AggValue("fg"));
        await src.SetAsync("f2", ViewClusterFixture.AggValue("fg"));
        await src.SetAsync("f3", ViewClusterFixture.AggValue("fg"));

        var view = await ViewAsync(ViewClusterFixture.FoldView);
        await view.WaitForSourceHeadAsync(Barrier);

        var acc = await view.GetAsync("fg");
        Assert.That(acc, Is.Not.Null);
        Assert.That(BitConverter.ToInt64(acc!), Is.EqualTo(3L));
    }

    [Test]
    public async Task Fold_view_refolds_group_on_source_delete()
    {
        var src = _fixture.Source(ViewClusterFixture.FoldSource);
        await src.SetAsync("fd1", ViewClusterFixture.AggValue("fdg"));
        await src.SetAsync("fd2", ViewClusterFixture.AggValue("fdg"));

        var view = await ViewAsync(ViewClusterFixture.FoldView);
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(BitConverter.ToInt64((await view.GetAsync("fdg"))!), Is.EqualTo(2L));

        await src.DeleteAsync("fd1");
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(BitConverter.ToInt64((await view.GetAsync("fdg"))!), Is.EqualTo(1L));
    }

    [Test]
    public async Task GetAsync_returns_null_for_unregistered_view()
    {
        var view = await _fixture.ViewFactory.GetAsync("no-such-view-xyz");
        Assert.That(view, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_on_startup_declared_view_throws()
    {
        Assert.That(
            async () => await _fixture.ViewFactory.DeleteAsync(ViewClusterFixture.CountView),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task DeleteAsync_for_unknown_view_is_idempotent_noop()
    {
        Assert.That(
            async () => await _fixture.ViewFactory.DeleteAsync("never-created-view"),
            Throws.Nothing);
    }

    [Test]
    public async Task Create_runtime_view_registers_handle_and_decommissions()
    {
        const string runtimeView = "runtime-count";
        var src = _fixture.Source("src-runtime");
        var definition = new LatticeViewDefinition(
            runtimeView,
            new AggregationLatticeViewProjection(
                AggregationKind.Count, ViewClusterFixture.GroupOf, "v1"));

        var view = _fixture.ViewFactory.Create(src, runtimeView, definition);
        Assert.That(view.ViewName, Is.EqualTo(runtimeView));

        // Create registers the view in the catalog, so GetAsync now resolves it.
        Assert.That(await _fixture.ViewFactory.GetAsync(runtimeView), Is.Not.Null);

        // A runtime-created view (unlike a startup-declared one) can be decommissioned.
        Assert.That(
            async () => await _fixture.ViewFactory.DeleteAsync(runtimeView),
            Throws.Nothing);
    }
}
