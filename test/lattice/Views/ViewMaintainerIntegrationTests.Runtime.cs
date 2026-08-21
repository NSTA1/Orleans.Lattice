using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Views;

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
        var view = await _fixture.ViewFactory.CreateAsync(
            src,
            runtimeView,
            new LatticeRuntimeViewProjectionDescriptor(
                ViewClusterFixture.RuntimeCountProvider,
                []));
        Assert.That(view.ViewName, Is.EqualTo(runtimeView));

        // CreateAsync persists before publishing the catalog entry and returning.
        Assert.That(await _fixture.ViewFactory.GetAsync(runtimeView), Is.Not.Null);
        var registry = _fixture.SiloServices.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        Assert.That((await registry.ListAsync()).Any(record => record.ViewName == runtimeView), Is.True);

        // A runtime-created view (unlike a startup-declared one) can be decommissioned.
        Assert.That(
            async () => await _fixture.ViewFactory.DeleteAsync(runtimeView),
            Throws.Nothing);
    }

    [Test]
    public void Create_startup_declared_aggregation_uses_the_supplied_definition()
    {
        var source = _fixture.Source(ViewClusterFixture.CountSource);

        var view = _fixture.ViewFactory.Create(
            source,
            ViewClusterFixture.CountView,
            new LatticeViewDefinition(
                ViewClusterFixture.CountView,
                new AggregationLatticeViewProjection(
                    AggregationKind.Count,
                    ViewClusterFixture.GroupOf,
                    "v1")));

        Assert.That(view.ViewName, Is.EqualTo(ViewClusterFixture.CountView));
    }

    [Test]
    public async Task Create_descriptor_only_ordinary_provider_registers_and_decommissions()
    {
        const string viewName = "runtime-ordinary";
        var view = _fixture.ViewFactory.Create(
            _fixture.Source("src-runtime-ordinary"),
            viewName,
            new LatticeRuntimeViewProjectionDescriptor(
                ViewClusterFixture.RuntimeScenarioProvider,
                [0]));

        Assert.That(view.ViewName, Is.EqualTo(viewName));
        var registry = _fixture.SiloServices.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!(await registry.ListAsync()).Any(record => record.ViewName == viewName))
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "runtime registration was not persisted");
            await Task.Delay(20);
        }

        await _fixture.ViewFactory.DeleteAsync(viewName);
    }

    [TestCase("missing-provider", new byte[] { 0 })]
    [TestCase(ViewClusterFixture.RuntimeScenarioProvider, new byte[] { 1 })]
    [TestCase(ViewClusterFixture.RuntimeScenarioProvider, new byte[] { 2 })]
    [TestCase(ViewClusterFixture.RuntimeScenarioProvider, new byte[] { 3 })]
    public void Create_descriptor_only_invalid_provider_result_throws(
        string providerKey,
        byte[] payload)
    {
        var viewName = $"runtime-invalid-{providerKey}-{payload[0]}";

        Assert.That(
            () => _fixture.ViewFactory.Create(
                _fixture.Source($"src-{viewName}"),
                viewName,
                new LatticeRuntimeViewProjectionDescriptor(providerKey, payload)),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Create_definition_with_explicit_descriptor_preflights_and_decommissions()
    {
        const string viewName = "runtime-explicit-definition";
        var descriptor = new LatticeRuntimeViewProjectionDescriptor(
            ViewClusterFixture.RuntimeScenarioProvider,
            [0]);

        var view = await _fixture.ViewFactory.CreateAsync(
            _fixture.Source("src-runtime-explicit-definition"),
            viewName,
            new LatticeViewDefinition(
                viewName,
                new PredicateLatticeViewProjection(),
                descriptor));

        Assert.That(view.ViewName, Is.EqualTo(viewName));
        await _fixture.ViewFactory.DeleteAsync(viewName);
    }

    [Test]
    public async Task Create_filterOnlyPredicate_automaticallyAttachesBuiltInDescriptor()
    {
        const string viewName = "runtime-auto-predicate";

        var view = await _fixture.ViewFactory.CreateAsync(
            _fixture.Source("src-runtime-auto-predicate"),
            viewName,
            new LatticeViewDefinition(
                viewName,
                new PredicateLatticeViewProjection()));

        Assert.That(view.ViewName, Is.EqualTo(viewName));
        await _fixture.ViewFactory.DeleteAsync(viewName);
    }

    [Test]
    public void Create_selectorBackedPredicate_withoutExplicitDescriptor_throws()
    {
        const string viewName = "runtime-selector-without-provider";

        Assert.That(
            () => _fixture.ViewFactory.Create(
                _fixture.Source("src-runtime-selector-without-provider"),
                viewName,
                new LatticeViewDefinition(
                    viewName,
                    new PredicateLatticeViewProjection(
                        valueSelector: value => value,
                        valueSelectorVersion: "v1"))),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task CreateAsync_descriptor_for_startup_view_does_not_persist_runtime_record()
    {
        var source = _fixture.Source(ViewClusterFixture.CountSource);
        var registry = _fixture.SiloServices.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);

        try
        {
            await _fixture.ViewFactory.CreateAsync(
                source,
                ViewClusterFixture.CountView,
                new LatticeRuntimeViewProjectionDescriptor(
                    ViewClusterFixture.RuntimeCountProvider,
                    []));

            Assert.That(
                (await registry.ListAsync()).Any(record => record.ViewName == ViewClusterFixture.CountView),
                Is.False);
        }
        finally
        {
            _fixture.ViewFactory.Create(
                source,
                ViewClusterFixture.CountView,
                new LatticeViewDefinition(
                    ViewClusterFixture.CountView,
                    new AggregationLatticeViewProjection(
                        AggregationKind.Count,
                        ViewClusterFixture.GroupOf,
                        "v1")));
        }
    }

    [Test]
    public void CreateAsync_definition_with_cancelled_token_throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _fixture.ViewFactory.CreateAsync(
                _fixture.Source("src-runtime-cancelled-definition"),
                "runtime-cancelled-definition",
                new LatticeViewDefinition(
                    "runtime-cancelled-definition",
                    new PredicateLatticeViewProjection()),
                cts.Token),
            Throws.TypeOf<OperationCanceledException>());
    }

    [Test]
    public void CreateAsync_descriptor_with_cancelled_token_throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _fixture.ViewFactory.CreateAsync(
                _fixture.Source("src-runtime-cancelled-descriptor"),
                "runtime-cancelled-descriptor",
                new LatticeRuntimeViewProjectionDescriptor(
                    ViewClusterFixture.RuntimeScenarioProvider,
                    [0]),
                cts.Token),
            Throws.TypeOf<OperationCanceledException>());
    }

    [Test]
    public async Task ActivationService_rehydrates_a_durable_runtime_view_into_an_empty_catalog()
    {
        const string viewName = "runtime-rehydrate";
        await _fixture.ViewFactory.CreateAsync(
            _fixture.Source("src-runtime-rehydrate"),
            viewName,
            new LatticeRuntimeViewProjectionDescriptor(
                ViewClusterFixture.RuntimeScenarioProvider,
                [0]));

        var services = _fixture.SiloServices;
        var registry = services.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        Assert.That((await registry.ListAsync()).Any(record => record.ViewName == viewName), Is.True);

        var catalog = services.GetRequiredService<IViewCatalog>();
        catalog.Remove(viewName);
        Assert.That(catalog.TryGet(viewName), Is.Null);

        var activation = new ViewActivationService(
            services,
            services.GetRequiredService<IReadOnlyList<StartupViewRegistration>>(),
            catalog,
            services.GetRequiredService<RuntimeViewProjectionProviderCatalog>(),
            services.GetRequiredService<IGrainFactory>(),
            NullLogger<ViewActivationService>.Instance);
        await activation.StartAsync(CancellationToken.None);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (catalog.TryGet(viewName) is null)
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "runtime view was not rehydrated");
            await Task.Delay(20);
        }

        await activation.StopAsync(CancellationToken.None);
        await _fixture.ViewFactory.DeleteAsync(viewName);
    }
}
