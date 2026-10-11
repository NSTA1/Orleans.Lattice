using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression for a deleted runtime view's registration surviving in another
/// silo's in-memory view catalog. <see cref="ILatticeViewFactory.DeleteAsync"/>
/// removed the catalog entry only on the silo that served the call, and the
/// maintainer preferred its silo's catalog over the durable runtime registry.
/// A maintainer activating on a silo that had registered (or re-hydrated) the
/// view before the delete therefore kept the deleted definition: it resurrected
/// the deleted view over its old source and, after a re-create under the same
/// name over a different source, projected the old source into the new view.
/// Placement is made deterministic with <see cref="PreferLocalPlacement"/>, so
/// each step activates the maintainer on the silo whose factory or grain
/// factory issued the call.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewCatalogStaleRegistrationTests
{
    private const string Provider = "tests.stale-catalog.v1";
    private const string ThresholdProvider = "tests.stale-catalog.threshold.v1";
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_view_recreated_over_a_new_source_never_projects_the_deleted_definitions_source()
    {
        const string viewName = "stale-catalog-recreate";
        var oldSource = _cluster.Client.GetGrain<ILattice>("stale-catalog-old-src");
        var newSource = _cluster.Client.GetGrain<ILattice>("stale-catalog-new-src");
        await oldSource.SetAsync("old", [1]);
        await newSource.SetAsync("new", [2]);

        var view = await ViewFactory(_cluster.Primary).CreateAsync(
            oldSource, viewName, new LatticeRuntimeViewProjectionDescriptor(Provider, []));
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("old"), Is.EqualTo(new byte[] { 1 }));
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        // Delete and re-create through the other silo's factory, as any client
        // routed to that silo would.
        await ViewFactory(_cluster.SecondarySilos[0]).DeleteAsync(viewName);

        // A read handle (or the keepalive, or the activation service) wakes the
        // maintainer on the silo that created the deleted definition.
        await GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName).GetActiveTreeIdAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        var recreated = await ViewFactory(_cluster.SecondarySilos[0]).CreateAsync(
            newSource, viewName, new LatticeRuntimeViewProjectionDescriptor(Provider, []));
        await GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName).EnsureActiveAsync();
        await recreated.WaitForSourceHeadAsync(Barrier);

        Assert.Multiple(async () =>
        {
            Assert.That(await recreated.GetAsync("new"), Is.EqualTo(new byte[] { 2 }), "The re-created view must project its new source.");
            Assert.That(await recreated.GetAsync("old"), Is.Null, "The re-created view must not project the deleted definition's source.");
        });
    }

    [Test]
    public async Task A_deleted_view_is_not_resurrected_by_a_maintainer_activating_on_the_creating_silo()
    {
        const string viewName = "stale-catalog-resurrect";
        var source = _cluster.Client.GetGrain<ILattice>("stale-catalog-resurrect-src");
        await source.SetAsync("a", [1]);

        var view = await ViewFactory(_cluster.Primary).CreateAsync(
            source, viewName, new LatticeRuntimeViewProjectionDescriptor(Provider, []));
        await view.WaitForSourceHeadAsync(Barrier);
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        await ViewFactory(_cluster.SecondarySilos[0]).DeleteAsync(viewName);

        // The activation service, a keepalive or a stale read handle starts the
        // maintainer again on the silo that created the view.
        var maintainer = GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);
        await source.SetAsync("b", [2]);
        await maintainer.DrainAsync();

        using var scope = ViewReadContext.BeginScope();
        Assert.That(
            await ReadDeletedTreeAsync(LatticeViewTrees.ComposeTreeId(viewName), "b"),
            Is.Null,
            "A deleted view's maintainer must not keep projecting its old source.");
    }

    [Test]
    public async Task A_maintainer_activating_before_the_factory_persists_the_durable_record_keeps_the_entry()
    {
        // The synchronous factory path registers the catalog entry and persists the
        // durable record in the background, so a maintainer can activate before the
        // record exists. Model that window deterministically: a pending entry with
        // no durable record yet.
        const string viewName = "stale-catalog-pending";
        var source = _cluster.Client.GetGrain<ILattice>("stale-catalog-pending-src");
        await source.SetAsync("a", [1]);

        var catalog = Services(_cluster.Primary).GetRequiredService<IViewCatalog>();
        var registration = new ViewRegistration(
            viewName, "stale-catalog-pending-src", new IdentityProjection(), ProjectionProviderKey: Provider);
        catalog.MarkDurablyPending(registration);
        catalog.Register(registration);

        await GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName).EnsureActiveAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        Assert.That(
            catalog.TryGet(viewName),
            Is.SameAs(registration),
            "A registry record still being persisted is not evidence of a delete.");
        var view = await ViewFactory(_cluster.Primary).GetAsync(viewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("a"), Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task An_entry_whose_persist_failed_is_kept_while_no_durable_record_exists()
    {
        const string viewName = "stale-catalog-failed-absent";
        var source = _cluster.Client.GetGrain<ILattice>("stale-catalog-failed-absent-src");
        await source.SetAsync("a", [1]);

        var catalog = Services(_cluster.Primary).GetRequiredService<IViewCatalog>();
        var registration = new ViewRegistration(
            viewName, "stale-catalog-failed-absent-src", new IdentityProjection(), ProjectionProviderKey: Provider);
        catalog.MarkDurablyPending(registration);
        catalog.Register(registration);
        catalog.MarkDurablePersistFailed(registration);

        await GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName).EnsureActiveAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        Assert.That(
            catalog.TryGet(viewName),
            Is.SameAs(registration),
            "A failed persist leaves the entry catalog-only until a restart; the missing record is not a delete.");
    }

    [Test]
    public async Task An_entry_whose_persist_failed_is_superseded_by_a_later_durable_re_create()
    {
        // A synchronous create on the primary whose background persist failed, then
        // a re-create of the same name over a different source through another silo
        // (which cannot see, and so cannot delete, the primary's catalog-only entry).
        // The failed-persist mark must not shield the stale definition from the
        // durable re-create on the primary's next maintainer activation.
        const string viewName = "stale-catalog-failed-recreate";
        var oldSource = _cluster.Client.GetGrain<ILattice>("stale-catalog-failed-old-src");
        var newSource = _cluster.Client.GetGrain<ILattice>("stale-catalog-failed-new-src");
        await oldSource.SetAsync("old", [1]);
        await newSource.SetAsync("new", [2]);

        var catalog = Services(_cluster.Primary).GetRequiredService<IViewCatalog>();
        var stale = new ViewRegistration(
            viewName, "stale-catalog-failed-old-src", new IdentityProjection(), ProjectionProviderKey: Provider);
        catalog.MarkDurablyPending(stale);
        catalog.Register(stale);
        catalog.MarkDurablePersistFailed(stale);

        await _cluster.Client.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).RegisterAsync(new RuntimeViewRegistration
        {
            ViewName = viewName,
            SourceTreeId = "stale-catalog-failed-new-src",
            ProjectionTypeName = typeof(IdentityProjection).AssemblyQualifiedName!,
            ProjectionVersion = "v1",
            ProjectionProviderKey = Provider,
            ProjectionProviderPayload = [],
        });

        var maintainer = GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);

        var current = catalog.TryGet(viewName);
        Assert.That(current, Is.Not.Null.And.Not.SameAs(stale));
        Assert.That(current!.SourceTreeId, Is.EqualTo("stale-catalog-failed-new-src"));
        Assert.That(catalog.GetDurability(stale), Is.EqualTo(ViewCatalogDurability.Settled), "Replacing an entry discards its mark.");

        var view = await ViewFactory(_cluster.Primary).GetAsync(viewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.Multiple(async () =>
        {
            Assert.That(await view.GetAsync("new"), Is.EqualTo(new byte[] { 2 }), "The view must project the durable re-create's source.");
            Assert.That(await view.GetAsync("old"), Is.Null, "The view must not project the failed create's source.");
        });
    }

    [Test]
    public async Task A_view_recreated_with_a_different_provider_payload_never_keeps_the_deleted_payloads_projection()
    {
        // The provider contract binds a projection version, not a payload, so a
        // delete and re-create over the same source, provider and version can
        // differ only in the durable payload (here a predicate threshold). The
        // primary's settled entry still holds the deleted payload's projection. The
        // re-create is written straight to the registry, as another silo's factory
        // does, so the maintainer's next activation is placed on the primary.
        const string viewName = "stale-catalog-payload";
        const string sourceTreeId = "stale-catalog-payload-src";
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        await source.SetAsync("low", [3]);
        await source.SetAsync("high", [7]);

        var view = await ViewFactory(_cluster.Primary).CreateAsync(
            source, viewName, new LatticeRuntimeViewProjectionDescriptor(ThresholdProvider, [5]));
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("low"), Is.Null, "Precondition: threshold 5 excludes the low key.");
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);
        var catalog = Services(_cluster.Primary).GetRequiredService<IViewCatalog>();
        var stale = catalog.TryGet(viewName);
        Assert.That(stale, Is.Not.Null);

        await ViewFactory(_cluster.SecondarySilos[0]).DeleteAsync(viewName);
        await _cluster.Client.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).RegisterAsync(new RuntimeViewRegistration
        {
            ViewName = viewName,
            SourceTreeId = sourceTreeId,
            ProjectionTypeName = typeof(ThresholdProjection).FullName!,
            ProjectionVersion = "v1",
            ProjectionProviderKey = ThresholdProvider,
            ProjectionProviderPayload = [1],
        });

        var maintainer = GrainFactory(_cluster.Primary).GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await AssertMaintainerOnAsync(viewName, _cluster.Primary);
        Assert.That(catalog.TryGet(viewName), Is.Not.Null.And.Not.SameAs(stale), "The deleted payload's entry must be replaced by the durable re-create.");

        var recreated = await ViewFactory(_cluster.Primary).GetAsync(viewName);
        Assert.That(recreated, Is.Not.Null);
        await recreated!.WaitForSourceHeadAsync(Barrier);
        Assert.Multiple(async () =>
        {
            Assert.That(await recreated.GetAsync("low"), Is.EqualTo(new byte[] { 3 }), "The re-created view must project with its own payload's threshold.");
            Assert.That(await recreated.GetAsync("high"), Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public void A_rehydrated_entry_is_bound_to_the_full_durable_record_it_was_built_from()
    {
        var services = Services(_cluster.Primary);
        var record = new RuntimeViewRegistration
        {
            ViewName = "stale-catalog-binding",
            SourceTreeId = "stale-catalog-binding-src",
            ProjectionTypeName = typeof(ThresholdProjection).FullName!,
            ProjectionVersion = "v1",
            ProjectionProviderKey = ThresholdProvider,
            ProjectionProviderPayload = [4],
        };

        var registration = RuntimeViewRehydrator.Resolve(
            record,
            services,
            services.GetRequiredService<RuntimeViewProjectionProviderCatalog>(),
            NullLogger.Instance);

        Assert.That(registration, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(registration!.DurableRecord, Is.EqualTo(record));
            Assert.That(registration.DurableRecord, Is.Not.EqualTo(record with { ProjectionProviderPayload = [5] }), "A different payload is a different binding.");
            Assert.That(registration.DurableRecord, Is.Not.EqualTo(record with { ProjectionTypeName = "Other.Type" }), "A different projection type is a different binding.");
        });
    }

    [Test]
    public async Task A_failed_background_persist_moves_the_factorys_entry_to_persist_failed()
    {
        const string viewName = "stale-catalog-factory-persist-fails";
        var services = Services(_cluster.Primary);
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.RegisterAsync(Arg.Any<RuntimeViewRegistration>())
            .Returns(Task.FromException(new InvalidOperationException("registry unavailable")));
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);
        grains.GetGrain<IViewMaintainerGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(Substitute.For<IViewMaintainerGrain>());
        var catalog = new ViewCatalog();
        var factory = new LatticeViewFactory(
            grains,
            catalog,
            [],
            services.GetRequiredService<ILatticeReplicationContext>(),
            services.GetRequiredService<IOptionsMonitor<LatticeViewOptions>>(),
            services,
            services.GetRequiredService<RuntimeViewProjectionProviderCatalog>(),
            services.GetRequiredService<PredicateRuntimeViewProjectionCodec>(),
            NullLogger<LatticeViewFactory>.Instance);

        factory.Create(
            _cluster.Client.GetGrain<ILattice>("stale-catalog-factory-persist-fails-src"),
            viewName,
            new LatticeRuntimeViewProjectionDescriptor(Provider, []));
        var registration = catalog.TryGet(viewName);
        Assert.That(registration, Is.Not.Null);

        var deadline = DateTime.UtcNow + Barrier;
        while (catalog.GetDurability(registration!) != ViewCatalogDurability.PersistFailed && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.That(
            catalog.GetDurability(registration!),
            Is.EqualTo(ViewCatalogDurability.PersistFailed),
            "A failed persist must not leave the entry marked in flight, which would shield it from every later durable re-create.");
        Assert.That(catalog.TryGet(viewName), Is.SameAs(registration), "The entry stays catalog-only after the failure.");
    }

    private async Task<byte[]?> ReadDeletedTreeAsync(string treeId, string key)
    {
        try
        {
            return await _cluster.Client.GetGrain<ILattice>(treeId).GetAsync(key);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("deleted", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
    }

    private async Task AssertMaintainerOnAsync(string viewName, SiloHandle silo)
    {
        var grainId = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName).GetGrainId().ToString();
        var stats = await _cluster.Client.GetGrain<IManagementGrain>(0).GetDetailedGrainStatistics();
        var hosts = stats.Where(s => s.GrainId.ToString() == grainId).Select(s => s.SiloAddress).ToList();
        Assert.That(hosts, Is.EqualTo(new[] { silo.SiloAddress }), "Placement precondition: the maintainer must run on the expected silo.");
    }

    private static IServiceProvider Services(SiloHandle silo) => ((InProcessSiloHandle)silo).SiloHost.Services;

    private static IGrainFactory GrainFactory(SiloHandle silo) => Services(silo).GetRequiredService<IGrainFactory>();

    private static ILatticeViewFactory ViewFactory(SiloHandle silo) => Services(silo).GetRequiredService<ILatticeViewFactory>();

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<PlacementStrategy, PreferLocalPlacement>();
            siloBuilder.AddLatticeViews(views => views.AddRuntimeProjectionProvider(
                Provider,
                (_, context) => new LatticeViewDefinition(context.ViewName, new IdentityProjection()))
                .AddRuntimeProjectionProvider(
                ThresholdProvider,
                (_, context) => new LatticeViewDefinition(context.ViewName, new ThresholdProjection(context.Payload[0]))));
        }
    }

    private sealed class IdentityProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => mutation.Kind switch
        {
            MutationKind.Set => [ViewWrite.Upsert(mutation.Key!, mutation.Value!, mutation.Timestamp)],
            MutationKind.Delete => [ViewWrite.Delete(mutation.Key!, mutation.Timestamp)],
            _ => [],
        };
    }

    private sealed class ThresholdProjection(byte threshold) : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => mutation.Kind switch
        {
            MutationKind.Set when mutation.Value![0] >= threshold => [ViewWrite.Upsert(mutation.Key!, mutation.Value!, mutation.Timestamp)],
            MutationKind.Set or MutationKind.Delete => [ViewWrite.Delete(mutation.Key!, mutation.Timestamp)],
            _ => [],
        };
    }
}