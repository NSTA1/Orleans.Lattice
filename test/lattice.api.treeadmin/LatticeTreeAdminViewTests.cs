using System.Reflection;
using System.Reflection.Emit;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the materialised-view administration operations on
/// <see cref="LatticeTreeAdmin"/>. The listing verb authorizes the cluster-wide
/// <c>Telemetry</c> capability fail-closed; the per-view verbs resolve each view's
/// source tree authoritatively (silo-local catalog first, then the cluster-wide
/// runtime-view registry, fail-closed to <see cref="KeyNotFoundException"/>) and
/// authorize whole-tree <c>Read</c> (status) or <c>Admin</c> (rebuild, reconcile,
/// drop) over that resolved source before dialing the maintainer or factory. When
/// the materialised-view subsystem is not enabled every verb throws
/// <see cref="InvalidOperationException"/>. Driven purely with substitutes and a
/// hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminViewTests
{
    [Test]
    public void CreateViewAsync_defaultInterfaceImplementation_throwsNotSupported()
    {
        var admin = CreateLegacyImplementation();

        Assert.That(
            async () => await admin.CreateViewAsync("view", "source", "provider", []),
            Throws.TypeOf<NotSupportedException>());
    }

    private static ILatticeTreeAdmin CreateLegacyImplementation()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("LegacyTreeAdminTests"),
            AssemblyBuilderAccess.Run);
        var type = assembly
            .DefineDynamicModule("LegacyTreeAdminTests")
            .DefineType(
                "LegacyTreeAdmin",
                TypeAttributes.Public | TypeAttributes.Sealed,
                typeof(object),
                [typeof(ILatticeTreeAdmin)]);
        type.DefineDefaultConstructor(MethodAttributes.Public);

        foreach (var contract in typeof(ILatticeTreeAdmin)
                     .GetMethods()
                     .Where(method => method.IsAbstract))
        {
            var method = type.DefineMethod(
                contract.Name,
                MethodAttributes.Public
                | MethodAttributes.Virtual
                | MethodAttributes.Final
                | MethodAttributes.HideBySig
                | MethodAttributes.NewSlot,
                contract.ReturnType,
                contract.GetParameters().Select(parameter => parameter.ParameterType).ToArray());
            var il = method.GetILGenerator();
            if (contract.ReturnType == typeof(void))
            {
                il.Emit(OpCodes.Ret);
            }
            else if (contract.ReturnType.IsValueType)
            {
                var value = il.DeclareLocal(contract.ReturnType);
                il.Emit(OpCodes.Ldloca_S, value);
                il.Emit(OpCodes.Initobj, contract.ReturnType);
                il.Emit(OpCodes.Ldloc, value);
                il.Emit(OpCodes.Ret);
            }
            else
            {
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Ret);
            }

            type.DefineMethodOverride(method, contract);
        }

        return (ILatticeTreeAdmin)Activator.CreateInstance(type.CreateType()!)!;
    }

    private const string ViewName = "orders-by-region";
    private const string SourceTree = "orders";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        bool allow = true,
        IViewCatalog? viewCatalog = null,
        ILatticeViewFactory? viewFactory = null,
        bool viewsEnabled = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            restoreService: null,
            viewCatalog: viewCatalog,
            viewFactory: viewsEnabled ? (viewFactory ?? Substitute.For<ILatticeViewFactory>()) : null);

    private static IViewRegistryGrain WireRegistry(IGrainFactory factory, params RuntimeViewRegistration[] views)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(new List<RuntimeViewRegistration>(views));
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        return registry;
    }

    private static IViewMaintainerGrain WireMaintainer(
        IGrainFactory factory, string viewName, long lag = 0, string activeTreeId = "")
    {
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetLagAsync(Arg.Any<CancellationToken>()).Returns(lag);
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns(activeTreeId);
        factory.GetGrain<IViewMaintainerGrain>(viewName).Returns(maintainer);
        return maintainer;
    }

    private static RuntimeViewRegistration Registration(
        string viewName = ViewName,
        string sourceTreeId = SourceTree,
        bool isAggregation = false,
        bool accumulative = false,
        string? providerKey = "test-provider")
        => new()
        {
            ViewName = viewName,
            SourceTreeId = sourceTreeId,
            ProjectionTypeName = "Test.Projection",
            ProjectionVersion = "v1",
            IsAggregation = isAggregation,
            Accumulative = accumulative,
            ProjectionProviderKey = providerKey,
        };

    // ----- ListViews -----

    [Test]
    public async Task ListViewsAsync_projects_the_runtime_registrations()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory,
            Registration("v1", "s1", isAggregation: false, accumulative: true),
            Registration("v2", "s2", isAggregation: true));
        var facade = Create(factory);

        var catalog = await facade.ListViewsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Views, Has.Length.EqualTo(2));
            Assert.That(catalog.Views[0].ViewName, Is.EqualTo("v1"));
            Assert.That(catalog.Views[0].SourceTreeId, Is.EqualTo("s1"));
            Assert.That(catalog.Views[0].Accumulative, Is.True);
            Assert.That(catalog.Views[0].ProviderKey, Is.EqualTo("test-provider"));
            Assert.That(catalog.Views[0].ProjectionVersion, Is.EqualTo("v1"));
            Assert.That(catalog.Views[0].GetType().GetProperty("Payload"), Is.Null);
            Assert.That(catalog.Views[1].IsAggregation, Is.True);
        });
    }

    // ----- CreateView -----

    [Test]
    public async Task CreateViewAsync_authorizes_source_before_invoking_factory_and_returns_server_metadata()
    {
        var calls = new List<string>();
        var gate = Substitute.For<ILatticeAccessGate>();
        gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls.Add("authorize");
                return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
            });
        var factory = Substitute.For<IGrainFactory>();
        var source = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(SourceTree).Returns(source);
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("provider-v3");
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(new ViewRegistration(
            ViewName,
            SourceTree,
            projection,
            ProjectionProviderKey: "provider-a"));
        var maintainer = WireMaintainer(factory, ViewName, lag: 7, activeTreeId: "view-orders-by-region");
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        viewFactory.CreateAsync(
                source,
                ViewName,
                Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls.Add("factory");
                return Task.FromResult(Substitute.For<ILatticeView>());
            });
        var facade = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(gate),
            Options.Create(new LatticeApiTreeAdminOptions()),
            viewCatalog: catalog,
            viewFactory: viewFactory);

        var status = await facade.CreateViewAsync(
            ViewName,
            SourceTree,
            "provider-a",
            [1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(new[] { "authorize", "factory" }));
            Assert.That(status.ProviderKey, Is.EqualTo("provider-a"));
            Assert.That(status.ProjectionVersion, Is.EqualTo("provider-v3"));
            Assert.That(status.ApplyLag, Is.EqualTo(7));
            Assert.That(status.GetType().GetProperty("Payload"), Is.Null);
        });
        await viewFactory.Received(1).CreateAsync(
            source,
            ViewName,
            Arg.Is<LatticeRuntimeViewProjectionDescriptor>(d =>
                d.ProviderKey == "provider-a" && d.Payload.SequenceEqual(new byte[] { 1, 2, 3 })),
            Arg.Any<CancellationToken>());
        await maintainer.Received(1).GetLagAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateViewAsync_denied_source_does_not_invoke_factory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = Create(factory, allow: false, viewFactory: viewFactory);

        Assert.That(
            async () => await facade.CreateViewAsync(ViewName, SourceTree, "provider-a", []),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        viewFactory.DidNotReceive().CreateAsync(
            Arg.Any<ILattice>(),
            Arg.Any<string>(),
            Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
            Arg.Any<CancellationToken>());
    }

    [TestCase("", SourceTree, "provider-a")]
    [TestCase(ViewName, "", "provider-a")]
    [TestCase(ViewName, SourceTree, "")]
    [TestCase(ViewName, "view-source", "provider-a")]
    public void CreateViewAsync_invalid_names_do_not_invoke_factory(
        string viewName,
        string sourceTreeId,
        string providerKey)
    {
        var factory = Substitute.For<IGrainFactory>();
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = Create(factory, viewFactory: viewFactory);

        Assert.That(
            async () => await facade.CreateViewAsync(viewName, sourceTreeId, providerKey, []),
            Throws.TypeOf<ArgumentException>());
        viewFactory.DidNotReceive().CreateAsync(
            Arg.Any<ILattice>(),
            Arg.Any<string>(),
            Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateViewAsync_oversized_payload_does_not_invoke_factory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = Create(factory, viewFactory: viewFactory);

        Assert.That(
            async () => await facade.CreateViewAsync(
                ViewName,
                SourceTree,
                "provider-a",
                new byte[LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes + 1]),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        viewFactory.DidNotReceive().CreateAsync(
            Arg.Any<ILattice>(),
            Arg.Any<string>(),
            Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void ListViewsAsync_denied_by_telemetry_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ListViewsAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void ListViewsAsync_without_views_enabled_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, viewsEnabled: false);

        Assert.That(async () => await facade.ListViewsAsync(),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- GetViewStatus -----

    [Test]
    public async Task GetViewStatusAsync_resolves_via_registry_and_projects_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration(isAggregation: true));
        WireMaintainer(factory, ViewName, lag: 42, activeTreeId: "view-orders-by-region");
        var facade = Create(factory);

        var status = await facade.GetViewStatusAsync(ViewName);

        Assert.Multiple(() =>
        {
            Assert.That(status.ViewName, Is.EqualTo(ViewName));
            Assert.That(status.SourceTreeId, Is.EqualTo(SourceTree));
            Assert.That(status.IsAggregation, Is.True);
            Assert.That(status.ApplyLag, Is.EqualTo(42));
            Assert.That(status.ActiveTreeId, Is.EqualTo("view-orders-by-region"));
            Assert.That(status.ProviderKey, Is.EqualTo("test-provider"));
            Assert.That(status.ProjectionVersion, Is.EqualTo("v1"));
        });
    }

    [Test]
    public async Task GetViewStatusAsync_resolves_via_catalog_and_short_circuits_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = WireRegistry(factory); // empty registry
        var catalog = Substitute.For<IViewCatalog>();
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("startup-v2");
        catalog.TryGet(ViewName).Returns(new ViewRegistration(
            ViewName, SourceTree, projection));
        WireMaintainer(factory, ViewName);
        var facade = Create(factory, viewCatalog: catalog);

        var status = await facade.GetViewStatusAsync(ViewName);

        Assert.That(status.SourceTreeId, Is.EqualTo(SourceTree));
        Assert.That(status.ProviderKey, Is.Null);
        Assert.That(status.ProjectionVersion, Is.EqualTo("startup-v2"));
        await registry.DidNotReceive().ListAsync();
    }

    [Test]
    public void GetViewStatusAsync_unknown_view_throws_key_not_found()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory); // empty
        var facade = Create(factory);

        Assert.That(async () => await facade.GetViewStatusAsync("nope"),
            Throws.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public void GetViewStatusAsync_denied_by_read_gate_throws_and_does_not_dial_maintainer()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var maintainer = WireMaintainer(factory, ViewName);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetViewStatusAsync(ViewName),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        maintainer.DidNotReceive().GetLagAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetViewStatusAsync_empty_view_name_throws_argument_exception()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(async () => await facade.GetViewStatusAsync(""),
            Throws.TypeOf<ArgumentException>());
    }

    // ----- RebuildView -----

    [Test]
    public async Task RebuildViewAsync_admin_gated_rebuilds_then_returns_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var maintainer = WireMaintainer(factory, ViewName, lag: 0, activeTreeId: "view-orders-by-region");
        var facade = Create(factory);

        var status = await facade.RebuildViewAsync(ViewName);

        await maintainer.Received(1).RebuildAsync(Arg.Any<CancellationToken>());
        Assert.That(status.ActiveTreeId, Is.EqualTo("view-orders-by-region"));
    }

    [Test]
    public void RebuildViewAsync_denied_by_admin_gate_throws_and_does_not_rebuild()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var maintainer = WireMaintainer(factory, ViewName);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.RebuildViewAsync(ViewName),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        maintainer.DidNotReceive().RebuildAsync(Arg.Any<CancellationToken>());
    }

    // ----- ReconcileView -----

    [Test]
    public async Task ReconcileViewAsync_reports_drift_repaired()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var maintainer = WireMaintainer(factory, ViewName);
        maintainer.ReconcileAsync(Arg.Any<CancellationToken>()).Returns(true);
        var facade = Create(factory);

        var result = await facade.ReconcileViewAsync(ViewName);

        Assert.Multiple(() =>
        {
            Assert.That(result.ViewName, Is.EqualTo(ViewName));
            Assert.That(result.SourceTreeId, Is.EqualTo(SourceTree));
            Assert.That(result.DriftRepaired, Is.True);
        });
    }

    // ----- DropView -----

    [Test]
    public async Task DropViewAsync_admin_gated_deletes_via_factory()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = Create(factory, viewFactory: viewFactory);

        await facade.DropViewAsync(ViewName);

        await viewFactory.Received(1).DeleteAsync(ViewName, Arg.Any<CancellationToken>());
    }

    [Test]
    public void DropViewAsync_denied_by_admin_gate_throws_and_does_not_delete()
    {
        var factory = Substitute.For<IGrainFactory>();
        WireRegistry(factory, Registration());
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = Create(factory, allow: false, viewFactory: viewFactory);

        Assert.That(async () => await facade.DropViewAsync(ViewName),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        viewFactory.DidNotReceive().DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
