using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression tests for the <em>second</em> authorization boundary on the
/// <see cref="LatticeTreeAdmin"/> verbs that accept more than one caller-supplied
/// identifier, or that rebind an already-registered named resource.
/// <para>
/// Each of these verbs used to authorize only its first identifier, so a caller
/// holding <c>Admin</c> on a tree it legitimately owns could reach a tree it holds
/// nothing on: by aliasing its own tree onto the target (the data-plane gate is
/// evaluated against the <em>logical</em> id, before the registry resolves the
/// alias), by snapshotting into a caller-named destination the snapshot itself
/// creates and populates, or by re-pointing an existing materialised view - whose
/// registration is last-write-wins - at its own source. Every fixture below grants
/// the first identifier and withholds the second, and asserts the verb is refused
/// before any mutation is dialed.
/// </para>
/// Driven purely with substitutes and a per-tree access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminSecondIdentifierAuthorizationTests
{
    private const string Owned = "orders";
    private const string Foreign = "payroll";

    /// <summary>
    /// An access gate that allows exactly the tree ids it is seeded with and denies
    /// every other, so a test can grant one identifier of a two-identifier verb and
    /// withhold the other. It also records every tree id it was asked about, which
    /// is what proves the second identifier reached the gate at all.
    /// </summary>
    private sealed class PerTreeGate(params string[] allowedTreeIds) : ILatticeAccessGate
    {
        private readonly HashSet<string> _allowed = new(allowedTreeIds, StringComparer.Ordinal);

        public List<string> Requested { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Requested.Add(request.TreeId);
            return new(_allowed.Contains(request.TreeId)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny($"no grant on '{request.TreeId}'"));
        }
    }

    private static LatticeTreeAdmin Create(
        IGrainFactory factory,
        ILatticeAccessGate gate,
        IViewCatalog? viewCatalog = null,
        ILatticeViewFactory? viewFactory = null)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(gate),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            viewCatalog: viewCatalog,
            viewFactory: viewFactory);

    private static ILatticeRegistry Registry(IGrainFactory factory)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return registry;
    }

    // ----- SetTreeAlias: the alias TARGET is a second authorization boundary -----

    [Test]
    public void SetTreeAliasAsync_denies_an_alias_target_the_caller_cannot_administer()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        var gate = new PerTreeGate(Owned);
        var facade = Create(factory, gate);

        Assert.That(async () => await facade.SetTreeAliasAsync(Owned, Foreign),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain(Foreign));
        registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task SetTreeAliasAsync_authorizes_both_the_logical_id_and_the_alias_target()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Registry(factory);
        registry.ResolveAsync(Owned).Returns("orders-v2");
        var gate = new PerTreeGate(Owned, "orders-v2");
        var facade = Create(factory, gate);

        var result = await facade.SetTreeAliasAsync(Owned, "orders-v2");

        Assert.Multiple(() =>
        {
            Assert.That(result.PhysicalTreeId, Is.EqualTo("orders-v2"));
            Assert.That(result.IsAliased, Is.True);
            Assert.That(gate.Requested, Is.EqualTo(new[] { Owned, "orders-v2" }));
        });
        await registry.Received(1).SetAliasAsync(Owned, "orders-v2");
    }

    // ----- SnapshotTree: the DESTINATION is a second authorization boundary -----

    [Test]
    public void SnapshotTreeAsync_denies_a_destination_the_caller_cannot_administer()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Owned).Returns(lattice);
        var gate = new PerTreeGate(Owned);
        var facade = Create(factory, gate);

        Assert.That(async () => await facade.SnapshotTreeAsync(Owned, Foreign, TreeSnapshotMode.Online),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain(Foreign));
        lattice.DidNotReceive().SnapshotAsync(
            Arg.Any<string>(), Arg.Any<SnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SnapshotTreeAsync_authorizes_both_the_source_and_the_destination()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Owned).Returns(lattice);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        var gate = new PerTreeGate(Owned, "orders-snap");
        var facade = Create(factory, gate);

        await facade.SnapshotTreeAsync(Owned, "orders-snap", TreeSnapshotMode.Offline);

        Assert.That(gate.Requested, Does.Contain("orders-snap"));
        await lattice.Received(1).SnapshotAsync(
            "orders-snap", SnapshotMode.Offline, null, null, Arg.Any<CancellationToken>());
    }

    // ----- CreateView: the INCUMBENT registration is a second authorization boundary -----

    private static IViewCatalog CatalogWith(string viewName, string sourceTreeId)
    {
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("provider-v1");
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(viewName).Returns(new ViewRegistration(
            viewName,
            sourceTreeId,
            projection,
            ProjectionProviderKey: "provider-a"));
        return catalog;
    }

    [Test]
    public void CreateViewAsync_denies_rebinding_a_view_whose_incumbent_source_the_caller_cannot_administer()
    {
        var factory = Substitute.For<IGrainFactory>();
        var source = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Owned).Returns(source);
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var gate = new PerTreeGate(Owned);
        var facade = Create(
            factory, gate, viewCatalog: CatalogWith("salaries", Foreign), viewFactory: viewFactory);

        Assert.That(
            async () => await facade.CreateViewAsync("salaries", Owned, "provider-a", [1, 2, 3]),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain(Foreign));
        viewFactory.DidNotReceive().CreateAsync(
            Arg.Any<ILattice>(),
            Arg.Any<string>(),
            Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateViewAsync_allows_rebinding_when_the_incumbent_source_is_also_administered()
    {
        var factory = Substitute.For<IGrainFactory>();
        var source = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Owned).Returns(source);
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetLagAsync(Arg.Any<CancellationToken>()).Returns(0L);
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns(string.Empty);
        factory.GetGrain<IViewMaintainerGrain>("salaries").Returns(maintainer);
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        viewFactory.CreateAsync(
                source, "salaries", Arg.Any<LatticeRuntimeViewProjectionDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Substitute.For<ILatticeView>()));
        var gate = new PerTreeGate(Owned, "legacy-orders");
        var facade = Create(
            factory, gate, viewCatalog: CatalogWith("salaries", "legacy-orders"), viewFactory: viewFactory);

        await facade.CreateViewAsync("salaries", Owned, "provider-a", [1, 2, 3]);

        Assert.That(gate.Requested, Does.Contain("legacy-orders"));
        await viewFactory.Received(1).CreateAsync(
            source, "salaries", Arg.Any<LatticeRuntimeViewProjectionDescriptor>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateViewAsync_first_time_create_authorizes_only_the_source()
    {
        var factory = Substitute.For<IGrainFactory>();
        var source = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Owned).Returns(source);
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(new List<RuntimeViewRegistration>());
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetLagAsync(Arg.Any<CancellationToken>()).Returns(0L);
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns(string.Empty);
        factory.GetGrain<IViewMaintainerGrain>("fresh").Returns(maintainer);
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        viewFactory.CreateAsync(
                source, "fresh", Arg.Any<LatticeRuntimeViewProjectionDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Substitute.For<ILatticeView>()));
        var catalog = Substitute.For<IViewCatalog>();
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("provider-v1");
        // Absent on the pre-create incumbent probe, present on the post-create status
        // read - the same transition the real catalog makes when the factory registers.
        catalog.TryGet("fresh").Returns(
            null,
            new ViewRegistration("fresh", Owned, projection, ProjectionProviderKey: "provider-a"));
        var gate = new PerTreeGate(Owned);
        var facade = Create(factory, gate, viewCatalog: catalog, viewFactory: viewFactory);

        await facade.CreateViewAsync("fresh", Owned, "provider-a", [1, 2, 3]);

        Assert.That(gate.Requested, Is.EqualTo(new[] { Owned }));
    }
}
