using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression tests for the structural tenant namespace (<c>t/{tenant}/{name}</c>)
/// guard on the tree-administration facade.
/// </summary>
/// <remarks>
/// The two paths disagreed: the data plane refuses a user-origin call naming a
/// <c>t/</c> id outright ("composed internally by the Lattice tenancy layer"),
/// while this facade accepted one and registered it. The result was a tree that
/// existed in the registry and the catalog but that every subsequent read and
/// write faulted on - a permanently unusable tree planted inside a tenant's
/// namespace. The guard admits exactly one legitimate source: an id whose
/// structural owner is the caller's own ambient active tenant, which is what
/// <c>LatticeTenantScopedTreeAdmin</c> composes.
/// </remarks>
[TestFixture]
public sealed class LatticeTreeAdminTenantNamespaceGuardTests
{
    private const string ViewName = "view-orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private sealed class FixedGate(bool allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()));

    [TestCase("t/globex/secrets")]
    [TestCase("t/acme/orders")]
    [TestCase("t/other-tenant/anything")]
    public void CreateTreeAsync_rejects_a_tenant_namespace_id_when_no_tenant_is_in_scope(string treeId)
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        // No ambient active tenant: nothing legitimately composes a t/ id here.
        Assert.That(
            async () => await facade.CreateTreeAsync(treeId),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CreateTreeAsync_rejects_another_tenants_namespace_even_with_a_tenant_in_scope()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);
        LatticeActiveTenantContext.Current = Acme;

        // The crux: operating as acme must not let a caller plant a tree inside
        // globex's structural namespace.
        Assert.That(
            async () => await facade.CreateTreeAsync("t/globex/planted-by-acme"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void The_guard_fires_before_any_grain_is_dialed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(
            async () => await facade.CreateTreeAsync("t/globex/secrets"),
            Throws.InstanceOf<ArgumentException>());

        Assert.That(factory.ReceivedCalls(), Is.Empty,
            "a reserved id is refused at the facade boundary, before any grain call");
    }

    [Test]
    public void The_guard_fires_even_when_authorization_would_allow()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory, allow: true);

        // Structural, not authorization-derived: a bootstrap administrator whose
        // gate allows everything is still refused, because the id is uncreatable
        // through this surface for anyone.
        Assert.That(
            async () => await facade.CreateTreeAsync("t/globex/secrets"),
            Throws.InstanceOf<ArgumentException>());
    }

    [TestCase("t/globex/secrets")]
    public void DeleteTreeAsync_rejects_a_foreign_tenant_namespace_id(string treeId)
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Assert.That(
            async () => await facade.DeleteTreeAsync(treeId),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CreateViewAsync_rejects_a_foreign_tenant_namespace_source()
    {
        var factory = Substitute.For<IGrainFactory>();
        var viewFactory = Substitute.For<ILatticeViewFactory>();
        var facade = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(true)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            viewFactory: viewFactory);

        // The caller-supplied source is the authorization boundary, so a view must
        // not be able to mirror another tenant's tree into an ordinary readable one.
        Assert.That(
            async () => await facade.CreateViewAsync(ViewName, "t/globex/secrets", "provider-a", []),
            Throws.InstanceOf<ArgumentException>());

        viewFactory.DidNotReceive().CreateAsync(
            Arg.Any<ILattice>(),
            Arg.Any<string>(),
            Arg.Any<LatticeRuntimeViewProjectionDescriptor>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_bare_tree_id_passes_the_namespace_guard()
    {
        // The guard must not disturb ordinary, non-tenant traffic. Asserted on the
        // rejection itself rather than on the call completing, because the grain
        // factory here is a stub: what matters is that a bare id is never refused
        // as reserved.
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(factory);

        Exception? caught = null;
        try
        {
            facade.CreateTreeAsync("ordinary-tree").GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.That(
            caught is ArgumentException { Message: var m } && m.Contains("reserved", StringComparison.Ordinal),
            Is.False,
            $"a bare tree id must not be rejected as reserved (got: {caught?.GetType().Name})");
    }
}
