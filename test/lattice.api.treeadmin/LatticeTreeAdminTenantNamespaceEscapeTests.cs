using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression coverage for tenant confinement across the reserved <c>sys-</c>
/// system-data namespace.
/// </summary>
/// <remarks>
/// <para>
/// Tenant scoping is applied by composing the caller's active tenant into the tree
/// id, and that composition deliberately passes an already-qualified name through
/// unchanged so a name is never double-composed. The reserved <c>sys-</c>
/// namespace counts as already-qualified, which is correct for the first-party
/// add-ons that own those trees - but it meant a <b>tenant</b> caller naming a
/// <c>sys-</c> tree had its name returned uncomposed, landing it in the global
/// platform namespace instead of its own.
/// </para>
/// <para>
/// The tenant-scoped administration facade is unaffected (it composes
/// unconditionally through <c>LatticeTenantTrees.Compose</c>), but this facade is
/// itself tenant-aware and directly reachable, and nothing between the caller and
/// the registry refused the namespace. Three consequences followed, one per strand
/// of tenant isolation:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>Quota evasion.</b> Per-tenant tree counts and storage footprint are
///     accounted by enumerating the tenant's <c>t/{tenant}/</c> prefix, so a tree
///     parked under <c>sys-</c> is invisible to that accounting. Neither
///     <c>MaxTreeCount</c> nor the footprint ceiling can bind on a tree they
///     cannot see, so the ceiling was evadable by naming alone.
///   </description></item>
///   <item><description>
///     <b>Cross-tenant data.</b> The uncomposed id is global, so two different
///     tenants naming the same <c>sys-</c> tree address one physical tree - a
///     direct read/write channel between tenants that are supposed to be unable to
///     observe one another at all.
///   </description></item>
///   <item><description>
///     <b>Shadowing first-party state.</b> The namespace holds the identity,
///     authorization, and tenant-registry stores, so the name a tenant chooses can
///     collide with one an add-on has not registered yet.
///   </description></item>
/// </list>
/// <para>
/// The guard refuses the namespace only for a genuinely confined caller - a
/// non-default active tenant, outside a system-origin scope - and it sits on the
/// single tenant-resolution seam every facade passes through, so a facade added
/// later inherits it rather than needing its own copy. A host with no tenancy
/// registered has no active tenant, so it never reaches the check and its
/// behaviour is unchanged; the first-party add-ons that legitimately administer
/// these trees run system-origin and are exempt for the same reason they are
/// exempt from every other reserved-namespace rejection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeTreeAdminTenantNamespaceEscapeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private sealed class AllowingGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new AllowingGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new AmbientTenantContextResolver(),
            restoreService: null,
            viewCatalog: null,
            viewFactory: null,
            tagIndexFactory: null,
            admission: null);

    private static (LatticeTreeAdmin Facade, ILatticeRegistry Registry) CreateWithRegistry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (Create(factory), registry);
    }

    private static IEnumerable<string> SystemDataNames()
    {
        yield return "sys-evil";
        yield return "sys-auth-policy";
        yield return "sys-membership-groups";
        yield return "sys-tenant-registry";
    }

    // ----- the defect: a tenant could park a tree in the global namespace -----

    [Test]
    public void CreateTreeAsync_refuses_the_system_data_namespace_for_a_tenant_caller(
        [ValueSource(nameof(SystemDataNames))] string name)
    {
        var (facade, registry) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = Acme;

        Assert.That(
            async () => await facade.CreateTreeAsync(name),
            Throws.TypeOf<LatticeTenantAccessDeniedException>().With.Message.Contains("sys-"),
            "a confined tenant naming a 'sys-' tree escaped its own namespace entirely: "
            + "the id is returned uncomposed, so the tree is global, uncounted against the "
            + "tenant's quota, and shared with every other tenant that picks the same name");

        Assert.That(
            registry.ReceivedCalls().Any(),
            Is.False,
            "the escape must be refused before any registry work");
    }

    [Test]
    public void DeleteTreeAsync_refuses_the_system_data_namespace_for_a_tenant_caller()
    {
        var (facade, _) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = Acme;

        // The guard is shared by every administration verb, so a tenant cannot
        // reach an existing first-party tree through a different door either.
        Assert.That(
            async () => await facade.DeleteTreeAsync("sys-auth-policy"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>().With.Message.Contains("sys-"));
    }

    // ----- the guard must not fire for callers that are not confined -----

    [Test]
    public void CreateTreeAsync_still_allows_the_system_data_namespace_with_no_active_tenant()
    {
        var (facade, _) = CreateWithRegistry();

        // A host that has not registered tenancy has no active tenant, so the
        // reserved-namespace behaviour must be byte-for-byte what it was before.
        Assert.That(
            async () => await facade.CreateTreeAsync("sys-auth-policy"),
            Throws.Nothing);
    }

    [Test]
    public void CreateTreeAsync_still_allows_the_system_data_namespace_for_the_default_tenant()
    {
        var (facade, _) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = TenantId.Default;

        // The reserved default tenant is the "tenancy off / adopted" identity; it
        // is not a confined tenant and must not be newly restricted.
        Assert.That(
            async () => await facade.CreateTreeAsync("sys-auth-policy"),
            Throws.Nothing);
    }

    [Test]
    public void CreateTreeAsync_still_allows_the_system_data_namespace_under_system_origin()
    {
        var (facade, _) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = Acme;

        // A first-party add-on administering its own store runs system-origin, and
        // is exempt here exactly as it is from every other reserved-namespace guard.
        using var _scope = LatticeAccessGateContext.EnterSystemOrigin();
        Assert.That(
            async () => await facade.CreateTreeAsync("sys-auth-policy"),
            Throws.Nothing);
    }

    [Test]
    public void CreateTreeAsync_still_refuses_the_control_namespace_for_a_tenant_caller()
    {
        var (facade, _) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = Acme;

        // The pre-existing '_lattice_' rejection is unchanged.
        Assert.That(
            async () => await facade.CreateTreeAsync(LatticeConstants.SystemTreePrefix + "wal"),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void CreateTreeAsync_still_allows_an_ordinary_tenant_local_name()
    {
        var (facade, _) = CreateWithRegistry();

        LatticeActiveTenantContext.Current = Acme;

        // The guard is namespace-shaped only: an ordinary name still composes and
        // still succeeds. A name that merely contains "sys-" is not in the
        // namespace and must not be caught.
        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTreeAsync("orders"), Throws.Nothing);
            Assert.That(async () => await facade.CreateTreeAsync("analysis-sys-dump"), Throws.Nothing);
        });
    }
}
