using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Named regression tests for the app tree namespace composition invariant (epic
/// #2235, item F2), driven through the real tenancy
/// <see cref="TenantContextResolver"/>. An app tree <c>a/{app}/{tree}</c>
/// (<see cref="LatticeConstants.AppTreePrefix"/>) must resolve bare when no tenant
/// is asserted and compose to <c>t/{tenant}/a/{app}/{tree}</c> under a validated
/// tenant, so tenancy stays the outer axis and two tenants installing the same app
/// never share its trees.
/// </summary>
/// <remarks>
/// Every dependency is a substitute and the ambient tenant is set directly, so each
/// decision is exact and timing-independent.
/// </remarks>
[TestFixture]
[Category("Unit")]
public sealed class AppTreePrefixTenancyCompositionTests
{
    private const string AppTree = "a/crm/contacts";
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static ILatticeMembershipContext Membership(string subjectId)
    {
        var membership = Substitute.For<ILatticeMembershipContext>();
        var subject = new LatticeSubject(subjectId);

        membership.TryResolveCurrent(out Arg.Any<LatticeSubject>())
            .Returns(call =>
            {
                call[0] = subject;
                return true;
            });

        membership.ResolveCurrentAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSubject>(subject));

        return membership;
    }

    private static ITenantPolicyEngine Engine(string subjectId, TenantId admitted)
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant(Arg.Any<string>(), Arg.Any<TenantId>())
            .Returns(TenantAccessDecision.Deny("not an admin"));
        engine.ValidateActiveTenant(subjectId, admitted).Returns(TenantAccessDecision.Allow());
        return engine;
    }

    private static TenantId Resolve(TenantContextResolver resolver)
    {
        Assert.That(resolver.TryResolveCurrent(out var tenant), Is.True);
        return tenant;
    }

    /// <summary>
    /// Builds a resolver with an authoritative compiled-policy snapshot (no
    /// rebuild outstanding), so every test below decides from the substituted
    /// engine exactly as before issue #4065 - the registry-confirmation path
    /// taken during a non-authoritative snapshot is exercised separately by
    /// <see cref="TenantContextResolverRegistryLagTests"/>.
    /// </summary>
    private static TenantContextResolver CreateResolver(ITenantPolicyEngine engine, ILatticeMembershipContext membership) =>
        new(
            engine,
            membership,
            AuthoritativePolicy(),
            Substitute.For<ITenantRegistry>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantContextResolver>.Instance);

    private static CompiledTenantPolicySnapshotMaintainer AuthoritativePolicy() =>
        TenantPolicyEpochTestCluster
            .LeasedAsync(new TenantPolicyTestData.FakeTenantRegistry())
            .GetAwaiter()
            .GetResult();

    [Test]
    public void AppTreePrefix_composition_with_tenancy_unasserted_returns_the_bare_app_tree()
    {
        var resolver = CreateResolver(Substitute.For<ITenantPolicyEngine>(), Membership("alice"));

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(resolver), AppTree);

        Assert.That(effective, Is.SameAs(AppTree));
    }

    [Test]
    public void AppTreePrefix_composition_with_a_validated_tenant_yields_the_tenant_scoped_app_tree()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = CreateResolver(Engine("alice", Acme), Membership("alice"));

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(resolver), AppTree);

        Assert.That(effective, Is.EqualTo("t/acme/a/crm/contacts"));
    }

    [Test]
    public void AppTreePrefix_is_not_reserved_so_two_tenants_get_distinct_app_trees()
    {
        var acmeResolver = CreateResolver(Engine("alice", Acme), Membership("alice"));
        var betaResolver = CreateResolver(Engine("bob", Beta), Membership("bob"));

        LatticeActiveTenantContext.Current = Acme;
        var acme = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(acmeResolver), AppTree);
        LatticeActiveTenantContext.Current = Beta;
        var beta = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(betaResolver), AppTree);

        Assert.Multiple(() =>
        {
            Assert.That(acme, Is.EqualTo("t/acme/a/crm/contacts"));
            Assert.That(beta, Is.EqualTo("t/beta/a/crm/contacts"));
        });
    }

    [Test]
    public void AppTreePrefix_composition_attributes_the_composed_app_tree_to_its_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = CreateResolver(Engine("alice", Acme), Membership("alice"));

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(resolver), AppTree);

        Assert.That(LatticeTenantTrees.GetOwner(effective), Is.EqualTo(TreeOwnership.ForTenant(Acme)));
    }

    [Test]
    public void AppTreePrefix_composition_does_not_double_compose_the_tenants_own_app_tree()
    {
        const string composed = "t/acme/a/crm/contacts";
        LatticeActiveTenantContext.Current = Acme;
        var resolver = CreateResolver(Engine("alice", Acme), Membership("alice"));

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(Resolve(resolver), composed);

        Assert.That(effective, Is.SameAs(composed));
    }
}
