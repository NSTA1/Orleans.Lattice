using NSubstitute;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantContextResolver"/>, the seam that decides which
/// physical tree an unqualified, tenant-local name addresses. It is security
/// critical twice over: it composes the tenant's <c>t/{tenant}/{name}</c>
/// namespace (so a wrong answer hands one tenant another's data), and it
/// re-validates the caller-supplied active-tenant assertion (so an unauthorized
/// assertion can never select a namespace).
/// </summary>
/// <remarks>
/// Every dependency is a substitute and the ambient tenant is set directly, so
/// each decision is exact and timing-independent.
/// </remarks>
[TestFixture]
public sealed class TenantContextResolverTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static ILatticeMembershipContext Membership(string subjectId, bool warm = true)
    {
        var membership = Substitute.For<ILatticeMembershipContext>();
        var subject = subjectId.Length == 0 ? LatticeSubject.Anonymous : new LatticeSubject(subjectId);

        membership.TryResolveCurrent(out Arg.Any<LatticeSubject>())
            .Returns(call =>
            {
                if (!warm)
                {
                    call[0] = default(LatticeSubject);
                    return false;
                }

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

    // ---- No assertion: default-tenant adoption --------------------------

    [Test]
    public void TryResolveCurrent_with_no_active_tenant_resolves_the_default_tenant()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        var resolver = new TenantContextResolver(engine, Membership("alice"));

        var resolved = resolver.TryResolveCurrent(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True, "an unasserted call resolves synchronously");
            Assert.That(tenant, Is.EqualTo(TenantId.Default));
            Assert.That(engine.ReceivedCalls(), Is.Empty,
                "no assertion means nothing to validate, so a tenant-unaware client pays nothing");
        });
    }

    [Test]
    public void ComposeEffectiveTreeId_under_the_default_tenant_returns_the_bare_name()
    {
        var resolver = new TenantContextResolver(
            Substitute.For<ITenantPolicyEngine>(), Membership("alice"));
        resolver.TryResolveCurrent(out var tenant);

        Assert.That(LatticeTenantResolution.ComposeEffectiveTreeId(tenant, "orders"), Is.EqualTo("orders"));
    }

    // ---- Valid assertion: the name is scoped into the tenant namespace ----

    [Test]
    public void TryResolveCurrent_with_a_validated_assertion_resolves_that_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(Engine("alice", Acme), Membership("alice"));

        var resolved = resolver.TryResolveCurrent(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(tenant, Is.EqualTo(Acme));
        });
    }

    [Test]
    public void A_validated_assertion_scopes_an_unqualified_name_into_the_tenant_namespace()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(Engine("alice", Acme), Membership("alice"));
        resolver.TryResolveCurrent(out var tenant);

        // The whole point of the seam: two tenants asking for "orders" must not be
        // handed the same physical tree.
        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(tenant, "orders"),
            Is.EqualTo("t/acme/orders"));
    }

    [Test]
    public void Two_tenants_asking_for_the_same_name_get_different_trees()
    {
        var acmeResolver = new TenantContextResolver(Engine("alice", Acme), Membership("alice"));
        var betaResolver = new TenantContextResolver(Engine("bob", Beta), Membership("bob"));

        LatticeActiveTenantContext.Current = Acme;
        acmeResolver.TryResolveCurrent(out var acme);
        LatticeActiveTenantContext.Current = Beta;
        betaResolver.TryResolveCurrent(out var beta);

        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(acme, "orders"),
            Is.Not.EqualTo(LatticeTenantResolution.ComposeEffectiveTreeId(beta, "orders")));
    }

    [Test]
    public void A_validated_tenants_tree_is_attributed_to_that_tenant_for_quota_admission()
    {
        // Closes the loop on quota evasion. Admission charges the tree's owning
        // tenant, and usage is metered by structural ownership, so both only line
        // up once the resolver actually scopes the name. While the seam returned
        // the bare name, a tenant's traffic landed on a tree owned by the reserved
        // default tenant - which is Unbounded - so an authored quota could never
        // bind however small it was set.
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(Engine("alice", Acme), Membership("alice"));
        resolver.TryResolveCurrent(out var tenant);

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, "orders");
        var owner = LatticeTenantTrees.GetOwner(effective);

        Assert.Multiple(() =>
        {
            Assert.That(owner.IsTenantOwned, Is.True);
            Assert.That(owner.Tenant, Is.EqualTo(Acme),
                "the effective tree must be owned by the acting tenant, so usage meters and quotas bind to it");
            Assert.That(owner.Tenant.IsDefault, Is.False,
                "attributing tenant traffic to the unbounded default tenant is exactly the quota-evasion defect");
        });
    }

    // ---- Unauthorized assertion: fail closed ----------------------------

    [Test]
    public void An_assertion_the_subject_may_not_act_as_fails_closed()
    {
        // 'mallory' asserts acme but the engine admits only 'alice'.
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(Engine("alice", Acme), Membership("mallory"));

        resolver.TryResolveCurrent(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(tenant.Value, Is.Null, "a denied assertion resolves the 'no tenant' value");
            Assert.That(
                () => LatticeTenantResolution.ComposeEffectiveTreeId(tenant, "orders"),
                Throws.TypeOf<LatticeTenantAccessDeniedException>(),
                "which the composer turns into a fail-closed denial rather than a silent default");
        });
    }

    [Test]
    public void An_anonymous_caller_can_never_act_as_a_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant(Arg.Any<string>(), Arg.Any<TenantId>())
            .Returns(TenantAccessDecision.Allow());
        var resolver = new TenantContextResolver(engine, Membership(string.Empty));

        resolver.TryResolveCurrent(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(tenant.Value, Is.Null);
            Assert.That(engine.ReceivedCalls(), Is.Empty,
                "an anonymous subject is refused without consulting the engine at all");
        });
    }

    // ---- Cold membership: the async fallback ----------------------------

    [Test]
    public void TryResolveCurrent_falls_back_to_the_async_path_when_membership_is_cold()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            Engine("alice", Acme), Membership("alice", warm: false));

        var resolved = resolver.TryResolveCurrent(out _);

        Assert.That(resolved, Is.False, "a cold membership cache defers to ResolveCurrentAsync");
    }

    [Test]
    public async Task ResolveCurrentAsync_validates_through_the_cold_membership_path()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            Engine("alice", Acme), Membership("alice", warm: false));

        var tenant = await resolver.ResolveCurrentAsync();

        Assert.That(tenant, Is.EqualTo(Acme));
    }

    [Test]
    public async Task ResolveCurrentAsync_denies_an_unauthorized_assertion_on_the_cold_path()
    {
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            Engine("alice", Acme), Membership("mallory", warm: false));

        var tenant = await resolver.ResolveCurrentAsync();

        Assert.That(tenant.Value, Is.Null);
    }

    [Test]
    public async Task ResolveCurrentAsync_with_no_assertion_is_the_default_tenant()
    {
        var resolver = new TenantContextResolver(
            Substitute.For<ITenantPolicyEngine>(), Membership("alice", warm: false));

        Assert.That(await resolver.ResolveCurrentAsync(), Is.EqualTo(TenantId.Default));
    }

    // ---- Construction ---------------------------------------------------

    [Test]
    public void Constructor_rejects_a_null_dependency()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantContextResolver(null!, Membership("alice")),
                Throws.ArgumentNullException);
            Assert.That(() => new TenantContextResolver(Substitute.For<ITenantPolicyEngine>(), null!),
                Throws.ArgumentNullException);
        });
    }
}
