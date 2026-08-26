using NSubstitute;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantGateEnforcer"/>: the active
/// <see cref="ITenantGateEnforcer"/> that applies tenant isolation at the auth
/// gate. The tenant-policy engine and the residency resolver are substituted and
/// the ambient active tenant is set directly, so every decision is exact and
/// timing-independent. The tree ownership is derived from the tree id by
/// <see cref="LatticeTenantTrees.GetOwner"/>, so ids are chosen to exercise each
/// ownership shape (platform, tenant-scoped, bare legacy).
/// </summary>
[TestFixture]
public sealed class TenantGateEnforcerTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private const string AcmeTree = "t/acme/orders";
    private const string BetaTree = "t/beta/orders";
    private const string LegacyTree = "app";
    private const string PlatformTree = "sys-foo";

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static TenantGateEnforcer CreateEnforcer(
        ITenantPolicyEngine engine,
        ITenantResidencyResolver? residency = null) =>
        new(engine, residency ?? new NullTenantResidencyResolver());

    private static LatticeAccessRequest Request(
        string treeId,
        LatticeOperation operation = LatticeOperation.Read,
        string subjectId = "alice") =>
        new(treeId, operation, new LatticeSubject(subjectId), "k");

    // ---- IsActive -------------------------------------------------------

    [Test]
    public void IsActive_is_true_for_the_active_enforcer()
    {
        var enforcer = CreateEnforcer(Substitute.For<ITenantPolicyEngine>());

        Assert.That(enforcer.IsActive, Is.True);
    }

    // ---- Platform / compatibility carve-outs ----------------------------

    [Test]
    public void Enforce_platform_tree_allows_without_consulting_the_engine()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        var enforcer = CreateEnforcer(engine);
        var request = Request(PlatformTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True, "a platform-owned system tree is not tenant data");
        Assert.That(engine.ReceivedCalls(), Is.Empty, "the engine is never consulted for a platform tree");
    }

    [Test]
    public void Enforce_bare_legacy_tree_with_no_active_tenant_allows()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        var enforcer = CreateEnforcer(engine);
        var request = Request(LegacyTree);

        // No active tenant is set (TearDown-cleared default).
        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True, "pre-tenancy traffic on a legacy tree is admitted");
        Assert.That(engine.ReceivedCalls(), Is.Empty, "the compatibility carve-out consults no engine");
    }

    // ---- (1)+(2) active tenant owns the tree ----------------------------

    [Test]
    public void Enforce_active_tenant_owns_tree_and_validation_allows_allows()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public void Enforce_active_tenant_owns_tree_but_validation_denies_denies()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Deny("not an admin of 'acme'"));
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not an admin"));
        });
    }

    // ---- (2) no active tenant on a tenant-owned tree --------------------

    [Test]
    public void Enforce_no_active_tenant_on_a_tenant_tree_denies_via_the_uninitialised_contract()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", default).Returns(
            TenantAccessDecision.Deny("the uninitialised 'no tenant' value cannot be an active tenant"));
        var enforcer = CreateEnforcer(engine);
        // No active tenant selected, but the tree is tenant-owned.
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False, "a tenant-owned tree with no active tenant fails closed");
            Assert.That(decision.Reason, Does.Contain("no tenant"));
        });
    }

    [Test]
    public void Enforce_no_active_tenant_allows_when_the_default_contract_admits()
    {
        // Defensive branch coverage: the real engine denies the uninitialised
        // tenant, but the enforcer honours an allow if the contract ever returns
        // one.
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", default).Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True);
    }

    // ---- (3) cross-tenant crossing --------------------------------------

    [Test]
    public void Enforce_active_tenant_does_not_own_tree_denies_without_a_grant()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Deny("no grant from 'beta' to 'acme'"));
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree);

        var decision = enforcer.Enforce(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("no grant"));
        });
        engine.Received().ValidateActiveTenant("alice", Acme);
    }

    [Test]
    public void Enforce_cross_tenant_denies_a_subject_that_may_not_act_as_the_active_tenant()
    {
        // Security regression: the active tenant arrives as a caller-supplied
        // assertion (the `lattice-active-tenant` header), so the subject's right
        // to act as it must be validated on the cross-tenant branch too. When it
        // was validated only on the owned-tree branch, any authenticated subject
        // could assert a tenant it has no membership of and consume that
        // tenant's inbound cross-tenant grants - reading (or, with a write
        // grant, writing) the granting tenant's data. The grant itself resolves
        // fine here; only the subject's right to wear the tenant is missing.
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("mallory", Acme)
            .Returns(TenantAccessDecision.Deny("subject is not a member of 'acme'"));
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree, subjectId: "mallory");

        var decision = enforcer.Enforce(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False,
                "an unvalidated active tenant can never consume another tenant's cross-tenant grant");
            Assert.That(decision.Reason, Does.Contain("not a member"));
        });
    }

    [Test]
    public void Enforce_cross_tenant_validates_the_active_tenant_before_resolving_a_grant()
    {
        // Fail-closed ordering: a subject that may not act as the asserted
        // active tenant is refused before any grant is resolved, so a grant
        // lookup can never admit an identity that was never validated.
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("mallory", Acme)
            .Returns(TenantAccessDecision.Deny("subject is not a member of 'acme'"));
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree, subjectId: "mallory");

        enforcer.Enforce(in request);

        engine.DidNotReceive().ResolveCrossTenantGrant(
            Arg.Any<TenantId>(), Arg.Any<TenantId>(), Arg.Any<string>(), Arg.Any<TenantGrantOperations>());
    }

    [Test]
    public void Enforce_cross_tenant_grant_allows_crossing()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True);
    }

    // ---- (3) grant-operation classification (fail-closed) ---------------

    [Test]
    public void Enforce_read_only_operation_requests_a_read_grant()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree, LatticeOperation.Read);

        enforcer.Enforce(in request);

        engine.Received().ResolveCrossTenantGrant(Acme, Beta, BetaTree, TenantGrantOperations.Read);
    }

    [Test]
    public void Enforce_write_operation_requests_a_write_grant()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Allow());
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree, LatticeOperation.Write);

        enforcer.Enforce(in request);

        engine.Received().ResolveCrossTenantGrant(Acme, Beta, BetaTree, TenantGrantOperations.Write);
    }

    [Test]
    public void Enforce_empty_operation_mask_requests_a_write_grant_fail_closed()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Deny("no grant"));
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree, LatticeOperation.None);

        enforcer.Enforce(in request);

        engine.Received().ResolveCrossTenantGrant(Acme, Beta, BetaTree, TenantGrantOperations.Write);
    }

    // ---- (4) residency / online -----------------------------------------

    [Test]
    public void Enforce_residency_not_online_denies_an_owned_tree()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(false);
        var enforcer = CreateEnforcer(engine, residency);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not online"));
        });
    }

    [Test]
    public void Enforce_residency_online_allows_an_owned_tree()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(true);
        var enforcer = CreateEnforcer(engine, residency);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public void Enforce_null_residency_default_allows_an_owned_tree()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        // NullTenantResidencyResolver (IsActive false) is the default.
        var enforcer = CreateEnforcer(engine);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(AcmeTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True, "an absent residency seam never denies");
    }

    [Test]
    public void Enforce_residency_gates_the_cross_tenant_path_too()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        engine.ResolveCrossTenantGrant(Acme, Beta, BetaTree, Arg.Any<TenantGrantOperations>())
            .Returns(TenantAccessDecision.Allow());
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(false);
        var enforcer = CreateEnforcer(engine, residency);
        LatticeActiveTenantContext.Current = Acme;
        var request = Request(BetaTree);

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.False, "a granted crossing is still gated on residency");
    }
}
