using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// The full per-operation authorization matrix for
/// <see cref="LatticeTenantGrantAdmin"/>. This is the substance of the surface:
/// unlike every sibling tenant-tier facade, the tenant whose admin authority is
/// required <b>differs per operation</b>, because a cross-tenant grant is an
/// agreement between two tenants and each step belongs to one side of it.
/// </summary>
/// <remarks>
/// <para>
/// Offering exposes the granting tenant's own data, so its admins may do it
/// alone. Activating exposes that data <em>to</em> the grantee, so approving and
/// rejecting belong to the grantee - an admin of the granting tenant must never be
/// able to approve its own offer, or the two-step agreement collapses into a
/// unilateral one. Revoking is open to both, because neither party should be
/// trapped in an agreement it wants out of. A third tenant's admin can do none of
/// it.
/// </para>
/// <para>
/// Every case is driven through <see cref="DefaultEffectAllowGate"/>, which
/// faithfully models the real core gate under <c>DefaultEffect = Allow</c>
/// (control-plane isolation on the reserved policy tree, fail-open only on
/// data-plane scopes), so the tier is proven to hold even on the most permissive
/// realistic cluster.
/// </para>
/// </remarks>
public sealed partial class LatticeTenantGrantAdminTests
{
    private const string Operator = "platform-operator";
    private const string GranterAdmin = "alice@acme.example";
    private const string GranteeAdmin = "bob@beta.example";
    private const string Outsider = "gamma";
    private const string OutsiderAdmin = "mallory@gamma.example";

    /// <summary>
    /// A registry with three independent tenants, so an attempted cross-side
    /// escalation has a real second and third tenant to be an admin of.
    /// </summary>
    private static FakeTenantRegistry ThreeTenantRegistry()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Tenant(Granter, GranterAdmin));
        registry.Seed(Tenant(Grantee, GranteeAdmin));
        registry.Seed(Tenant(Outsider, OutsiderAdmin));
        return registry;
    }

    /// <summary>
    /// Builds the facade as seen by <paramref name="callerSubjectId"/>, with the
    /// control-plane-faithful gate that grants operator authority only to
    /// <see cref="Operator"/>. Passing <see langword="null"/> registers no
    /// membership context, so the caller resolves to anonymous.
    /// </summary>
    private static LatticeTenantGrantAdmin AdminAs(FakeTenantRegistry registry, string? callerSubjectId) =>
        new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new DefaultEffectAllowGate(policyTreeAdminSubjectId: Operator),
                registry,
                callerSubjectId is null ? null : new FixedMembershipContext(new LatticeSubject(callerSubjectId))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    /// <summary>Seeds a pending offer directly on the record, bypassing the facade's own gate.</summary>
    private static void SeedPendingGrant(FakeTenantRegistry registry, string scope = Scope)
    {
        registry.Peek(Granter)!.OfferGrant(
            CrossTenantGrant.Create(Grantee, TenantGranteeKind.Tenant, scope, TenantGrantOperations.ReadWrite),
            Stamp(100),
            "seed");
    }

    /// <summary>Seeds an approved grant directly on the record, bypassing the facade's own gate.</summary>
    private static void SeedActiveGrant(FakeTenantRegistry registry, string scope = Scope)
    {
        SeedPendingGrant(registry, scope);
        var grantId = CrossTenantGrant
            .Create(Grantee, TenantGranteeKind.Tenant, scope, TenantGrantOperations.None).GrantId;
        registry.Peek(Granter)!.TransitionGrant(grantId, TenantGrantState.Active, Stamp(110), "seed");
    }

    // ---- tier 1: the platform operator -------------------------------------

    [Test]
    public async Task An_operator_may_drive_every_step_of_the_agreement()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, Operator);

        var offered = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);
        var approved = await admin.ApproveGrantAsync(Granter, Grantee, Scope);
        var revoked = await admin.RevokeGrantAsync(Granter, Grantee, Scope);
        var listed = await admin.ListGrantsAsync(Granter);

        Assert.Multiple(() =>
        {
            Assert.That(offered.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(approved.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(revoked.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
            Assert.That(listed.Issued, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void An_operator_naming_an_unregistered_granting_tenant_on_offer_is_told_not_found()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, Operator);

        Assert.That(
            async () => await admin.OfferGrantAsync("ghost", Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void An_operator_naming_an_unregistered_tenant_on_list_is_told_not_found()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, Operator);

        Assert.That(
            async () => await admin.ListGrantsAsync("ghost"), Throws.TypeOf<TenantNotFoundException>());
    }

    // ---- offer belongs to the granting tenant ------------------------------

    [Test]
    public async Task The_granting_tenants_admin_may_offer_its_own_data()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, GranterAdmin);

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
    }

    [Test]
    public void The_grantees_admin_may_not_offer_a_grant_from_the_granting_tenant()
    {
        // Offering from another tenant would let a grantee help itself to that
        // tenant's data, needing only its own approval to complete the agreement.
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, GranteeAdmin);

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_third_tenants_admin_may_not_offer_a_grant()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void An_anonymous_caller_may_not_offer_a_grant()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, callerSubjectId: null);

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- approve and reject belong to the grantee ---------------------------

    [Test]
    public async Task The_grantees_admin_may_approve_a_grant_offered_to_it()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, GranteeAdmin);

        var result = await admin.ApproveGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Active));
    }

    [Test]
    public async Task The_grantees_admin_may_reject_a_grant_offered_to_it()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, GranteeAdmin);

        var result = await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Rejected));
    }

    /// <summary>
    /// The single most important case in the whole issue: the tenant that offered
    /// the grant must not be able to complete the agreement by itself. If this
    /// fails, the approval step is decorative and offering alone is a unilateral
    /// cross-tenant escalation.
    /// </summary>
    [Test]
    public void The_granting_tenants_admin_may_not_approve_its_own_offer()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, GranterAdmin);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void The_granting_tenants_admin_may_not_reject_on_the_grantees_behalf()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, GranterAdmin);

        Assert.That(
            async () => await admin.RejectGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_third_tenants_admin_may_not_approve_a_grant_offered_to_another_tenant()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void An_anonymous_caller_may_not_approve_a_grant()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, callerSubjectId: null);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_refused_approval_does_not_change_the_grant()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, GranterAdmin);
        var putsBefore = registry.Puts;

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.Multiple(() =>
        {
            Assert.That(registry.Puts, Is.EqualTo(putsBefore));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Pending));
        });
    }

    // ---- revoke belongs to both parties ------------------------------------

    [Test]
    public async Task The_granting_tenants_admin_may_revoke_an_active_grant()
    {
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, GranterAdmin);

        var result = await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
    }

    [Test]
    public async Task The_grantees_admin_may_also_revoke_an_active_grant()
    {
        // Either party can walk away, so the grantee is not trapped holding access
        // it no longer wants to be accountable for.
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, GranteeAdmin);

        var result = await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
    }

    [Test]
    public void A_third_tenants_admin_may_not_revoke_a_grant_it_is_not_party_to()
    {
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void An_anonymous_caller_may_not_revoke_a_grant()
    {
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, callerSubjectId: null);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_refused_revocation_does_not_change_the_grant()
    {
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Active));
    }

    // ---- list belongs to the tenant whose grants are listed -----------------

    [Test]
    public async Task Each_partys_admin_may_list_its_own_side_of_the_agreement()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);

        var issued = await AdminAs(registry, GranterAdmin).ListGrantsAsync(Granter);
        var inbox = await AdminAs(registry, GranteeAdmin).ListGrantsAsync(Grantee);

        Assert.Multiple(() =>
        {
            Assert.That(issued.Issued, Has.Count.EqualTo(1));
            Assert.That(inbox.Received, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void An_admin_may_not_list_another_tenants_grants()
    {
        var registry = ThreeTenantRegistry();
        SeedPendingGrant(registry);
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.ListGrantsAsync(Granter),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void The_grantees_admin_may_not_list_the_granting_tenants_grants()
    {
        // Being party to one grant does not confer visibility of everything else
        // the granting tenant has shared.
        var registry = ThreeTenantRegistry();
        SeedActiveGrant(registry);
        var admin = AdminAs(registry, GranteeAdmin);

        Assert.That(
            async () => await admin.ListGrantsAsync(Granter),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void An_anonymous_caller_may_not_list_grants()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, callerSubjectId: null);

        Assert.That(
            async () => await admin.ListGrantsAsync(Granter),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- existence is never probeable --------------------------------------

    [Test]
    public void A_non_operator_naming_an_unregistered_tenant_is_told_denied_not_not_found()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, GranterAdmin);

        Assert.That(
            async () => await admin.ListGrantsAsync("ghost"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "a not-found here would turn the surface into a tenant-existence oracle");
    }

    [Test]
    public void A_non_operator_offering_from_an_unregistered_tenant_is_told_denied()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, GranterAdmin);

        Assert.That(
            async () => await admin.OfferGrantAsync("ghost", Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_non_operator_approving_a_grant_from_an_unregistered_tenant_is_told_denied()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, GranterAdmin);

        // Denied on the grantee side, before the granting tenant is ever looked up.
        Assert.That(
            async () => await admin.ApproveGrantAsync("ghost", Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_non_operator_revoking_a_grant_between_two_tenants_it_administers_neither_of_is_told_denied()
    {
        var registry = ThreeTenantRegistry();
        var admin = AdminAs(registry, OutsiderAdmin);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "the refusal must not depend on whether the grant exists");
    }

    // ---- the tier does not leak into the operator-only lifecycle tier -------

    /// <summary>
    /// The boundary this whole surface depends on: a tenant admin that may
    /// administer its own grants must <b>not</b> thereby be able to drive an
    /// operator-tier lifecycle mutation. Wiring the wrong identically-named
    /// <c>AuthorizeTenantAdminAsync</c> would break exactly one of these two
    /// directions, so both are asserted.
    /// </summary>
    [Test]
    public async Task A_tenant_admin_that_may_offer_a_grant_may_still_not_set_quotas()
    {
        var registry = ThreeTenantRegistry();
        var gate = new DefaultEffectAllowGate(policyTreeAdminSubjectId: Operator);
        var membership = new FixedMembershipContext(new LatticeSubject(GranterAdmin));

        var grantAdmin = new LatticeTenantGrantAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(gate, registry, membership),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

        var lifecycleAdmin = new LatticeTenantAdmin(
            registry,
            new TenantAdminAccessAuthorizer(gate, membership),
            new IncrementingClock(),
            new StubCascade(0),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            membership);

        var offered = await grantAdmin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.Multiple(() =>
        {
            Assert.That(offered.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(
                async () => await lifecycleAdmin.SetTenantQuotasAsync(
                    Granter, new TenantQuotasDescriptor { MaxTreeCount = 5 }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "grant administration must not confer operator-tier lifecycle authority");
        });
    }
}
