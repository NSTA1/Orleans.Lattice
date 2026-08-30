using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantGrantAdmin"/>, the five-operation
/// cross-tenant grant facade (list, offer, approve, reject, revoke). Covers the
/// two-step flow end to end, the idempotency of every mutation, the illegal
/// transitions each one refuses, the two-directional listing, the reserved-tenant
/// refusals, and the input guards. The per-operation authorization matrix - the
/// substance of the surface, since the tenant whose authority is required differs
/// per operation - lives in the sibling <c>.Authorization</c> partial, and the
/// concurrent-transition convergence in the <c>.Concurrency</c> partial. All
/// doubles are deterministic: no cluster, no threads, no timing.
/// </summary>
[TestFixture]
public sealed partial class LatticeTenantGrantAdminTests
{
    private const string Granter = "acme";
    private const string Grantee = "beta";
    private const string Scope = "orders";

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord Tenant(string tenantId, params string[] adminSubjects)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Stamp(1),
            "seed");

        var stamp = 2L;
        foreach (var subjectId in adminSubjects)
        {
            record.AddAdminSubject(subjectId, Stamp(stamp++), "seed");
        }

        return record;
    }

    /// <summary>A registry holding the granting and grantee tenants, each with its own admin.</summary>
    private static FakeTenantRegistry TwoTenantRegistry()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Tenant(Granter, "alice@acme.example"));
        registry.Seed(Tenant(Grantee, "bob@beta.example"));
        return registry;
    }

    /// <summary>
    /// Builds the facade with a uniformly allowing gate, which models a platform
    /// operator - the tier the behavioural tests want. The authorization matrix
    /// uses the control-plane-faithful gate instead.
    /// </summary>
    private static LatticeTenantGrantAdmin Admin(
        ITenantRegistry registry, bool authorized = true, ITenantAdminClock? clock = null) =>
        new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: authorized), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            clock ?? new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    private static async Task<LatticeTenantGrantAdmin> WithPendingGrantAsync(FakeTenantRegistry registry)
    {
        var admin = Admin(registry);
        await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.ReadWrite);
        return admin;
    }

    private static async Task<LatticeTenantGrantAdmin> WithActiveGrantAsync(FakeTenantRegistry registry)
    {
        var admin = await WithPendingGrantAsync(registry);
        await admin.ApproveGrantAsync(Granter, Grantee, Scope);
        return admin;
    }

    private static TenantGrantState EngineState(FakeTenantRegistry registry)
    {
        var grantId = CrossTenantGrant
            .Create(Grantee, TenantGranteeKind.Tenant, Scope, TenantGrantOperations.None).GrantId;
        return registry.Peek(Granter)!.TryGetGrant(grantId, out var grant)
            ? grant.State
            : throw new InvalidOperationException("no live grant on the granting tenant's record");
    }

    // ---- ctor guards -------------------------------------------------------

    [Test]
    public void Ctor_null_registry_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantGrantAdmin(
                null!, authorizer, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_authorizer_throws() =>
        Assert.That(
            () => new LatticeTenantGrantAdmin(
                new FakeTenantRegistry(), null!, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_clock_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantGrantAdmin(registry, authorizer, null!, Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_cluster_options_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantGrantAdmin(registry, authorizer, new IncrementingClock(), null!),
            Throws.ArgumentNullException);
    }

    // ---- the two-step flow -------------------------------------------------

    [Test]
    public async Task OfferGrantAsync_creates_the_grant_pending()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(result.Grant.GranterTenantId, Is.EqualTo(Granter));
            Assert.That(result.Grant.GranteeTenantId, Is.EqualTo(Grantee));
            Assert.That(result.Grant.Scope, Is.EqualTo(Scope));
            Assert.That(result.Grant.Operations, Is.EqualTo(TenantGrantAccess.Read));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Pending));
        });
    }

    [Test]
    public async Task OfferGrantAsync_writes_the_grant_to_the_granting_tenants_record()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);

        await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.Multiple(() =>
        {
            Assert.That(registry.Peek(Granter)!.GrantCount, Is.EqualTo(1));
            Assert.That(
                registry.Peek(Grantee)!.GrantCount,
                Is.Zero,
                "the grant lives on the granting tenant only, so there is no second copy to diverge");
        });
    }

    [Test]
    public async Task ApproveGrantAsync_activates_a_pending_grant()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        var result = await admin.ApproveGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Active));
        });
    }

    [Test]
    public async Task RejectGrantAsync_terminally_declines_a_pending_grant()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        var result = await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Rejected));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Rejected));
        });
    }

    [Test]
    public async Task RevokeGrantAsync_terminally_withdraws_an_active_grant()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);

        var result = await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Revoked));
        });
    }

    [Test]
    public async Task A_revoked_grant_can_be_offered_again()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);
        await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.Multiple(() =>
        {
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Pending));
        });
    }

    [Test]
    public async Task A_rejected_grant_can_be_offered_again()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);
        await admin.RejectGrantAsync(Granter, Grantee, Scope);

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
    }

    [Test]
    public async Task Offering_new_terms_on_an_unanswered_offer_amends_it_in_place()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Grant.Operations, Is.EqualTo(TenantGrantAccess.Read));
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(registry.Peek(Granter)!.GrantCount, Is.EqualTo(1));
        });
    }

    // ---- idempotency -------------------------------------------------------

    [Test]
    public async Task Re_sending_an_identical_offer_is_an_idempotent_no_op()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);
        await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.ReadWrite);
        var putsAfterFirst = registry.Puts;

        var result = await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.ReadWrite);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(registry.Puts, Is.EqualTo(putsAfterFirst), "an idempotent no-op must not write");
        });
    }

    [Test]
    public async Task Approving_an_already_active_grant_is_an_idempotent_no_op()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);
        var putsAfterApprove = registry.Puts;

        var result = await admin.ApproveGrantAsync(Granter, Grantee, Scope);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(registry.Puts, Is.EqualTo(putsAfterApprove));
        });
    }

    [Test]
    public async Task Rejecting_an_already_rejected_grant_is_an_idempotent_no_op()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);
        await admin.RejectGrantAsync(Granter, Grantee, Scope);

        var result = await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Changed, Is.False);
    }

    [Test]
    public async Task Revoking_an_already_revoked_grant_is_an_idempotent_no_op()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);
        await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        var result = await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        Assert.That(result.Changed, Is.False);
    }

    // ---- illegal transitions ----------------------------------------------

    [Test]
    public async Task Approving_a_rejected_grant_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);
        await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.CurrentState))
                .EqualTo(TenantGrantLifecycleState.Rejected));
    }

    [Test]
    public async Task Approving_a_revoked_grant_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);
        await admin.RevokeGrantAsync(Granter, Grantee, Scope);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.RequestedState))
                .EqualTo(TenantGrantLifecycleState.Active));
    }

    [Test]
    public async Task Rejecting_an_active_grant_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);

        Assert.That(
            async () => await admin.RejectGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>());
    }

    [Test]
    public async Task Revoking_a_pending_offer_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.CurrentState))
                .EqualTo(TenantGrantLifecycleState.Pending));
    }

    [Test]
    public async Task Revoking_a_rejected_grant_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);
        await admin.RejectGrantAsync(Granter, Grantee, Scope);

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>());
    }

    [Test]
    public async Task Offering_new_terms_over_a_live_agreement_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithActiveGrantAsync(registry);

        // Amending an approved grant would let the granting tenant redefine what
        // the grantee agreed to, without the grantee agreeing again.
        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<TenantGrantTransitionException>()
                .With.Property(nameof(TenantGrantTransitionException.CurrentState))
                .EqualTo(TenantGrantLifecycleState.Active));
    }

    [Test]
    public async Task An_illegal_transition_writes_nothing()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);
        var putsBefore = registry.Puts;

        Assert.That(
            async () => await admin.RevokeGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantTransitionException>());

        Assert.Multiple(() =>
        {
            Assert.That(registry.Puts, Is.EqualTo(putsBefore));
            Assert.That(EngineState(registry), Is.EqualTo(TenantGrantState.Pending));
        });
    }

    // ---- not found ---------------------------------------------------------

    [Test]
    public void Approving_an_unoffered_grant_reports_not_found()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantNotFoundException>());
    }

    [Test]
    public async Task Approving_a_grant_on_a_different_scope_reports_not_found()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, "invoices"),
            Throws.TypeOf<TenantGrantNotFoundException>());
    }

    [Test]
    public void An_unregistered_granting_tenant_is_reported_as_an_unoffered_grant()
    {
        // Reported identically to a genuinely unoffered grant, so a grantee admin
        // cannot use the surface to probe which tenants exist.
        var registry = new FakeTenantRegistry();
        registry.Seed(Tenant(Grantee, "bob@beta.example"));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.ApproveGrantAsync("ghost", Grantee, Scope),
            Throws.TypeOf<TenantGrantNotFoundException>());
    }

    [Test]
    public void Revoking_a_grant_from_an_unregistered_tenant_reports_not_found()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Tenant(Grantee, "bob@beta.example"));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.RevokeGrantAsync("ghost", Grantee, Scope),
            Throws.TypeOf<TenantGrantNotFoundException>());
    }

    // ---- listing -----------------------------------------------------------

    [Test]
    public async Task ListGrantsAsync_reports_a_granting_tenants_issued_grants()
    {
        var registry = TwoTenantRegistry();
        var admin = await WithPendingGrantAsync(registry);

        var report = await admin.ListGrantsAsync(Granter);

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo(Granter));
            Assert.That(report.Issued, Has.Count.EqualTo(1));
            Assert.That(report.Issued[0].GranteeTenantId, Is.EqualTo(Grantee));
            Assert.That(report.Issued[0].State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(report.Received, Is.Empty);
        });
    }

    [Test]
    public async Task ListGrantsAsync_gives_the_grantee_an_inbox_of_grants_offered_to_it()
    {
        var registry = TwoTenantRegistry();
        await WithPendingGrantAsync(registry);
        var admin = Admin(registry);

        var report = await admin.ListGrantsAsync(Grantee);

        Assert.Multiple(() =>
        {
            Assert.That(report.Issued, Is.Empty);
            Assert.That(report.Received, Has.Count.EqualTo(1));
            Assert.That(report.Received[0].GranterTenantId, Is.EqualTo(Granter));
            Assert.That(report.Received[0].State, Is.EqualTo(TenantGrantLifecycleState.Pending));
        });
    }

    [Test]
    public async Task ListGrantsAsync_reports_grants_in_every_lifecycle_state()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);
        await admin.OfferGrantAsync(Granter, Grantee, "orders", TenantGrantAccess.Read);
        await admin.OfferGrantAsync(Granter, Grantee, "invoices", TenantGrantAccess.Read);
        await admin.ApproveGrantAsync(Granter, Grantee, "invoices");
        await admin.OfferGrantAsync(Granter, Grantee, "ledger", TenantGrantAccess.Read);
        await admin.RejectGrantAsync(Granter, Grantee, "ledger");

        var report = await admin.ListGrantsAsync(Granter);

        Assert.Multiple(() =>
        {
            Assert.That(report.Issued, Has.Count.EqualTo(3));
            Assert.That(
                report.Issued.Select(static g => g.Scope),
                Is.EqualTo(new[] { "invoices", "ledger", "orders" }),
                "issued grants are ordered by grant id, which is derived from grantee and scope");
        });
    }

    [Test]
    public async Task ListGrantsAsync_orders_an_inbox_by_granting_tenant_then_grant_id()
    {
        var registry = TwoTenantRegistry();
        registry.Seed(Tenant("zeta", "zoe@zeta.example"));
        registry.Seed(Tenant("delta", "dan@delta.example"));
        var admin = Admin(registry);
        await admin.OfferGrantAsync("zeta", Grantee, "z-scope", TenantGrantAccess.Read);
        await admin.OfferGrantAsync("delta", Grantee, "d-scope", TenantGrantAccess.Read);
        await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.Read);

        var report = await admin.ListGrantsAsync(Grantee);

        Assert.That(
            report.Received.Select(static g => g.GranterTenantId),
            Is.EqualTo(new[] { "acme", "delta", "zeta" }),
            "the registry enumerates in no defined order, so the inbox must be sorted");
    }

    [Test]
    public async Task ListGrantsAsync_excludes_a_grant_offered_to_another_tenant()
    {
        var registry = TwoTenantRegistry();
        registry.Seed(Tenant("gamma", "gil@gamma.example"));
        var admin = Admin(registry);
        await admin.OfferGrantAsync(Granter, "gamma", Scope, TenantGrantAccess.Read);

        var report = await admin.ListGrantsAsync(Grantee);

        Assert.That(report.Received, Is.Empty);
    }

    [Test]
    public async Task ListGrantsAsync_excludes_subject_grantee_grants_from_the_issued_list()
    {
        // A subject-grantee grant has no counterparty tenant able to approve it and
        // is not administered through this surface.
        var registry = TwoTenantRegistry();
        registry.Peek(Granter)!.AddGrant(
            CrossTenantGrant.Create("carol@acme.example", TenantGranteeKind.Subject, Scope, TenantGrantOperations.Read),
            Stamp(50),
            "seed");
        var admin = Admin(registry);

        var report = await admin.ListGrantsAsync(Granter);

        Assert.That(report.Issued, Is.Empty);
    }

    [Test]
    public async Task ListGrantsAsync_reports_empty_lists_for_a_tenant_with_no_grants()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);

        var report = await admin.ListGrantsAsync(Granter);

        Assert.Multiple(() =>
        {
            Assert.That(report.Issued, Is.Empty);
            Assert.That(report.Received, Is.Empty);
        });
    }

    // ---- reserved tenant ---------------------------------------------------

    [Test]
    public void Offering_from_the_reserved_default_tenant_is_refused()
    {
        var registry = TwoTenantRegistry();
        registry.Seed(Tenant(TenantId.DefaultId, "alice@acme.example"));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.OfferGrantAsync(TenantId.DefaultId, Grantee, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<ReservedTenantOperationException>());
    }

    [Test]
    public void Offering_to_the_reserved_default_tenant_is_refused()
    {
        var registry = TwoTenantRegistry();
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, TenantId.DefaultId, Scope, TenantGrantAccess.Read),
            Throws.TypeOf<ReservedTenantOperationException>());
    }

    // ---- input guards ------------------------------------------------------

    [Test]
    public void OfferGrantAsync_rejects_an_empty_granter()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(string.Empty, Grantee, Scope, TenantGrantAccess.Read),
            Throws.ArgumentException);
    }

    [Test]
    public void OfferGrantAsync_rejects_an_empty_grantee()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, string.Empty, Scope, TenantGrantAccess.Read),
            Throws.ArgumentException);
    }

    [Test]
    public void OfferGrantAsync_rejects_a_whitespace_scope()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, "   ", TenantGrantAccess.Read),
            Throws.ArgumentException);
    }

    [Test]
    public void OfferGrantAsync_rejects_a_grant_that_would_authorize_nothing()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, TenantGrantAccess.None),
            Throws.ArgumentException);
    }

    [Test]
    public void OfferGrantAsync_rejects_operation_bits_this_build_does_not_recognise()
    {
        // Unknown bits are dropped rather than forwarded, so an operation set made
        // entirely of them authorizes nothing and is refused outright.
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Grantee, Scope, (TenantGrantAccess)64),
            Throws.ArgumentException);
    }

    [Test]
    public void A_grant_from_a_tenant_to_itself_is_refused()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(
            async () => await admin.OfferGrantAsync(Granter, Granter, Scope, TenantGrantAccess.Read),
            Throws.ArgumentException);
    }

    [Test]
    public void The_transitions_refuse_a_grant_from_a_tenant_to_itself()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.ApproveGrantAsync(Granter, Granter, Scope), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RejectGrantAsync(Granter, Granter, Scope), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RevokeGrantAsync(Granter, Granter, Scope), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_transitions_reject_a_malformed_tenant_id()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.ApproveGrantAsync("NOT A TENANT", Grantee, Scope), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RejectGrantAsync(Granter, "NOT A TENANT", Scope), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RevokeGrantAsync("NOT A TENANT", Grantee, Scope), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_transitions_reject_an_empty_scope()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.ApproveGrantAsync(Granter, Grantee, string.Empty), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RejectGrantAsync(Granter, Grantee, string.Empty), Throws.ArgumentException);
            Assert.That(
                async () => await admin.RevokeGrantAsync(Granter, Grantee, string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public void ListGrantsAsync_rejects_an_empty_tenant_id()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(async () => await admin.ListGrantsAsync(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void ListGrantsAsync_rejects_a_malformed_tenant_id()
    {
        var admin = Admin(TwoTenantRegistry());

        Assert.That(async () => await admin.ListGrantsAsync("NOT A TENANT"), Throws.ArgumentException);
    }
}
