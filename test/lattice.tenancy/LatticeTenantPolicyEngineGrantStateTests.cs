using Microsoft.Extensions.Logging.Abstractions;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// The load-bearing security property of the two-step cross-tenant grant flow,
/// asserted at the <b>policy engine</b> - the single seam at which a grant becomes
/// an allow - rather than only at the control facade.
/// </summary>
/// <remarks>
/// Only an <see cref="TenantGrantState.Active"/> grant may authorize. If a
/// <see cref="TenantGrantState.Pending"/> grant ever authorized anything, the
/// grantee's approval step would be decorative and the granting tenant could widen
/// another tenant's access by offering alone; if a
/// <see cref="TenantGrantState.Revoked"/> one did, a party that walked away would
/// still be exposing its data. Each case below holds the operation and scope match
/// constant and varies only the state, so nothing but the lifecycle gate can be
/// what makes the difference.
/// </remarks>
[TestFixture]
public sealed class LatticeTenantPolicyEngineGrantStateTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private const string Scope = "orders";

    private static async Task<LatticeTenantPolicyEngine> EngineWithGrantInAsync(TenantGrantState state)
    {
        // The granting tenant's record holds the grant. It is written through the
        // real record API, then transitioned into the state under test, so the
        // compiled snapshot sees exactly what production would.
        var record = Record("acme", admins: ["alice"]);
        var offered = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, Scope, TenantGrantOperations.ReadWrite);

        switch (state)
        {
            case TenantGrantState.Active:
                record.OfferGrant(offered, Clock(100), "granter");
                record.TransitionGrant(offered.GrantId, TenantGrantState.Active, Clock(110), "grantee");
                break;
            case TenantGrantState.Pending:
                record.OfferGrant(offered, Clock(100), "granter");
                break;
            case TenantGrantState.Rejected:
                record.OfferGrant(offered, Clock(100), "granter");
                record.TransitionGrant(offered.GrantId, TenantGrantState.Rejected, Clock(110), "grantee");
                break;
            case TenantGrantState.Revoked:
                record.OfferGrant(offered, Clock(100), "granter");
                record.TransitionGrant(offered.GrantId, TenantGrantState.Active, Clock(110), "grantee");
                record.TransitionGrant(offered.GrantId, TenantGrantState.Revoked, Clock(120), "granter");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state));
        }

        var registry = new FakeTenantRegistry();
        registry.Records.Add(record);
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return new LatticeTenantPolicyEngine(maintainer);
    }

    private static async Task<TenantAccessDecision> ResolveAsync(TenantGrantState state)
    {
        var engine = await EngineWithGrantInAsync(state);
        return engine.ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.Read);
    }

    [Test]
    public async Task An_active_grant_allows()
    {
        Assert.That((await ResolveAsync(TenantGrantState.Active)).Allowed, Is.True);
    }

    [Test]
    public async Task A_pending_grant_denies()
    {
        var decision = await ResolveAsync(TenantGrantState.Pending);

        Assert.That(
            decision.Allowed,
            Is.False,
            "an offer the grantee has not approved must authorize nothing, or step one alone "
            + "is a unilateral cross-tenant escalation");
    }

    [Test]
    public async Task A_rejected_grant_denies()
    {
        Assert.That((await ResolveAsync(TenantGrantState.Rejected)).Allowed, Is.False);
    }

    [Test]
    public async Task A_revoked_grant_denies()
    {
        Assert.That(
            (await ResolveAsync(TenantGrantState.Revoked)).Allowed,
            Is.False,
            "a party that walked away must stop authorizing");
    }

    [Test]
    public async Task A_pending_grant_denies_every_operation_it_nominally_covers()
    {
        var engine = await EngineWithGrantInAsync(TenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(engine.ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.Read).Allowed, Is.False);
            Assert.That(engine.ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.Write).Allowed, Is.False);
            Assert.That(engine.ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.ReadWrite).Allowed, Is.False);
        });
    }

    [Test]
    public async Task A_pending_grant_denies_a_hierarchical_child_scope_too()
    {
        // Scope coverage is generous by design, so the state gate must apply
        // before it rather than only to an exact scope match.
        var engine = await EngineWithGrantInAsync(TenantGrantState.Pending);

        Assert.That(
            engine.ResolveCrossTenantGrant(Beta, Acme, "orders/2024", TenantGrantOperations.Read).Allowed,
            Is.False);
    }

    [Test]
    public async Task A_denial_for_a_pending_grant_names_the_missing_active_grant()
    {
        var decision = await ResolveAsync(TenantGrantState.Pending);

        Assert.That(decision.Reason, Does.Contain("no active grant"));
    }

    /// <summary>
    /// The end-to-end lifecycle as the engine sees it, over one grant: offered and
    /// denying, approved and allowing, revoked and denying again.
    /// </summary>
    [Test]
    public async Task The_grant_only_authorizes_between_approval_and_revocation()
    {
        var record = Record("acme", admins: ["alice"]);
        var offered = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, Scope, TenantGrantOperations.Read);

        record.OfferGrant(offered, Clock(100), "granter");
        var whileOffered = await ResolveOnAsync(record);

        record.TransitionGrant(offered.GrantId, TenantGrantState.Active, Clock(110), "grantee");
        var whileActive = await ResolveOnAsync(record);

        record.TransitionGrant(offered.GrantId, TenantGrantState.Revoked, Clock(120), "granter");
        var whileRevoked = await ResolveOnAsync(record);

        Assert.Multiple(() =>
        {
            Assert.That(whileOffered.Allowed, Is.False, "offered");
            Assert.That(whileActive.Allowed, Is.True, "approved");
            Assert.That(whileRevoked.Allowed, Is.False, "revoked");
        });
    }

    /// <summary>
    /// Compiles a fresh snapshot over <paramref name="record"/>'s current contents
    /// and resolves the grant against it, so each step of the lifecycle is observed
    /// through a deterministically warmed engine rather than a refresh race.
    /// </summary>
    private static async Task<TenantAccessDecision> ResolveOnAsync(TenantRecord record)
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(record.Clone());
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return new LatticeTenantPolicyEngine(maintainer)
            .ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.Read);
    }

    [Test]
    public async Task A_grant_issued_through_the_pre_existing_single_step_path_still_allows()
    {
        // AddGrant is the shipped in-process issue path and lands an active grant,
        // so an upgraded cluster keeps authorizing exactly what it did before.
        var engine = await EngineWithGrantAddedDirectlyAsync();

        Assert.That(
            engine.ResolveCrossTenantGrant(Beta, Acme, Scope, TenantGrantOperations.Read).Allowed, Is.True);
    }

    private static async Task<LatticeTenantPolicyEngine> EngineWithGrantAddedDirectlyAsync()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(
            Record(
                "acme",
                admins: ["alice"],
                grants: [TenantGrant("beta", Scope, TenantGrantOperations.ReadWrite)]));
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return new LatticeTenantPolicyEngine(maintainer);
    }
}
