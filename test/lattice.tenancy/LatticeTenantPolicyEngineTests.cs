using Microsoft.Extensions.Logging.Abstractions;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantPolicyEngine"/>: the three decision
/// surfaces (subject-to-allowed-tenants, active-tenant validation, cross-tenant
/// grant resolution), the epoch delegation, and the null-argument guards. The
/// engine is driven over a deterministically warmed snapshot maintainer, so no
/// test depends on timing.
/// </summary>
[TestFixture]
public sealed class LatticeTenantPolicyEngineTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");
    private static readonly TenantId Gamma = TenantId.Parse("gamma");

    private static async Task<LatticeTenantPolicyEngine> CreateEngineAsync(params TenantRecord[] records)
    {
        var registry = new FakeTenantRegistry();
        registry.Records.AddRange(records);
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return new LatticeTenantPolicyEngine(maintainer);
    }

    [Test]
    public async Task CurrentEpoch_delegates_to_the_maintainer()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        var engine = new LatticeTenantPolicyEngine(maintainer);

        Assert.That(engine.CurrentEpoch, Is.EqualTo(0), "a cold maintainer reports epoch 0");

        await maintainer.EnsureWarmAsync();

        Assert.That(engine.CurrentEpoch, Is.EqualTo(1), "the engine reflects the maintainer's epoch after warm-up");
    }

    // -------- ResolveAllowedTenants --------

    [Test]
    public async Task ResolveAllowedTenants_null_subject_throws()
    {
        var engine = await CreateEngineAsync();

        Assert.That(() => engine.ResolveAllowedTenants(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ResolveAllowedTenants_returns_empty_for_a_subject_with_no_tenants()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        Assert.That(engine.ResolveAllowedTenants("nobody"), Is.Empty);
    }

    [Test]
    public async Task ResolveAllowedTenants_returns_every_tenant_a_subject_administers()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"]),
            Record("beta", admins: ["alice"]),
            Record("gamma", admins: ["bob"]));

        Assert.That(engine.ResolveAllowedTenants("alice"), Is.EqualTo(new[] { Acme, Beta }));
    }

    // -------- ValidateActiveTenant --------

    [Test]
    public async Task ValidateActiveTenant_null_subject_throws()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        Assert.That(() => engine.ValidateActiveTenant(null!, Acme), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ValidateActiveTenant_allows_an_admin_of_an_active_tenant()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ValidateActiveTenant("alice", Acme);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public async Task ValidateActiveTenant_denies_the_uninitialised_tenant()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ValidateActiveTenant("alice", default);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task ValidateActiveTenant_denies_an_unregistered_tenant()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ValidateActiveTenant("alice", Beta);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not registered"));
        });
    }

    [Test]
    public async Task ValidateActiveTenant_denies_a_suspended_tenant()
    {
        var engine = await CreateEngineAsync(Record("acme", TenantStatus.Suspended, admins: ["alice"]));

        var decision = engine.ValidateActiveTenant("alice", Acme);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not active"));
        });
    }

    [Test]
    public async Task ValidateActiveTenant_denies_a_non_admin_subject()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ValidateActiveTenant("bob", Acme);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not an admin"));
        });
    }

    // -------- ResolveCrossTenantGrant --------

    [Test]
    public async Task ResolveCrossTenantGrant_null_scope_throws()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        Assert.That(() => engine.ResolveCrossTenantGrant(Beta, Acme, null!, TenantGrantOperations.Read), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_the_source_tenant_is_uninitialised()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ResolveCrossTenantGrant(default, Acme, "orders", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_the_target_tenant_is_uninitialised()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ResolveCrossTenantGrant(Beta, default, "orders", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_the_target_tenant_is_unregistered()
    {
        var engine = await CreateEngineAsync(Record("acme", admins: ["alice"]));

        var decision = engine.ResolveCrossTenantGrant(Beta, Gamma, "orders", TenantGrantOperations.Read);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("not registered"));
        });
    }

    [Test]
    public async Task ResolveCrossTenantGrant_allows_a_matching_grantee_scope_and_operation()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "orders", TenantGrantOperations.ReadWrite)]));

        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "orders", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_allows_a_scope_prefix_grant()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "orders", TenantGrantOperations.Read)]));

        // The grant's scope "orders" is a prefix of the requested "orders/2024".
        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "orders/2024", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_a_sibling_tree_that_only_shares_a_name_prefix()
    {
        // The gate passes the full composed tree id (t/{tenant}/{name}) as the
        // requested scope. A grant scoped to one tree must not leak to a distinct
        // sibling tree that merely shares a leading substring of its id.
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "t/acme/orders", TenantGrantOperations.Read)]));

        // "t/acme/orders-archive" is a different tree, not a hierarchical child of
        // "t/acme/orders"; the grant must not cover it.
        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "t/acme/orders-archive", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.False, "a grant for one tree must not cover a sibling tree sharing its name prefix");
    }

    [Test]
    public async Task ResolveCrossTenantGrant_allows_a_hierarchical_child_of_the_grant_scope()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "t/acme/orders", TenantGrantOperations.Read)]));

        // A '/'-delimited child of the grant scope is covered (segment-boundary prefix).
        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "t/acme/orders/2024", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.True, "a hierarchical child tree under the grant scope is covered");
    }

    [Test]
    public async Task ResolveCrossTenantGrant_allows_a_whole_tenant_prefix_scope_ending_in_separator()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "t/acme/", TenantGrantOperations.Read)]));

        // A grant scope that already ends in the segment separator covers every
        // tree under it.
        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "t/acme/orders", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.True, "a scope ending in the '/' separator covers trees beneath it");
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_no_grant_exists_for_the_source()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "orders", TenantGrantOperations.Read)]));

        // gamma holds no grant against acme.
        var decision = engine.ResolveCrossTenantGrant(Gamma, Acme, "orders", TenantGrantOperations.Read);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Does.Contain("no grant"));
        });
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_the_operation_is_not_granted()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "orders", TenantGrantOperations.Read)]));

        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "orders", TenantGrantOperations.Write);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_denies_when_the_scope_does_not_match()
    {
        var engine = await CreateEngineAsync(
            Record("acme", admins: ["alice"], grants: [TenantGrant("beta", "orders", TenantGrantOperations.Read)]));

        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "invoices", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public async Task ResolveCrossTenantGrant_does_not_resolve_a_subject_grant_as_a_tenant_grant()
    {
        var engine = await CreateEngineAsync(
            Record(
                "acme",
                admins: ["alice"],
                grants: [CrossTenantGrant.Create("beta", TenantGranteeKind.Subject, "orders", TenantGrantOperations.Read)]));

        // A subject-grantee grant named "beta" must not satisfy a tenant-to-tenant resolution.
        var decision = engine.ResolveCrossTenantGrant(Beta, Acme, "orders", TenantGrantOperations.Read);

        Assert.That(decision.Allowed, Is.False);
    }
}
