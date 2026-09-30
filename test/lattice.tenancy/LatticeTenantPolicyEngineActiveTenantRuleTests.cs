using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the explicit-policy overload
/// <see cref="LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy, string, TenantId)"/>:
/// the single home of the active-tenant rule, applied by the engine to its snapshot
/// and by the data-plane gate to a policy compiled from the authoritative registry
/// record while that snapshot is not authoritative (issue #4053). The parity test
/// pins that the two paths cannot diverge.
/// </summary>
[TestFixture]
public sealed class LatticeTenantPolicyEngineActiveTenantRuleTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static CompiledTenantPolicy Policy(params TenantRecord[] records) => CompiledTenantPolicy.Compile(records);

    [Test]
    public void ValidateActiveTenant_null_policy_throws()
    {
        Assert.That(
            () => LatticeTenantPolicyEngine.ValidateActiveTenant(null!, "alice", Acme),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ValidateActiveTenant_null_subject_throws()
    {
        Assert.That(
            () => LatticeTenantPolicyEngine.ValidateActiveTenant(Policy(Record("acme", admins: ["alice"])), null!, Acme),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ValidateActiveTenant_allows_an_admin_of_an_active_tenant()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(Policy(Record("acme", admins: ["alice"])), "alice", Acme);

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.Reason, Is.Null);
    }

    [Test]
    public void ValidateActiveTenant_denies_the_uninitialised_tenant()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(Policy(Record("acme", admins: ["alice"])), "alice", default);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("'no tenant'"));
    }

    [Test]
    public void ValidateActiveTenant_denies_a_tenant_absent_from_the_policy()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy.Empty, "alice", Acme);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("Tenant 'acme' is not registered"));
    }

    [Test]
    public void ValidateActiveTenant_denies_a_suspended_tenant()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(
            Policy(Record("acme", TenantStatus.Suspended, admins: ["alice"])),
            "alice",
            Acme);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("is not active"));
    }

    [Test]
    public void ValidateActiveTenant_denies_a_subject_the_tenant_does_not_list()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(Policy(Record("acme", admins: ["alice"])), "bob", Acme);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("Subject 'bob' is not an admin of tenant 'acme'"));
    }

    [Test]
    public void ValidateActiveTenant_does_not_accept_membership_of_another_tenant_in_the_same_policy()
    {
        var decision = LatticeTenantPolicyEngine.ValidateActiveTenant(
            Policy(Record("acme", admins: ["alice"]), Record("beta", admins: ["bob"])),
            "bob",
            Acme);

        Assert.That(decision.Allowed, Is.False, "being an admin of beta never validates acting as acme");
    }

    [Test]
    public async Task ValidateActiveTenant_snapshot_and_explicit_policy_paths_agree_on_every_case()
    {
        var records = new[]
        {
            Record("acme", admins: ["alice"]),
            Record("beta", TenantStatus.Suspended, admins: ["bob"]),
        };
        var registry = new FakeTenantRegistry();
        registry.Records.AddRange(records);
        var maintainer = TenantPolicyEpochTestCluster.Unleased(registry);
        await maintainer.EnsureWarmAsync();
        var engine = new LatticeTenantPolicyEngine(maintainer);
        var explicitPolicy = Policy(records);

        var cases = new (string Subject, TenantId Tenant)[]
        {
            ("alice", Acme), ("bob", Acme), ("bob", Beta), ("alice", Beta), ("alice", TenantId.Parse("gamma")), ("alice", default),
        };

        Assert.Multiple(() =>
        {
            foreach (var (subject, tenant) in cases)
            {
                var viaSnapshot = engine.ValidateActiveTenant(subject, tenant);
                var viaPolicy = LatticeTenantPolicyEngine.ValidateActiveTenant(explicitPolicy, subject, tenant);
                Assert.That(viaPolicy.Allowed, Is.EqualTo(viaSnapshot.Allowed), $"{subject} as {tenant.Value ?? "<none>"}: verdict");
                Assert.That(viaPolicy.Reason, Is.EqualTo(viaSnapshot.Reason), $"{subject} as {tenant.Value ?? "<none>"}: reason");
            }
        });
        Assert.That(engine.ValidateActiveTenant("alice", Acme).Allowed, Is.True, "the parity set includes an allow");
    }
}
