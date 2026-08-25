using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the tenant-isolation composition wired into
/// <see cref="PolicyAccessGate"/> (issue #1624). The gate consults the
/// <see cref="ITenantGateEnforcer"/> seam only for a request the policy engine
/// already allowed, and a deny from either side denies. A cluster without the
/// tenancy add-on runs the allow-everything null seam, so its behaviour is
/// unchanged. These tests drive the gate directly over the in-process harness
/// with hand-rolled enforcers, so every decision is exact.
/// </summary>
[TestFixture]
public sealed class PolicyAccessGateTenantEnforcementTests
{
    /// <summary>An enforcer that records whether it was consulted and returns a fixed decision.</summary>
    private sealed class RecordingTenantGateEnforcer(bool isActive, LatticeAccessDecision decision) : ITenantGateEnforcer
    {
        public bool EnforceCalled { get; private set; }

        public bool IsActive => isActive;

        public LatticeAccessDecision Enforce(in LatticeAccessRequest request)
        {
            EnforceCalled = true;
            return decision;
        }
    }

    private static LatticeAccessRequest OrdinaryRead(string subjectId = "alice") =>
        new("app", LatticeOperation.Read, new LatticeSubject(subjectId), "k");

    // ---- tenancy off (null seam) ----------------------------------------

    [Test]
    public async Task AuthorizeAsync_with_the_null_enforcer_leaves_an_allow_unchanged()
    {
        // Default harness overload installs NullTenantGateEnforcer (tenancy off).
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var decision = await harness.Gate.AuthorizeAsync(OrdinaryRead());

        Assert.That(decision.Allowed, Is.True, "the no-tenancy path is unchanged");
    }

    // ---- active enforcer composes with the policy allow -----------------

    [Test]
    public async Task AuthorizeAsync_active_enforcer_deny_overrides_a_policy_allow()
    {
        var enforcer = new RecordingTenantGateEnforcer(
            isActive: true, LatticeAccessDecision.Deny("active tenant does not own tree 'app'"));
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, enforcer);

        var decision = await harness.Gate.AuthorizeAsync(OrdinaryRead());

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False, "a tenant deny denies an otherwise-allowed request");
            Assert.That(decision.Reason, Does.Contain("does not own"));
            Assert.That(enforcer.EnforceCalled, Is.True);
        });
    }

    [Test]
    public async Task AuthorizeAsync_active_enforcer_allow_leaves_a_policy_allow_intact()
    {
        var enforcer = new RecordingTenantGateEnforcer(isActive: true, LatticeAccessDecision.Allow());
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, enforcer);

        var decision = await harness.Gate.AuthorizeAsync(OrdinaryRead());

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(enforcer.EnforceCalled, Is.True);
        });
    }

    // ---- the enforcer never turns a policy deny into an allow -----------

    [Test]
    public async Task AuthorizeAsync_policy_deny_is_not_consulted_by_an_allowing_enforcer()
    {
        var enforcer = new RecordingTenantGateEnforcer(isActive: true, LatticeAccessDecision.Allow());
        // Default effect deny + no rules -> policy denies.
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, enforcer);

        var decision = await harness.Gate.AuthorizeAsync(OrdinaryRead());

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False, "a policy deny stands");
            Assert.That(enforcer.EnforceCalled, Is.False, "the enforcer is off the deny fast path");
        });
    }

    // ---- platform-operator crossing (bootstrap-admin bypass) ------------

    [Test]
    public async Task AuthorizeAsync_bootstrap_administrator_bypasses_the_tenant_enforcer()
    {
        var enforcer = new RecordingTenantGateEnforcer(
            isActive: true, LatticeAccessDecision.Deny("would deny every tenant tree"));
        var options = new LatticeAuthOptions
        {
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options, enforcer);
        var request = new LatticeAccessRequest("t/acme/orders", LatticeOperation.Write, new LatticeSubject("root"), "k");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True, "a platform operator crosses any tenant boundary");
            Assert.That(enforcer.EnforceCalled, Is.False, "the bootstrap bypass never reaches the tenant enforcer");
        });
    }
}
