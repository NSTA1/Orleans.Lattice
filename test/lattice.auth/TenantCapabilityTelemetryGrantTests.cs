using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Proves, against the <b>real</b> <see cref="PolicyAccessGate"/> and decision
/// engine (driven over the in-process <see cref="AuthGateHarness"/>, so these are
/// deterministic unit tests with no cluster), that a
/// <see cref="LatticeOperation.Telemetry"/> grant written over
/// <see cref="LatticeScope.TenantCapability(TenantId)"/> admits its subject to that
/// one tenant's telemetry capability and to nothing else (issue #5582): not another
/// tenant's, not the cluster-wide capability, and not the delegated per-tenant
/// <see cref="LatticeOperation.Admin"/> capability that shares the id - and the
/// reverse. Every denial is re-proved under <see cref="LatticeEffect.Allow"/> by
/// default, because the scope is a control-plane namespace.
/// </summary>
[TestFixture]
public sealed class TenantCapabilityTelemetryGrantTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Globex = TenantId.Parse("globex");
    private static readonly LatticeSubject Ada = new("ada");
    private static readonly LatticeSubject Gus = new("gus");

    private static LatticeAuthorizationRule Grant(string ruleId, string subjectId, LatticeScope scope, LatticeOperation operations) =>
        new(ruleId, LatticeSubjectSelector.User(subjectId), scope, operations, LatticeEffect.Allow);

    private static LatticeAuthorizationRule AcmeTelemetry() =>
        Grant("acme-telemetry", "ada", LatticeScope.TenantCapability(Acme), LatticeOperation.Telemetry);

    private static LatticeAuthorizationRule GlobexTelemetry() =>
        Grant("globex-telemetry", "gus", LatticeScope.TenantCapability(Globex), LatticeOperation.Telemetry);

    private static async Task<bool> AllowsAsync(AuthGateHarness.Harness harness, string treeId, LatticeOperation operation, LatticeSubject subject)
    {
        var request = new LatticeAccessRequest(treeId, operation, subject);
        var decision = await harness.Gate.AuthorizeAsync(in request);
        return decision.Allowed && decision.KeyFilter is null;
    }

    private static string CapabilityOf(TenantId tenant) => LatticeScope.TenantCapability(tenant).TreeId;

    [TestCase(LatticeEffect.Deny)]
    [TestCase(LatticeEffect.Allow)]
    public async Task A_tenant_telemetry_grant_admits_its_subject_to_its_own_tenant_only(LatticeEffect defaultEffect)
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = defaultEffect }, AcmeTelemetry(), GlobexTelemetry());

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, Ada), Is.True, "ada on acme");
            Assert.That(await AllowsAsync(harness, CapabilityOf(Globex), LatticeOperation.Telemetry, Gus), Is.True, "gus on globex");
            Assert.That(await AllowsAsync(harness, CapabilityOf(Globex), LatticeOperation.Telemetry, Ada), Is.False, "ada must never reach globex");
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, Gus), Is.False, "gus must never reach acme");
        });
    }

    [TestCase(LatticeEffect.Deny)]
    [TestCase(LatticeEffect.Allow)]
    public async Task A_tenant_telemetry_grant_never_confers_cluster_wide_telemetry(LatticeEffect defaultEffect)
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = defaultEffect }, AcmeTelemetry());

        Assert.That(
            await AllowsAsync(harness, LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, Ada),
            Is.False,
            "a per-tenant grant can never match the cluster-wide sentinel");
    }

    [TestCase(LatticeEffect.Deny)]
    [TestCase(LatticeEffect.Allow)]
    public async Task An_unmatched_caller_is_denied_tenant_telemetry_whatever_the_default_effect(LatticeEffect defaultEffect)
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = defaultEffect }, AcmeTelemetry());

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, new LatticeSubject("mallory")), Is.False);
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, LatticeSubject.Anonymous), Is.False);
        });
    }

    [Test]
    public async Task A_tenant_telemetry_grant_confers_nothing_of_the_admin_bit_on_the_same_scope()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, AcmeTelemetry());

        Assert.That(
            await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Admin, Ada),
            Is.False,
            "Telemetry on acme's capability scope must not make ada acme's delegated administrator");
    }

    [Test]
    public async Task The_admin_bit_on_a_tenant_capability_scope_confers_nothing_of_telemetry()
    {
        var delegatedAdmin = Grant(
            "acme-admin", "ada", LatticeScope.Tree(LatticeTenantAdminScope.ForTenant(Acme).TreeScope), LatticeOperation.Admin);
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, delegatedAdmin);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Admin, Ada), Is.True,
                "positive control: the delegated admin grant itself is honoured");
            Assert.That(await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, Ada), Is.False,
                "Admin on acme's capability scope must not admit ada to acme's telemetry");
        });
    }

    [Test]
    public async Task A_cluster_wide_wildcard_telemetry_grant_does_not_reach_a_tenant_capability_scope()
    {
        // The all-trees tier never folds into a control-plane capability namespace, so a
        // Tree:* rule cannot be laundered into a tenant capability. Cluster-wide telemetry
        // is decided on the sentinel itself, never through the tenant scope.
        var wildcard = Grant("wildcard", "wanda", LatticeScope.ClusterWide(), LatticeOperation.Telemetry | LatticeOperation.Read);
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AllTreesGrantsEnabled = true }, wildcard);

        Assert.That(
            await AllowsAsync(harness, CapabilityOf(Acme), LatticeOperation.Telemetry, new LatticeSubject("wanda")),
            Is.False);
    }

    [Test]
    public void A_tenant_telemetry_grant_is_authorable_on_the_policy_store()
    {
        Assert.DoesNotThrow(() => AuthConstants.EnsureAuthorableRuleScope(
            AcmeTelemetry(), delegationEnabled: false, allTreesGrantsEnabled: false));
    }
}
