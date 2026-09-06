using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// End-to-end unit coverage that the platform-operator versus
/// delegated-per-tenant-admin capability distinction, modelled by
/// <see cref="LatticeTenantAdminScope"/> and enforced by
/// <see cref="LatticeTenantAdminAuthorizer"/>, holds against the <b>real</b>
/// <see cref="PolicyAccessGate"/> and decision engine (driven over the in-process
/// <see cref="AuthGateHarness"/>, so these are deterministic unit tests with no
/// cluster). Both capability namespaces are governed by control-plane isolation, so
/// the invariants hold <em>independently of the data-plane
/// <see cref="LatticeAuthOptions.DefaultEffect"/></em>: the final fixture re-proves
/// every denial under <see cref="LatticeEffect.Allow"/> by default.
/// </summary>
[TestFixture]
public sealed class TenantAdminCapabilityScopeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static LatticeAuthorizationRule AdminGrant(string ruleId, string subjectId, LatticeScope scope) =>
        new(ruleId, LatticeSubjectSelector.User(subjectId), scope, LatticeOperation.Admin, LatticeEffect.Allow);

    private static LatticeAuthorizationRule DelegatedTenantGrant(string subjectId, TenantId tenant) =>
        AdminGrant($"{subjectId}-grant", subjectId, LatticeScope.Tree(LatticeTenantAdminScope.ForTenant(tenant).TreeScope));

    // ---- the platform scope id is the reserved policy tree ---------------

    [Test]
    public void PlatformScopeId_equals_the_reserved_auth_policy_tree_id()
    {
        // Drift guard: the core-owned platform scope id must equal the auth package's
        // reserved policy tree id, so the platform capability is routed through the
        // gate's existing control-plane isolation for that reserved id.
        Assert.That(LatticeTenantAdminScope.PlatformScopeId, Is.EqualTo(LatticeAuthReservedTrees.PolicyTreeId));
    }

    // ---- delegated per-tenant admin: scoped, cannot cross or escalate ----

    [Test]
    public async Task Delegated_admin_is_authorized_for_its_own_tenant()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), new LatticeSubject("acme-admin"));

        Assert.That(allowed, Is.True);
    }

    [Test]
    public async Task Delegated_admin_is_denied_across_tenants()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), new LatticeSubject("acme-admin"));

        Assert.That(allowed, Is.False, "a grant on acme's reserved id can never match beta's");
    }

    [Test]
    public async Task Delegated_admin_is_denied_cluster_wide_escalation()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, new LatticeSubject("acme-admin"));

        Assert.That(allowed, Is.False, "a per-tenant grant can never match the platform policy-tree scope");
    }

    [Test]
    public async Task Anonymous_is_denied_the_delegated_capability()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), LatticeSubject.Anonymous);

        Assert.That(allowed, Is.False);
    }

    // ---- the all-trees wildcard tier never confers the capability --------

    [Test]
    public async Task All_trees_wildcard_grant_does_not_confer_delegated_tenant_administration()
    {
        // A cluster-wide "Tree:*" Admin grant is a data-plane wildcard. It must never
        // be laundered into the tenant-administration control plane, or one wildcard
        // rule would silently confer delegated administration over every tenant in
        // the cluster - including tenants the policy has never named.
        var wildcard = AdminGrant("wildcard", "wildcard-admin", LatticeScope.ClusterWide());
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AllTreesGrantsEnabled = true }, wildcard);
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);
        var subject = new LatticeSubject("wildcard-admin");

        Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), subject), Is.False);
        Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), subject), Is.False);
        Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, subject), Is.False,
            "the platform capability is the reserved policy tree, already excluded from the tier");
    }

    [Test]
    public async Task All_trees_wildcard_grant_does_not_expose_the_capability_to_the_existence_probe()
    {
        // The gate's existence-hiding probe has to agree with its enforcement branch:
        // if HasAnyGrantAsync reported a grant the gate would refuse to honour, a
        // wildcard holder could enumerate the tenant-administration control plane.
        var wildcard = AdminGrant("wildcard", "wildcard-admin", LatticeScope.ClusterWide());
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AllTreesGrantsEnabled = true }, wildcard);
        var scope = LatticeTenantAdminScope.ForTenant(Acme).TreeScope;

        var probed = await harness.Gate.HasAnyGrantAsync(scope, new LatticeSubject("wildcard-admin"), LatticeOperation.Admin);

        Assert.That(probed, Is.False);
    }

    [Test]
    public async Task A_delegated_grant_still_works_with_the_all_trees_tier_enabled()
    {
        // Positive control for the two denials above: excluding the capability
        // namespace from the wildcard tier must not disturb the legitimate,
        // explicitly delegated per-tenant grant.
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AllTreesGrantsEnabled = true },
            DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), new LatticeSubject("acme-admin")),
            Is.True);
        Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), new LatticeSubject("acme-admin")),
            Is.False);
    }

    // ---- platform operator via the access-administration delegation grant --

    [Test]
    public async Task Delegated_access_administrator_is_authorized_for_the_platform_capability()
    {
        // The platform-operator capability is Admin on the reserved policy tree - the
        // same access-administration delegation grant the auth surface already uses,
        // reconciled with T12/T17. It is NOT an all-trees "*" data grant.
        var grant = AdminGrant("platform-grant", "root", LatticeScope.Tree(LatticeTenantAdminScope.PlatformScopeId));
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AccessAdministrationDelegationEnabled = true }, grant);
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, new LatticeSubject("root"));

        Assert.That(allowed, Is.True);
    }

    [Test]
    public async Task Anonymous_is_denied_the_platform_capability()
    {
        var grant = AdminGrant("platform-grant", "root", LatticeScope.Tree(LatticeTenantAdminScope.PlatformScopeId));
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny, AccessAdministrationDelegationEnabled = true }, grant);
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, LatticeSubject.Anonymous);

        Assert.That(allowed, Is.False);
    }

    // ---- platform operator via the bootstrap root-of-trust ---------------

    [Test]
    public async Task Bootstrap_administrator_is_authorized_cluster_wide_and_for_every_tenant()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);
        var root = new LatticeSubject("root");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, root), Is.True);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), root), Is.True);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), root), Is.True);
        });
    }

    [Test]
    public async Task A_non_bootstrap_non_granted_subject_is_denied_every_capability()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);
        var mallory = new LatticeSubject("mallory");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, mallory), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), mallory), Is.False);
        });
    }

    // ---- the isolation holds under DefaultEffect = Allow -----------------
    // These are the regression proofs for the review finding: control-plane isolation
    // forces an unmatched capability request closed regardless of the data-plane
    // default effect, so an allow-by-default deployment does not silently grant
    // tenant-admin or platform-operator capabilities.

    [Test]
    public async Task Under_default_allow_anonymous_is_denied_platform_and_tenant()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, LatticeSubject.Anonymous), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), LatticeSubject.Anonymous), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), LatticeSubject.Anonymous), Is.False);
        });
    }

    [Test]
    public async Task Under_default_allow_a_non_granted_subject_is_denied_platform_and_tenant()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);
        var mallory = new LatticeSubject("mallory");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, mallory), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), mallory), Is.False);
        });
    }

    [Test]
    public async Task Under_default_allow_a_delegated_admin_cannot_cross_tenants_or_escalate()
    {
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);
        var acmeAdmin = new LatticeSubject("acme-admin");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), acmeAdmin), Is.False, "cross-tenant");
            Assert.That(await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, acmeAdmin), Is.False, "escalation");
        });
    }

    [Test]
    public async Task Under_default_allow_a_delegated_admin_is_still_authorized_for_its_own_tenant()
    {
        // Positive control: an explicit matched allow is still honoured under
        // control-plane isolation, so the fix denies only the unmatched requests.
        var harness = await AuthGateHarness.CreateAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, DelegatedTenantGrant("acme-admin", Acme));
        var authorizer = new LatticeTenantAdminAuthorizer(harness.Gate);

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), new LatticeSubject("acme-admin"));

        Assert.That(allowed, Is.True);
    }
}
