using Orleans.Lattice;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAdminAccessAuthorizer"/>, the single fail-closed
/// choke point every tenant-admin operation consults. Proves it authorizes the
/// cluster-wide <see cref="LatticeOperation.Admin"/> capability, denies fail-closed
/// on a plain deny and on a partial (key-filtered) allow, honours the system-origin
/// bypass, and that the read-only probe never throws for a denial. Driven with
/// hand-written gates - no cluster.
/// </summary>
[TestFixture]
public sealed class TenantAdminAccessAuthorizerTests
{
    [Test]
    public void Constructor_rejects_a_null_gate()
    {
        Assert.That(() => new TenantAdminAccessAuthorizer(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_allows_when_the_gate_allows()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(allow: true));

        await authorizer.AuthorizeTenantAdminAsync();

        Assert.Pass("An allowing gate authorizes without throwing.");
    }

    [Test]
    public void AuthorizeTenantAdminAsync_denies_fail_closed_when_the_gate_denies()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(allow: false));

        Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantAdminAsync_rejects_a_partial_key_filtered_allow_fail_closed()
    {
        // A cluster-wide administrative operation can never be narrowed to a subset
        // of keys, so a key-filtered allow is treated as a deny.
        var authorizer = new TenantAdminAccessAuthorizer(new FilteredGate());

        Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_authorizes_the_platform_operator_admin_capability()
    {
        var gate = new RecordingGate();
        var authorizer = new TenantAdminAccessAuthorizer(gate);

        await authorizer.AuthorizeTenantAdminAsync();

        Assert.Multiple(() =>
        {
            Assert.That(gate.Calls, Is.EqualTo(1));
            Assert.That(gate.LastOperation, Is.EqualTo(LatticeOperation.Admin));
            // Tenant administration is a control-plane action: it authorizes over the
            // reserved policy tree (control-plane isolated), NOT the data-plane "*"
            // sentinel, which would fail open under DefaultEffect=Allow.
            Assert.That(gate.LastScope, Is.EqualTo(TenantAdminAccessAuthorizer.PlatformOperatorScope));
            Assert.That(gate.LastScope, Is.EqualTo(Orleans.Lattice.Auth.LatticeAuthReservedTrees.PolicyTreeId));
        });
    }

    // ----- Control-plane isolation regression (issue #1616 tenancy security review):
    // tenant administration must fail closed under DefaultEffect=Allow, not inherit
    // the data-plane default effect the way a cluster-wide "*" data scope does. -----

    [Test]
    public void AuthorizeTenantAdminAsync_denies_an_anonymous_caller_under_default_allow()
    {
        // Regression proof. The gate faithfully models the real PolicyAccessGate
        // under LatticeAuthOptions.DefaultEffect=Allow with no rules authored.
        // Authorizing over a data-plane "*" scope (the prior behaviour) inherits
        // Allow there and hands full tenant administration - create/suspend/resume/
        // delete of ANY tenant - to any caller, including an anonymous one. Routing
        // through the control-plane-isolated policy tree denies the unmatched
        // request regardless of the default effect.
        var gate = new DefaultEffectAllowGate();
        // No membership context -> the caller resolves to anonymous.
        var authorizer = new TenantAdminAccessAuthorizer(gate);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(gate.LastScope, Is.EqualTo(Orleans.Lattice.Auth.LatticeAuthReservedTrees.PolicyTreeId),
                "the request must target the control-plane-isolated policy tree, not a data-plane scope");
        });
    }

    [Test]
    public void AuthorizeTenantAdminAsync_denies_a_non_operator_subject_under_default_allow()
    {
        // A genuine (non-anonymous) but non-operator caller is likewise denied under
        // DefaultEffect=Allow: it holds no Admin grant on the reserved policy tree.
        var gate = new DefaultEffectAllowGate(policyTreeAdminSubjectId: "platform-operator");
        var membership = new FixedMembershipContext(new LatticeSubject("regular-user"));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_allows_a_platform_operator_granted_on_the_policy_tree_under_default_allow()
    {
        // Positive control: a real platform operator (Admin on the reserved policy
        // tree) is still authorized under DefaultEffect=Allow, so the fix denies the
        // unauthorized without also denying the legitimately authorized.
        var gate = new DefaultEffectAllowGate(policyTreeAdminSubjectId: "platform-operator");
        var membership = new FixedMembershipContext(new LatticeSubject("platform-operator"));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        await authorizer.AuthorizeTenantAdminAsync();

        Assert.Pass("A platform operator granted on the policy tree is authorized under DefaultEffect=Allow.");
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_bypasses_the_gate_under_system_origin()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(allow: false));

        using (LatticeSystemOrigin.Enter())
        {
            await authorizer.AuthorizeTenantAdminAsync();
        }

        Assert.Pass("System origin bypasses the gate, so even a denying gate authorizes.");
    }

    // ----- Subject-resolving authorization (delegates the allow/deny decision to
    // the real gate over the resolved caller subject) -----

    private const string AdminSubjectId = "cluster-admin";

    [Test]
    public async Task AuthorizeTenantAdminAsync_allows_a_genuine_admin_subject()
    {
        var gate = new AdminSubjectGate(AdminSubjectId);
        var membership = new FixedMembershipContext(new LatticeSubject(AdminSubjectId));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        await authorizer.AuthorizeTenantAdminAsync();

        Assert.That(gate.LastSubjectId, Is.EqualTo(AdminSubjectId),
            "The resolved caller subject is handed to the gate for the decision.");
    }

    [Test]
    public void AuthorizeTenantAdminAsync_denies_a_non_admin_subject_fail_closed()
    {
        var gate = new AdminSubjectGate(AdminSubjectId);
        var membership = new FixedMembershipContext(new LatticeSubject("regular-user"));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantAdminAsync_denies_a_wrong_tenant_subject_fail_closed()
    {
        // A tenant-scoped principal is not the cluster-wide tenant administrator,
        // so the gate denies it and the authorizer refuses fail-closed.
        var gate = new AdminSubjectGate(AdminSubjectId);
        var membership = new FixedMembershipContext(new LatticeSubject("tenant:acme:operator"));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        Assert.That(async () => await authorizer.AuthorizeTenantAdminAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_under_system_origin_never_consults_the_gate_or_resolves_the_subject()
    {
        // Break-glass follows core exactly: a system-origin caller short-circuits
        // to allow before the subject is resolved or the gate is consulted, even
        // when the gate would deny the resolved (non-admin) subject.
        var gate = new AdminSubjectGate(AdminSubjectId);
        var membership = new FixedMembershipContext(new LatticeSubject("regular-user"));
        var authorizer = new TenantAdminAccessAuthorizer(gate, membership);

        using (LatticeSystemOrigin.Enter())
        {
            await authorizer.AuthorizeTenantAdminAsync();
        }

        Assert.That(gate.LastSubjectId, Is.Null, "System origin authorizes without consulting the gate.");
    }

    [Test]
    public async Task IsTenantAdminAuthorizedAsync_reports_true_when_authorized()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(allow: true));

        Assert.That(await authorizer.IsTenantAdminAuthorizedAsync(), Is.True);
    }

    [Test]
    public async Task IsTenantAdminAuthorizedAsync_reports_false_when_denied_without_throwing()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(allow: false));

        Assert.That(await authorizer.IsTenantAdminAuthorizedAsync(), Is.False);
    }

    [Test]
    public async Task IsTenantAdminAuthorizedAsync_reports_false_for_a_partial_allow()
    {
        var authorizer = new TenantAdminAccessAuthorizer(new FilteredGate());

        Assert.That(await authorizer.IsTenantAdminAuthorizedAsync(), Is.False);
    }
}
