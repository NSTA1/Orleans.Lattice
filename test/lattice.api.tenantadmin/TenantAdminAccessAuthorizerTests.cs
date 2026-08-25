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
    public async Task AuthorizeTenantAdminAsync_authorizes_the_cluster_wide_admin_capability()
    {
        var gate = new RecordingGate();
        var authorizer = new TenantAdminAccessAuthorizer(gate);

        await authorizer.AuthorizeTenantAdminAsync();

        Assert.Multiple(() =>
        {
            Assert.That(gate.Calls, Is.EqualTo(1));
            Assert.That(gate.LastOperation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(gate.LastScope, Is.EqualTo(TenantAdminAccessAuthorizer.ClusterWideScope));
        });
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
