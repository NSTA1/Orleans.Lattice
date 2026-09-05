namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdminAuthorizer"/>: the single core seam
/// that resolves the platform-operator versus delegated-per-tenant-admin distinction
/// against the registered <see cref="ILatticeAccessGate"/>. The gate is a
/// hand-written fake (NSubstitute cannot mock the <c>in</c> parameter) that decides
/// by exact tree id, mirroring the real policy engine's exact-id matching, so the
/// security invariants are proven deterministically in-process: a delegated admin is
/// confined to its own tenant (denied cross-tenant, denied cluster-wide escalation),
/// a platform operator is allowed cluster-wide, and a key-filtered allow is
/// fail-closed to a deny.
/// </summary>
[TestFixture]
public class LatticeTenantAdminAuthorizerTests
{
    /// <summary>A gate that decides from the request via a supplied delegate and counts calls.</summary>
    private sealed class FakeGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide) : ILatticeAccessGate
    {
        public int CallCount { get; private set; }

        public LatticeAccessRequest LastRequest { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var copy = request;
            CallCount++;
            LastRequest = copy;
            return new ValueTask<LatticeAccessDecision>(decide(copy));
        }
    }

    /// <summary>A gate that allows an <see cref="LatticeOperation.Admin"/> request on exactly one tree id.</summary>
    private static FakeGate AllowingExactly(string treeId) =>
        new(r => string.Equals(r.TreeId, treeId, StringComparison.Ordinal) && r.Operation == LatticeOperation.Admin
            ? LatticeAccessDecision.Allow()
            : LatticeAccessDecision.Deny($"no grant on '{r.TreeId}'"));

    private static LatticeTenantAdminAuthorizer Authorizer(ILatticeAccessGate gate) => new(gate);

    private static TenantId Acme => TenantId.Parse("acme");

    private static TenantId Beta => TenantId.Parse("beta");

    private static LatticeSubject Admin => new("admin");

    // ---- construction guard ---------------------------------------------

    [Test]
    public void Constructor_null_gate_throws()
    {
        Assert.That(() => new LatticeTenantAdminAuthorizer(null!), Throws.ArgumentNullException);
    }

    // ---- faithful reflection of the gate decision -----------------------

    [Test]
    public async Task IsAuthorizedAsync_allow_returns_true()
    {
        var authorizer = Authorizer(new FakeGate(_ => LatticeAccessDecision.Allow()));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, Admin);

        Assert.That(allowed, Is.True);
    }

    [Test]
    public async Task IsAuthorizedAsync_deny_returns_false()
    {
        var authorizer = Authorizer(new FakeGate(_ => LatticeAccessDecision.Deny("nope")));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, Admin);

        Assert.That(allowed, Is.False);
    }

    [Test]
    public async Task IsAuthorizedAsync_builds_an_admin_request_for_the_scope_id()
    {
        var gate = new FakeGate(_ => LatticeAccessDecision.Allow());
        var authorizer = Authorizer(gate);

        await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), Admin);

        Assert.Multiple(() =>
        {
            Assert.That(gate.LastRequest.TreeId, Is.EqualTo("_lattice_tenant_admin_acme"));
            Assert.That(gate.LastRequest.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(gate.LastRequest.Subject, Is.EqualTo(Admin));
        });
    }

    // ---- fail-closed on a filtered allow --------------------------------

    [Test]
    public async Task IsAuthorizedAsync_filtered_allow_is_denied()
    {
        // A whole-scope admin capability can never be narrowed to a key subset.
        var authorizer = Authorizer(new FakeGate(_ => LatticeAccessDecision.Filtered(_ => true)));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, Admin);

        Assert.That(allowed, Is.False);
    }

    // ---- delegated per-tenant admin: confined to its own tenant ---------

    [Test]
    public async Task IsAuthorizedAsync_delegated_admin_allowed_for_its_own_tenant()
    {
        var authorizer = Authorizer(AllowingExactly("_lattice_tenant_admin_acme"));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Acme), Admin);

        Assert.That(allowed, Is.True);
    }

    [Test]
    public async Task IsAuthorizedAsync_delegated_admin_denied_across_tenants()
    {
        // A grant on acme's reserved id can never match beta's reserved id.
        var authorizer = Authorizer(AllowingExactly("_lattice_tenant_admin_acme"));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.ForTenant(Beta), Admin);

        Assert.That(allowed, Is.False);
    }

    [Test]
    public async Task IsAuthorizedAsync_delegated_admin_denied_cluster_wide_escalation()
    {
        // A per-tenant grant can never match the cluster-wide sentinel scope.
        var authorizer = Authorizer(AllowingExactly("_lattice_tenant_admin_acme"));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, Admin);

        Assert.That(allowed, Is.False);
    }

    // ---- platform operator: cluster-wide --------------------------------

    [Test]
    public async Task IsAuthorizedAsync_platform_operator_allowed_cluster_wide()
    {
        var authorizer = Authorizer(AllowingExactly("sys-auth-policy"));

        var allowed = await authorizer.IsAuthorizedAsync(LatticeTenantAdminScope.Platform, Admin);

        Assert.That(allowed, Is.True);
    }

    // ---- AuthorizeAsync throw path --------------------------------------

    [Test]
    public async Task AuthorizeAsync_allow_does_not_throw()
    {
        var gate = new FakeGate(_ => LatticeAccessDecision.Allow());
        var authorizer = Authorizer(gate);

        await authorizer.AuthorizeAsync(LatticeTenantAdminScope.ForTenant(Acme), Admin);

        // Not throwing is only half the claim: an authorizer that stopped
        // consulting the gate would also not throw, and would silently fail
        // open. Assert that the allow was actually asked for, on the scope's
        // own tree, so the pass is evidence the gate ran.
        Assert.Multiple(() =>
        {
            Assert.That(gate.CallCount, Is.EqualTo(1),
                "the allow path must consult the gate exactly once");
            Assert.That(gate.LastRequest.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(gate.LastRequest.TreeId, Is.EqualTo(LatticeTenantAdminScope.ForTenant(Acme).TreeScope));
        });
    }

    [Test]
    public void AuthorizeAsync_deny_throws_with_scope_context()
    {
        var authorizer = Authorizer(AllowingExactly("_lattice_tenant_admin_acme"));

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeAsync(LatticeTenantAdminScope.ForTenant(Beta), new LatticeSubject("beta-admin")));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo("_lattice_tenant_admin_beta"));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(ex.SubjectId, Is.EqualTo("beta-admin"));
        });
    }

    [Test]
    public void AuthorizeAsync_filtered_allow_throws()
    {
        var authorizer = Authorizer(new FakeGate(_ => LatticeAccessDecision.Filtered(_ => true)));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeAsync(LatticeTenantAdminScope.Platform, Admin));
    }

    [Test]
    public void AuthorizeAsync_anonymous_denied_by_a_default_deny_gate_throws()
    {
        // Model a default-deny gate: an anonymous (unauthorized) caller is denied.
        var authorizer = Authorizer(new FakeGate(r =>
            r.Subject.IsAnonymous ? LatticeAccessDecision.Deny("anonymous") : LatticeAccessDecision.Allow()));

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeAsync(LatticeTenantAdminScope.Platform, LatticeSubject.Anonymous));
    }
}
