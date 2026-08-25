namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdminScope"/>: the in-process vocabulary
/// that names the platform-operator versus delegated-per-tenant-admin capability as
/// a scope over the existing <see cref="LatticeOperation.Admin"/> operation. The
/// tests pin the exact tree-scope ids each form resolves to (the ids the policy
/// engine matches exactly), the request the scope builds, the guard on an
/// uninitialised tenant, and the value equality that makes distinct tenants distinct
/// scopes.
/// </summary>
[TestFixture]
public class LatticeTenantAdminScopeTests
{
    private static TenantId Acme => TenantId.Parse("acme");

    private static TenantId Beta => TenantId.Parse("beta");

    [Test]
    public void PlatformScopeId_is_the_auth_policy_tree()
    {
        Assert.That(LatticeTenantAdminScope.PlatformScopeId, Is.EqualTo("sys-auth-policy"));
    }

    [Test]
    public void TenantScopePrefix_is_the_reserved_platform_owned_prefix()
    {
        Assert.That(LatticeTenantAdminScope.TenantScopePrefix, Is.EqualTo("_lattice_tenant_admin_"));
    }

    [Test]
    public void Platform_is_cluster_wide_with_the_policy_tree_scope()
    {
        var scope = LatticeTenantAdminScope.Platform;

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsPlatformWide, Is.True);
            Assert.That(scope.TreeScope, Is.EqualTo("sys-auth-policy"));
            Assert.That(scope.Tenant.Value, Is.Null, "the platform scope carries no tenant");
        });
    }

    [Test]
    public void ForTenant_is_tenant_scoped_with_the_reserved_id()
    {
        var scope = LatticeTenantAdminScope.ForTenant(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsPlatformWide, Is.False);
            Assert.That(scope.Tenant, Is.EqualTo(Acme));
            Assert.That(scope.TreeScope, Is.EqualTo("_lattice_tenant_admin_acme"));
        });
    }

    [Test]
    public void ForTenant_uninitialised_tenant_throws()
    {
        Assert.That(
            () => LatticeTenantAdminScope.ForTenant(default),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void ForTenant_the_default_tenant_is_a_valid_distinct_scope()
    {
        // TenantId.Default (Value "default") is an initialised, valid tenant, so it
        // resolves to its own reserved id and is not rejected like default(TenantId).
        var scope = LatticeTenantAdminScope.ForTenant(TenantId.Default);

        Assert.That(scope.TreeScope, Is.EqualTo("_lattice_tenant_admin_default"));
    }

    [Test]
    public void ForTenant_distinct_tenants_resolve_to_distinct_scope_ids()
    {
        // The structural basis of cross-tenant isolation: the policy engine matches a
        // rule to a request by exact tree id, so distinct ids can never cross.
        Assert.That(
            LatticeTenantAdminScope.ForTenant(Acme).TreeScope,
            Is.Not.EqualTo(LatticeTenantAdminScope.ForTenant(Beta).TreeScope));
    }

    [Test]
    public void ToAdminRequest_platform_targets_the_policy_tree_with_admin()
    {
        var subject = new LatticeSubject("root");

        var request = LatticeTenantAdminScope.Platform.ToAdminRequest(subject);

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeId, Is.EqualTo("sys-auth-policy"));
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(request.Subject, Is.EqualTo(subject));
            Assert.That(request.Key, Is.Null, "a capability check is a whole-scope request");
        });
    }

    [Test]
    public void ToAdminRequest_tenant_targets_the_reserved_id_with_admin()
    {
        var subject = new LatticeSubject("acme-admin");

        var request = LatticeTenantAdminScope.ForTenant(Acme).ToAdminRequest(subject);

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeId, Is.EqualTo("_lattice_tenant_admin_acme"));
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(request.Subject, Is.EqualTo(subject));
        });
    }

    [Test]
    public void Equality_same_form_is_equal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminScope.Platform, Is.EqualTo(LatticeTenantAdminScope.Platform));
            Assert.That(
                LatticeTenantAdminScope.ForTenant(Acme),
                Is.EqualTo(LatticeTenantAdminScope.ForTenant(Acme)));
        });
    }

    [Test]
    public void Equality_different_form_or_tenant_is_not_equal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTenantAdminScope.ForTenant(Acme),
                Is.Not.EqualTo(LatticeTenantAdminScope.ForTenant(Beta)));
            Assert.That(
                LatticeTenantAdminScope.Platform,
                Is.Not.EqualTo(LatticeTenantAdminScope.ForTenant(Acme)));
        });
    }
}
