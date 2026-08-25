namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="TreeOwnership"/>: the platform-owned singleton, the
/// tenant-owned factory, the initialised-tenant guard, and the derived
/// <see cref="TreeOwnership.IsPlatformOwned"/> predicate.
/// </summary>
[TestFixture]
public sealed class TreeOwnershipTests
{
    [Test]
    public void Platform_is_not_tenant_owned()
    {
        Assert.That(TreeOwnership.Platform.IsTenantOwned, Is.False);
    }

    [Test]
    public void Platform_is_platform_owned()
    {
        Assert.That(TreeOwnership.Platform.IsPlatformOwned, Is.True);
    }

    [Test]
    public void Platform_has_the_no_tenant_value()
    {
        Assert.That(TreeOwnership.Platform.Tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void ForTenant_is_tenant_owned()
    {
        var ownership = TreeOwnership.ForTenant(TenantId.Parse("contoso"));

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.IsPlatformOwned, Is.False);
    }

    [Test]
    public void ForTenant_carries_the_owning_tenant()
    {
        var tenant = TenantId.Parse("contoso");

        var ownership = TreeOwnership.ForTenant(tenant);

        Assert.That(ownership.Tenant, Is.EqualTo(tenant));
    }

    [Test]
    public void ForTenant_default_tenant_is_tenant_owned()
    {
        var ownership = TreeOwnership.ForTenant(TenantId.Default);

        Assert.That(ownership.IsTenantOwned, Is.True);
        Assert.That(ownership.Tenant, Is.EqualTo(TenantId.Default));
    }

    [Test]
    public void ForTenant_no_tenant_value_throws_argument()
    {
        Assert.That(() => TreeOwnership.ForTenant(default), Throws.ArgumentException);
    }

    [Test]
    public void ForTenant_equality_is_by_value()
    {
        var a = TreeOwnership.ForTenant(TenantId.Parse("contoso"));
        var b = TreeOwnership.ForTenant(TenantId.Parse("contoso"));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Platform_and_a_tenant_owner_are_not_equal()
    {
        Assert.That(TreeOwnership.Platform, Is.Not.EqualTo(TreeOwnership.ForTenant(TenantId.Default)));
    }
}
