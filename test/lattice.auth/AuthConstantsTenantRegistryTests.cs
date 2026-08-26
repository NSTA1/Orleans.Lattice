using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthConstants.IsTenantRegistryTree"/> - the
/// predicate that routes the tenant-registry system-data namespace
/// (<c>sys-tenant-*</c>) through control-plane read isolation (issue #1671). The
/// predicate keys off the canonical core prefix
/// <c>LatticeConstants.TenantRegistryTreePrefix</c>; a tenancy test-project drift
/// guard pins that core constant to the tenancy runtime's own copy.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AuthConstantsTenantRegistryTests
{
    [TestCase("sys-tenant-registry")]
    [TestCase("sys-tenant-usage")]
    [TestCase("sys-tenant-overage")]
    [TestCase("sys-tenant-registry-history")]
    public void IsTenantRegistryTree_true_for_the_tenant_registry_namespace(string treeId)
    {
        Assert.That(AuthConstants.IsTenantRegistryTree(treeId), Is.True);
    }

    [TestCase("app")]
    [TestCase("sys-auth-policy")]
    [TestCase("sys-membership-users")]
    [TestCase("sys-tenan")]
    [TestCase("*")]
    public void IsTenantRegistryTree_false_for_everything_else(string treeId)
    {
        Assert.That(AuthConstants.IsTenantRegistryTree(treeId), Is.False);
    }

    [Test]
    public void IsTenantRegistryTree_null_throws()
    {
        Assert.That(() => AuthConstants.IsTenantRegistryTree(null!), Throws.ArgumentNullException);
    }
}
