using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthConstants.IsControlPlaneRegistryTree"/> - the single
/// predicate that routes both control-plane registry system-data namespaces, the tenant
/// registry (<c>sys-tenant-*</c>) and the app registry (<c>sys-app-*</c>), through
/// control-plane read isolation at the enforcement gate and out of the evaluator's
/// all-trees tier.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AuthConstantsControlPlaneRegistryTests
{
    [TestCase("sys-tenant-registry")]
    [TestCase("sys-tenant-usage")]
    [TestCase("sys-tenant-overage")]
    [TestCase("sys-app-registry")]
    [TestCase("sys-app-registry-history")]
    [TestCase("sys-app-trees")]
    [TestCase("sys-app-activation")]
    [TestCase("sys-app-")]
    public void IsControlPlaneRegistryTree_true_for_both_registry_namespaces(string treeId)
    {
        Assert.That(AuthConstants.IsControlPlaneRegistryTree(treeId), Is.True);
    }

    [TestCase("app")]
    [TestCase("a/notes/pages")]
    [TestCase("sys-app")]
    [TestCase("sys-apps")]
    [TestCase("sys-application")]
    [TestCase("sys-auth-policy")]
    [TestCase("sys-membership-users")]
    [TestCase("SYS-APP-REGISTRY")]
    [TestCase("t/acme/sys-app-registry")]
    [TestCase("*")]
    public void IsControlPlaneRegistryTree_false_for_everything_else(string treeId)
    {
        Assert.That(AuthConstants.IsControlPlaneRegistryTree(treeId), Is.False);
    }

    [Test]
    public void IsControlPlaneRegistryTree_null_throws()
    {
        Assert.That(() => AuthConstants.IsControlPlaneRegistryTree(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_app_registry_is_not_classified_as_the_tenant_registry()
    {
        Assert.That(AuthConstants.IsTenantRegistryTree("sys-app-registry"), Is.False,
            "the two prefixes are disjoint; only the combined predicate covers both");
    }
}
