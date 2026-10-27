using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthConstants.IsControlPlaneDataTree"/> - the predicate
/// that routes every control-plane system-data tree (the tenant and app registries and
/// the membership store) through control-plane read isolation at the enforcement gate
/// and out of the evaluator's all-trees tier.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AuthConstantsControlPlaneDataTreeTests
{
    [TestCase("sys-membership-groups")]
    [TestCase("sys-membership-edges")]
    [TestCase("sys-membership-groups-history")]
    [TestCase("sys-membership-edges-history")]
    [TestCase("sys-tenant-registry")]
    [TestCase("sys-app-registry")]
    public void IsControlPlaneDataTree_true_for_registries_and_the_membership_store(string treeId)
    {
        Assert.That(AuthConstants.IsControlPlaneDataTree(treeId), Is.True);
    }

    [TestCase("sys-membership")]
    [TestCase("SYS-MEMBERSHIP-GROUPS")]
    [TestCase("t/acme/sys-membership-groups")]
    [TestCase("membership")]
    [TestCase("sys-auth-policy")]
    [TestCase("orders")]
    public void IsControlPlaneDataTree_false_for_everything_else(string treeId)
    {
        Assert.That(AuthConstants.IsControlPlaneDataTree(treeId), Is.False);
    }

    [Test]
    public void IsControlPlaneDataTree_null_throws()
    {
        Assert.That(() => AuthConstants.IsControlPlaneDataTree(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Every_membership_tree_the_cluster_replicates_is_covered()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AuthConstants.IsControlPlaneDataTree(LatticeSystemTreeNames.MembershipGroups), Is.True);
            Assert.That(AuthConstants.IsControlPlaneDataTree(LatticeSystemTreeNames.MembershipEdges), Is.True);
        });
    }
}
