using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthConstants.IsControlPlaneDataTree"/> - the predicate
/// that routes every control-plane system-data tree (the tenant and app registries and
/// the membership and schema stores) through control-plane read isolation at the enforcement gate
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
    [TestCase("sys-schema-dlq")]
    [TestCase("sys-schema-policy")]
    [TestCase("sys-schema-version")]
    [TestCase("sys-schema-policy-history")]
    public void IsControlPlaneDataTree_true_for_control_plane_backing_stores(string treeId)
    {
        Assert.That(AuthConstants.IsControlPlaneDataTree(treeId), Is.True);
    }

    [TestCase("sys-membership")]
    [TestCase("SYS-MEMBERSHIP-GROUPS")]
    [TestCase("t/acme/sys-membership-groups")]
    [TestCase("membership")]
    [TestCase("sys-auth-policy")]
    [TestCase("orders")]
    [TestCase("sys-schema")]
    [TestCase("SYS-SCHEMA-DLQ")]
    [TestCase("t/acme/sys-schema-dlq")]
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
