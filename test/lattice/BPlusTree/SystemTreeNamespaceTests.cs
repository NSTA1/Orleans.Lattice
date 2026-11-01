using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins <see cref="SystemTreeNamespace.IsSystemOwned"/> to exactly the
/// namespaces that refuse user-origin content (issue #5569). The reserved
/// system replay lane trusts this predicate, so a name it wrongly accepts is a
/// name any caller could use to jump the lane.
/// </summary>
[TestFixture]
public class SystemTreeNamespaceTests
{
    [TestCase("_lattice_trees")]
    [TestCase("_lattice_replog_dlq_orders")]
    [TestCase("sys-membership-groups")]
    [TestCase("sys-auth-policy")]
    [TestCase("sys-tenant-registry")]
    public void IsSystemOwned_reserved_namespace_is_accepted(string treeId)
        => Assert.That(SystemTreeNamespace.IsSystemOwned(treeId), Is.True);

    [TestCase("orders")]
    [TestCase("view-sys-membership-groups-history")]
    [TestCase("view-sys-anything")]
    [TestCase("tag-sys-status")]
    [TestCase("t/acme/sys-membership-groups")]
    [TestCase("a/app/sys-data")]
    [TestCase("Sys-membership-groups")]
    [TestCase("lattice_trees")]
    [TestCase("system")]
    public void IsSystemOwned_user_nameable_tree_is_rejected(string treeId)
        => Assert.That(SystemTreeNamespace.IsSystemOwned(treeId), Is.False);

    [TestCase(null)]
    [TestCase("")]
    public void IsSystemOwned_missing_tree_id_is_rejected(string? treeId)
        => Assert.That(SystemTreeNamespace.IsSystemOwned(treeId), Is.False);

    [Test]
    public void IsSystemOwned_matches_the_prefixes_the_creation_guards_reserve()
    {
        Assert.That(SystemTreeNamespace.IsSystemOwned(LatticeConstants.SystemTreePrefix + "x"), Is.True);
        Assert.That(SystemTreeNamespace.IsSystemOwned(LatticeConstants.SystemDataTreePrefix + "x"), Is.True);
        Assert.That(SystemTreeNamespace.IsSystemOwned(LatticeConstants.ViewTreePrefix + LatticeConstants.SystemDataTreePrefix + "x"),
            Is.False, "a view's name is caller-chosen, so view-sys-* is not system-owned");
    }
}
