using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Drift guard for the tenant-registry reserved prefix. The authorization package
/// applies control-plane read isolation to the <c>sys-tenant-*</c> namespace
/// (issue #1671) by keying off the core constant
/// <see cref="LatticeConstants.TenantRegistryTreePrefix"/>, which the tenancy
/// runtime cannot see (it is core-internal and tenancy is not on core's
/// InternalsVisibleTo list), so tenancy re-declares the value as its own internal
/// <see cref="TenantTreeNames.TreePrefix"/>. This test - which can see both the
/// core constant and the tenancy constant - fails the moment the two diverge, so a
/// rename can never silently stop the registry from being isolated (which would
/// re-open the cross-tenant metadata leak).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TenantTreeNamesDriftGuardTests
{
    [Test]
    public void Tenant_tree_prefix_matches_the_canonical_core_constant()
    {
        Assert.That(TenantTreeNames.TreePrefix, Is.EqualTo(LatticeConstants.TenantRegistryTreePrefix));
    }

    [TestCase(TenantTreeNames.RegistryTree)]
    [TestCase(TenantTreeNames.RegistryHistoryView)]
    [TestCase(TenantTreeNames.UsageTree)]
    [TestCase(TenantTreeNames.OverageTree)]
    public void Every_backing_tree_name_stays_within_the_isolated_prefix(string treeId)
    {
        Assert.That(treeId, Does.StartWith(LatticeConstants.TenantRegistryTreePrefix));
    }
}
