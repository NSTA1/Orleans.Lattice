using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the app-related tree-name prefixes on <see cref="LatticeConstants"/>
/// (epic #2235): the reserved <see cref="LatticeConstants.AppRegistryTreePrefix"/>
/// system-data namespace and the user-facing
/// <see cref="LatticeConstants.AppTreePrefix"/>.
/// </summary>
[TestFixture]
public sealed class LatticeConstantsTests
{
    [Test]
    public void AppRegistryTreePrefix_is_the_sys_app_segment()
    {
        Assert.That(LatticeConstants.AppRegistryTreePrefix, Is.EqualTo("sys-app-"));
    }

    [Test]
    public void AppRegistryTreePrefix_is_subsumed_by_the_system_data_prefix()
    {
        // Subsumption is what makes the app registry inherit catalog hiding, the
        // user-origin write guard, and the tenant namespace-escape refusal.
        Assert.That(
            LatticeConstants.AppRegistryTreePrefix,
            Does.StartWith(LatticeConstants.SystemDataTreePrefix));
    }

    [Test]
    public void AppRegistryTreePrefix_and_TenantRegistryTreePrefix_are_disjoint()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeConstants.AppRegistryTreePrefix,
                Does.Not.StartWith(LatticeConstants.TenantRegistryTreePrefix));
            Assert.That(
                LatticeConstants.TenantRegistryTreePrefix,
                Does.Not.StartWith(LatticeConstants.AppRegistryTreePrefix));
        });
    }

    [Test]
    public void AppTreePrefix_is_not_in_the_system_data_namespace()
    {
        // App trees are ordinary tenant-local trees; putting them under 'sys-'
        // would hide them from the catalog and refuse them to confined tenants.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeConstants.AppTreePrefix, Does.Not.StartWith(LatticeConstants.SystemDataTreePrefix));
            Assert.That(LatticeConstants.AppRegistryTreePrefix, Does.Not.StartWith(LatticeConstants.AppTreePrefix));
        });
    }
}
