using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Covers <see cref="TenantLifecycleMapping"/>, the projection of stored tenant
/// and region states onto the reported lifecycle statuses.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TenantLifecycleMappingTests
{
    [TestCase(TenantStatus.Active, TenantLifecycleStatus.Active)]
    [TestCase(TenantStatus.Suspended, TenantLifecycleStatus.Suspended)]
    public void ToLifecycleStatus_maps_each_tenant_status(TenantStatus status, TenantLifecycleStatus expected)
    {
        Assert.That(TenantLifecycleMapping.ToLifecycleStatus(status), Is.EqualTo(expected));
    }

    [TestCase(TenantRegionStatus.Provisioning, TenantRegionLifecycleStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Backfilling, TenantRegionLifecycleStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Online, TenantRegionLifecycleStatus.Online)]
    [TestCase(TenantRegionStatus.Draining, TenantRegionLifecycleStatus.Draining)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionLifecycleStatus.Offline)]
    [TestCase(TenantRegionStatus.Removed, TenantRegionLifecycleStatus.Removed)]
    public void ToLifecycleStatus_maps_each_region_status(TenantRegionStatus status, TenantRegionLifecycleStatus expected)
    {
        Assert.That(TenantLifecycleMapping.ToLifecycleStatus(status), Is.EqualTo(expected));
    }

    [Test]
    public void DescribeRegions_lists_every_named_region_in_ordinal_order()
    {
        var record = TenantRecord.Create(
            TenantId.Parse("acme"), TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 }, "seed");
        record.SetRegionStatus("west", TenantRegionStatus.Online, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        record.SetRegionStatus("east", TenantRegionStatus.Draining, new HybridLogicalClock { WallClockTicks = 3 }, "seed");

        var regions = TenantLifecycleMapping.DescribeRegions(record);

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "east", "west" }));
            Assert.That(regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Draining));
            Assert.That(regions[1].Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
        });
    }
}
