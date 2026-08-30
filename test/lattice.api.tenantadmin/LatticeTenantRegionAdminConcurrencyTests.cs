using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Concurrency regression tests for <see cref="LatticeTenantRegionAdmin"/> over a
/// CRDT-merged <see cref="ITenantRegistry"/>.
/// <para>
/// The last-resident-region invariant cannot be held by a pre-write read-check
/// alone: <c>TenantRecord.RegionStatuses</c> is an LWW-element-map keyed by
/// region id, so two callers draining <i>different</i> regions each pass the guard
/// and the join keeps both tombstones, emptying residency. These tests drive that
/// exact interleaving deterministically - the competing write is folded in inside
/// the caller's read-to-write window at an explicitly supplied stamp, so there are
/// no threads, no clock, and no ordering assumption.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeTenantRegionAdminConcurrencyTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    /// <summary>
    /// A stamp that supersedes every seeded stamp but stays far below the
    /// wall-clock stamps the admin's own monotonic clock mints, so the competing
    /// write wins its own slot and the admin's later self-heal wins the slot it
    /// re-asserts.
    /// </summary>
    private static HybridLogicalClock RacingStamp => Stamp(1_000);

    private static TenantRecord SeededRecord(
        IEnumerable<string> allowed,
        IEnumerable<(string Region, TenantRegionStatus Status)> statuses)
    {
        var record = TenantRecord.Create(
            Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Stamp(1), "seed");
        var stamp = 2L;
        foreach (var region in allowed)
        {
            record.AuthorizeRegion(region, Stamp(stamp++), "seed");
        }

        foreach (var (region, status) in statuses)
        {
            record.SetRegionStatus(region, status, Stamp(stamp++), "seed");
        }

        return record;
    }

    private static LatticeTenantRegionAdmin Admin(ITenantRegistry registry)
    {
        var authorizer = new TenantRegionResidencyAuthorizer(
            new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op")));
        return new LatticeTenantRegionAdmin(
            registry,
            authorizer,
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));
    }

    [Test]
    public void SetResidencyAsync_refuses_when_a_concurrent_removal_would_empty_residency()
    {
        // Both regions are resident. This caller drops region-b; a second caller
        // drops region-a inside this call's read-to-write window. Each passes its
        // own pre-write guard, and the tombstones land on different keys.
        var registry = new RacingResidencyRemovalRegistry(Acme, "region-a", RacingStamp);
        registry.Seed(SeededRecord(
            ["region-a", "region-b"],
            [("region-a", TenantRegionStatus.Online), ("region-b", TenantRegionStatus.Online)]));
        var admin = Admin(registry);

        Assert.ThrowsAsync<TenantLastRegionException>(
            () => admin.SetResidencyAsync("acme", ["region-a"]));
    }

    [Test]
    public async Task SetResidencyAsync_repairs_only_its_own_removal_when_refused_by_the_merged_record()
    {
        var registry = new RacingResidencyRemovalRegistry(Acme, "region-a", RacingStamp);
        registry.Seed(SeededRecord(
            ["region-a", "region-b"],
            [("region-a", TenantRegionStatus.Online), ("region-b", TenantRegionStatus.Online)]));
        var admin = Admin(registry);

        try
        {
            await admin.SetResidencyAsync("acme", ["region-a"]);
            Assert.Fail("Expected TenantLastRegionException.");
        }
        catch (TenantLastRegionException)
        {
            // Expected; the assertions below are about what the repair left behind.
        }

        var stored = registry.Peek("acme")!;
        Assert.Multiple(() =>
        {
            Assert.That(stored.ResidentRegionCount, Is.GreaterThan(0),
                "The whole point of the guard is that a tenant is never left with zero "
                + "resident regions.");
            Assert.That(stored.GetRegionStatus("region-b"), Is.EqualTo(TenantRegionStatus.Online),
                "The repair must restore the region this call drained, at its prior status.");
            Assert.That(stored.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Draining),
                "The repair must leave the other caller's removal standing - re-asserting the "
                + "whole pre-merge set would silently undo their legitimate write.");
        });
    }

    [Test]
    public async Task SetResidencyAsync_succeeds_when_a_concurrent_removal_leaves_a_resident_region()
    {
        // Three regions resident. This caller drops region-c; the racer drops
        // region-a. region-b survives, so the invariant holds and the post-merge
        // re-check must not fire - it is not a blanket "re-check after write".
        var registry = new RacingResidencyRemovalRegistry(Acme, "region-a", RacingStamp);
        registry.Seed(SeededRecord(
            ["region-a", "region-b", "region-c"],
            [
                ("region-a", TenantRegionStatus.Online),
                ("region-b", TenantRegionStatus.Online),
                ("region-c", TenantRegionStatus.Online),
            ]));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", ["region-a", "region-b"]);

        Assert.Multiple(() =>
        {
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "region-c" }));
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-b"),
                Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-c"),
                Is.EqualTo(TenantRegionStatus.Draining));
        });
    }

    [Test]
    public async Task SetResidencyAsync_reports_regions_from_the_merged_record()
    {
        // A concurrent writer brings region-c online on a key this caller never
        // touched. The reported region set is the registry's committed join, so
        // the concurrent change is present rather than silently absent.
        var registry = new RacingResidencyGrantRegistry(Acme, "region-c", RacingStamp);
        registry.Seed(SeededRecord(
            ["region-a", "region-b", "region-c"],
            [("region-a", TenantRegionStatus.Online), ("region-b", TenantRegionStatus.Online)]));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", ["region-a"]);

        var regionC = result.Regions.SingleOrDefault(r => r.RegionId == "region-c");
        Assert.Multiple(() =>
        {
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "region-b" }));
            Assert.That(regionC, Is.Not.Null,
                "The result must be built from the merged record, not the pre-merge local view.");
            Assert.That(regionC!.Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
        });
    }

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_reports_allowed_regions_from_the_merged_record()
    {
        var registry = new RacingAllowedRegionRegistry(Acme, "region-z", RacingStamp);
        registry.Seed(SeededRecord(["region-a"], []));
        var admin = Admin(registry);

        var result = await admin.AuthorizeAllowedRegionsAsync("acme", ["region-a", "region-b"]);

        Assert.That(result.AllowedRegions, Does.Contain("region-z"),
            "The result must be built from the merged record, so a concurrently authorized "
            + "region is not silently absent from the reported allowed set.");
    }
}
