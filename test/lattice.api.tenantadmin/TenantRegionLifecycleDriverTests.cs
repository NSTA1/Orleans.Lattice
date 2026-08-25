using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRegionLifecycleDriver"/>, the internal
/// system-driven promotion driver that advances a region one legal lifecycle step
/// at a time (Provisioning -&gt; Backfilling -&gt; Online on the add path;
/// Draining -&gt; Offline -&gt; Removed on the remove path) and is an idempotent
/// no-op at a terminal or non-transitional status. Deterministic doubles only - no
/// timing, no ordering.
/// </summary>
[TestFixture]
public sealed class TenantRegionLifecycleDriverTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord RecordWith(TenantRegionStatus? status)
    {
        var record = TenantRecord.Create(
            Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Stamp(1), "seed");
        record.AuthorizeRegion("region-a", Stamp(2), "seed");
        if (status is { } s)
        {
            record.SetRegionStatus("region-a", s, Stamp(3), "seed");
        }

        return record;
    }

    private static TenantRegionLifecycleDriver Driver(FakeTenantRegistry registry) =>
        new(registry, new IncrementingClock(), Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_registry_throws() =>
        Assert.That(
            () => new TenantRegionLifecycleDriver(null!, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_clock_throws() =>
        Assert.That(
            () => new TenantRegionLifecycleDriver(new FakeTenantRegistry(), null!, Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_cluster_options_throws() =>
        Assert.That(
            () => new TenantRegionLifecycleDriver(new FakeTenantRegistry(), new IncrementingClock(), null!),
            Throws.ArgumentNullException);

    // ---- add path --------------------------------------------------------

    [Test]
    public async Task AdvanceAsync_drives_the_full_add_path_to_online()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Provisioning));
        var driver = Driver(registry);

        var afterFirst = await driver.AdvanceAsync(Acme, "region-a");
        var afterSecond = await driver.AdvanceAsync(Acme, "region-a");
        var afterThird = await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(TenantRegionStatus.Backfilling));
            Assert.That(afterSecond, Is.EqualTo(TenantRegionStatus.Online));
            // Online is terminal on the add path: a further advance is a no-op.
            Assert.That(afterThird, Is.EqualTo(TenantRegionStatus.Online));
        });
    }

    // ---- remove path -----------------------------------------------------

    [Test]
    public async Task AdvanceAsync_drives_the_full_remove_path_to_removed()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Draining));
        var driver = Driver(registry);

        var afterFirst = await driver.AdvanceAsync(Acme, "region-a");
        var afterSecond = await driver.AdvanceAsync(Acme, "region-a");
        var afterThird = await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(TenantRegionStatus.Offline));
            Assert.That(afterSecond, Is.EqualTo(TenantRegionStatus.Removed));
            // Removed is terminal: a further advance is a no-op.
            Assert.That(afterThird, Is.EqualTo(TenantRegionStatus.Removed));
        });
    }

    [Test]
    public async Task AdvanceAsync_persists_each_promotion()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Provisioning));
        var driver = Driver(registry);

        await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Backfilling));
            Assert.That(registry.Puts, Is.EqualTo(1));
        });
    }

    // ---- no-op paths -----------------------------------------------------

    [Test]
    public async Task AdvanceAsync_is_a_no_op_for_a_region_with_no_status()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(status: null));
        var driver = Driver(registry);

        var result = await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(TenantRegionStatus.None));
            Assert.That(registry.Puts, Is.Zero, "a non-transitional status must not write");
        });
    }

    [Test]
    public async Task AdvanceAsync_is_a_no_op_at_a_terminal_status()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Online));
        var driver = Driver(registry);

        var result = await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    // ---- guards ----------------------------------------------------------

    [Test]
    public void AdvanceAsync_on_a_missing_tenant_throws_not_found()
    {
        var driver = Driver(new FakeTenantRegistry());

        Assert.That(
            async () => await driver.AdvanceAsync(Acme, "region-a"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [TestCase(null)]
    [TestCase("")]
    public void AdvanceAsync_null_or_empty_region_throws(string? regionId)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Provisioning));
        var driver = Driver(registry);

        Assert.That(
            async () => await driver.AdvanceAsync(Acme, regionId!),
            Throws.InstanceOf<ArgumentException>());
    }
}
