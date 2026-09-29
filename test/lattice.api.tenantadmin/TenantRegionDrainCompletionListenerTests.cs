using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRegionDrainCompletionListener"/>, the
/// residency-change listener that completes the drain of the silo's own serving
/// region automatically (issue #3897) and never drives the add path.
/// Deterministic doubles only.
/// </summary>
[TestFixture]
public sealed class TenantRegionDrainCompletionListenerTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantRecord RecordWith(TenantRegionStatus status)
    {
        var record = TenantRecord.Create(
            Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 }, "seed");
        record.SetRegionStatus("region-a", status, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        return record;
    }

    private static TenantRegionDrainCompletionListener Listener(ITenantRegistry registry) =>
        new(
            new TenantRegionLifecycleDriver(registry, Options.Create(new ClusterOptions { ClusterId = "region-a" })),
            NullLogger<TenantRegionDrainCompletionListener>.Instance);

    private static TenantRegionStatusChange Change(TenantRegionStatus previous, TenantRegionStatus current) =>
        new(Acme, "region-a", previous, current);

    [Test]
    public void Ctor_null_driver_throws() =>
        Assert.That(
            () => new TenantRegionDrainCompletionListener(null!, NullLogger<TenantRegionDrainCompletionListener>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_logger_throws() =>
        Assert.That(
            () => new TenantRegionDrainCompletionListener(
                new TenantRegionLifecycleDriver(new FakeTenantRegistry(), Options.Create(new ClusterOptions())), null!),
            Throws.ArgumentNullException);

    [TestCase(TenantRegionStatus.Draining, TenantRegionStatus.Offline)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionStatus.Removed)]
    public async Task A_drain_step_transition_applies_the_next_remove_path_step(
        TenantRegionStatus current, TenantRegionStatus expected)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(current));

        await Listener(registry).OnRegionStatusChangedAsync(
            Change(TenantRegionStatus.Online, current), CancellationToken.None);

        Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(expected));
    }

    [TestCase(TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Removed)]
    [TestCase(TenantRegionStatus.None)]
    public async Task A_transition_off_the_remove_path_writes_nothing(TenantRegionStatus current)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(current));

        await Listener(registry).OnRegionStatusChangedAsync(
            Change(TenantRegionStatus.None, current), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(registry.Puts, Is.Zero, "the add path is the operator's step, never the listener's");
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(current));
        });
    }

    [Test]
    public async Task A_stale_drain_notification_does_not_advance_a_region_since_re_added()
    {
        // The snapshot saw Draining, but the tenant admin re-added the region before
        // the listener ran: the committed status is Provisioning, and the drain
        // listener must not push it along the add path.
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Provisioning));

        await Listener(registry).OnRegionStatusChangedAsync(
            Change(TenantRegionStatus.Online, TenantRegionStatus.Draining), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(registry.Puts, Is.Zero);
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Provisioning));
        });
    }

    [Test]
    public void A_drain_notification_for_a_deleted_tenant_is_not_a_fault() =>
        Assert.That(
            async () => await Listener(new FakeTenantRegistry()).OnRegionStatusChangedAsync(
                Change(TenantRegionStatus.Online, TenantRegionStatus.Draining), CancellationToken.None),
            Throws.Nothing);
}
