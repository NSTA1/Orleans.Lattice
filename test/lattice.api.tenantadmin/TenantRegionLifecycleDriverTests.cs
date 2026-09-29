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

    private static TenantRegionLifecycleDriver Driver(ITenantRegistry registry) =>
        new(registry, Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_registry_throws() =>
        Assert.That(
            () => new TenantRegionLifecycleDriver(null!, Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_cluster_options_throws() =>
        Assert.That(
            () => new TenantRegionLifecycleDriver(new FakeTenantRegistry(), null!),
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

    // ---- drain-only advance (issue #3897) ---------------------------------

    [Test]
    public async Task CompleteDrainStepAsync_drives_the_remove_path_to_removed()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Draining));
        var driver = Driver(registry);

        var afterFirst = await driver.CompleteDrainStepAsync(Acme, "region-a");
        var afterSecond = await driver.CompleteDrainStepAsync(Acme, "region-a");
        var afterThird = await driver.CompleteDrainStepAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(TenantRegionStatus.Offline));
            Assert.That(afterSecond, Is.EqualTo(TenantRegionStatus.Removed));
            Assert.That(afterThird, Is.EqualTo(TenantRegionStatus.Removed));
            Assert.That(registry.Puts, Is.EqualTo(2), "the terminal status writes nothing");
        });
    }

    [TestCase(TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Removed)]
    public async Task CompleteDrainStepAsync_never_advances_a_status_off_the_remove_path(TenantRegionStatus status)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(status));
        var driver = Driver(registry);

        var result = await driver.CompleteDrainStepAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(status), "an added region must never be promoted by the drain path");
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void CompleteDrainStepAsync_on_a_missing_tenant_throws_not_found() =>
        Assert.That(
            async () => await Driver(new FakeTenantRegistry()).CompleteDrainStepAsync(Acme, "region-a"),
            Throws.TypeOf<TenantNotFoundException>());

    [TestCase(null)]
    [TestCase("")]
    public void CompleteDrainStepAsync_null_or_empty_region_throws(string? regionId)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(RecordWith(TenantRegionStatus.Draining));

        Assert.That(
            async () => await Driver(registry).CompleteDrainStepAsync(Acme, regionId!),
            Throws.InstanceOf<ArgumentException>());
    }

    [TestCase(TenantRegionStatus.Draining, true)]
    [TestCase(TenantRegionStatus.Offline, true)]
    [TestCase(TenantRegionStatus.None, false)]
    [TestCase(TenantRegionStatus.Provisioning, false)]
    [TestCase(TenantRegionStatus.Backfilling, false)]
    [TestCase(TenantRegionStatus.Online, false)]
    [TestCase(TenantRegionStatus.Removed, false)]
    public void IsDrainStep_classifies_only_the_statuses_whose_next_step_completes_a_drain(
        TenantRegionStatus status, bool expected) =>
        Assert.That(TenantRegionLifecycleDriver.IsDrainStep(status), Is.EqualTo(expected));

    // ---- concurrency (issue #3897) ---------------------------------------

    [Test]
    public async Task AdvanceAsync_never_overwrites_a_residency_write_that_lands_after_its_read()
    {
        // The driver reads Provisioning; before it writes, a tenant admin drains the
        // region at a later wall-clock stamp. A promotion stamped at wall-now would
        // supersede that later write under the per-field LWW join and silently undo
        // the admin's removal (resurrecting the region as Backfilling). The promotion
        // must supersede only the version it observed, so the admin's write wins.
        var registry = new ConcurrentWriteRegistry(
            RecordWith(TenantRegionStatus.Provisioning),
            onRead: stored => stored.SetRegionStatus(
                "region-a", TenantRegionStatus.Draining, HybridLogicalClock.Tick(HybridLogicalClock.Zero), "admin"));
        var driver = Driver(registry);

        var result = await driver.AdvanceAsync(Acme, "region-a");

        Assert.Multiple(() =>
        {
            Assert.That(
                registry.Stored.GetRegionStatus("region-a"),
                Is.EqualTo(TenantRegionStatus.Draining),
                "the admin's later removal must survive the driver's stale promotion");
            Assert.That(result, Is.EqualTo(TenantRegionStatus.Draining), "the driver reports the committed join, not its own intent");
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

    /// <summary>
    /// A registry double with the production read-merge-write shape: a read hands
    /// back an independent copy, and a write is joined into the stored record with
    /// the record's own LWW merge. <c>onRead</c> runs against the stored record just
    /// after a copy is handed out, modelling a competing writer that commits between
    /// the caller's read and its write.
    /// </summary>
    private sealed class ConcurrentWriteRegistry(TenantRecord seed, Action<TenantRecord> onRead) : ITenantRegistry
    {
        private bool _raced;

        public TenantRecord Stored { get; } = seed;

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            var copy = Stored.Clone();
            if (!_raced)
            {
                _raced = true;
                onRead(Stored);
            }

            return Task.FromResult<TenantRecord?>(copy);
        }

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return Stored.Clone();
            await Task.CompletedTask.ConfigureAwait(false);
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            Stored.MergeFrom(record);
            return Task.FromResult(Stored.Clone());
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(false);
    }
}
