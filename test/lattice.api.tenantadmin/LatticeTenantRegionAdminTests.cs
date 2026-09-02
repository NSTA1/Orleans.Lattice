using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantRegionAdmin"/>, the three-operation
/// per-tenant region-residency facade: operator-authorized allowed-region set,
/// tenant-admin residency within the allowed set, and a queryable per-region status
/// report. Covers the allowed-set subset invariant, the last-resident-region guard,
/// the add (-&gt; Provisioning) and remove (-&gt; Draining) residency transitions,
/// idempotency, and the input guards. All doubles are deterministic - no cluster, no
/// timing.
/// </summary>
[TestFixture]
public sealed class LatticeTenantRegionAdminTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord SeededRecord(
        IEnumerable<string>? allowed = null,
        IEnumerable<(string Region, TenantRegionStatus Status)>? statuses = null)
    {
        var record = TenantRecord.Create(
            Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Stamp(1), "seed");
        var stamp = 2L;
        foreach (var region in allowed ?? Array.Empty<string>())
        {
            record.AuthorizeRegion(region, Stamp(stamp++), "seed");
        }

        foreach (var (region, status) in statuses ?? Array.Empty<(string, TenantRegionStatus)>())
        {
            record.SetRegionStatus(region, status, Stamp(stamp++), "seed");
        }

        return record;
    }

    private static LatticeTenantRegionAdmin Admin(FakeTenantRegistry registry, bool authorized = true)
    {
        var gate = new FixedGate(allow: authorized);
        var authorizer = new TenantRegionResidencyAuthorizer(
            gate, registry, new FixedMembershipContext(new LatticeSubject("op")));
        return new LatticeTenantRegionAdmin(
            registry, authorizer, new IncrementingClock(), Options.Create(new ClusterOptions { ClusterId = "region-a" }));
    }

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_registry_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantRegionAdmin(
                null!, authorizer, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_authorizer_throws() =>
        Assert.That(
            () => new LatticeTenantRegionAdmin(
                new FakeTenantRegistry(), null!, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_clock_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantRegionAdmin(registry, authorizer, null!, Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_cluster_options_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantRegionAdmin(registry, authorizer, new IncrementingClock(), null!),
            Throws.ArgumentNullException);
    }

    // ---- AuthorizeAllowedRegionsAsync (operator) -------------------------

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_authorizes_the_desired_regions()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord());
        var admin = Admin(registry);

        var result = await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a", "region-b" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AllowedRegions, Is.EqualTo(new[] { "region-a", "region-b" }));
            Assert.That(registry.Peek("acme")!.IsRegionAllowed("region-a"), Is.True);
            Assert.That(registry.Peek("acme")!.IsRegionAllowed("region-b"), Is.True);
            Assert.That(registry.Puts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_revokes_a_region_dropped_from_the_desired_set()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(allowed: new[] { "region-a", "region-b" }));
        var admin = Admin(registry);

        var result = await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AllowedRegions, Is.EqualTo(new[] { "region-a" }));
            Assert.That(registry.Peek("acme")!.IsRegionAllowed("region-b"), Is.False);
        });
    }

    [Test]
    public async Task AuthorizeAllowedRegionsAsync_is_a_no_op_write_when_the_set_is_unchanged()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(allowed: new[] { "region-a" }));
        var admin = Admin(registry);

        await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a" });

        Assert.That(registry.Puts, Is.Zero, "an unchanged allowed set must not write to the registry");
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_refuses_to_revoke_a_still_resident_region()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a", "region-b" },
            statuses: new[] { ("region-b", TenantRegionStatus.Online) }));
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a" }),
                Throws.TypeOf<TenantRegionNotAllowedException>());
            Assert.That(registry.Puts, Is.Zero, "the rejection must apply nothing");
        });
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_on_a_missing_tenant_throws_not_found()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a" }),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_a_gate_denial_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord());
        var admin = Admin(registry, authorized: false);

        Assert.That(
            async () => await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a" }),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_null_region_set_throws()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.AuthorizeAllowedRegionsAsync("acme", null!),
            Throws.ArgumentNullException);
    }

    [TestCase(null)]
    [TestCase("")]
    public void AuthorizeAllowedRegionsAsync_null_or_empty_tenant_throws(string? tenantId)
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.AuthorizeAllowedRegionsAsync(tenantId!, new[] { "region-a" }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AuthorizeAllowedRegionsAsync_an_empty_region_id_in_the_set_throws()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.AuthorizeAllowedRegionsAsync("acme", new[] { "region-a", "" }),
            Throws.TypeOf<ArgumentException>());
    }

    // ---- SetResidencyAsync (tenant admin) --------------------------------

    [Test]
    public async Task SetResidencyAsync_begins_provisioning_a_newly_requested_region()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(allowed: new[] { "region-a" }));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", new[] { "region-a" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.EqualTo(new[] { "region-a" }));
            Assert.That(result.RemovedRegions, Is.Empty);
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Provisioning));
        });
    }

    [Test]
    public async Task SetResidencyAsync_begins_draining_a_region_dropped_from_the_set()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a", "region-b" },
            statuses: new[]
            {
                ("region-a", TenantRegionStatus.Online),
                ("region-b", TenantRegionStatus.Online),
            }));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", new[] { "region-a" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.Empty);
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "region-b" }));
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-b"), Is.EqualTo(TenantRegionStatus.Draining));
            Assert.That(registry.Peek("acme")!.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Online));
        });
    }

    [Test]
    public void SetResidencyAsync_refuses_a_region_that_is_not_allowed()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(allowed: new[] { "region-a" }));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.SetResidencyAsync("acme", new[] { "region-b" }),
            Throws.TypeOf<TenantRegionNotAllowedException>());
    }

    [Test]
    public void SetResidencyAsync_refuses_to_empty_the_residency_of_a_resident_tenant()
    {
        // Last-resident-region guard: a tenant must always be resident somewhere.
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a" },
            statuses: new[] { ("region-a", TenantRegionStatus.Online) }));
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.SetResidencyAsync("acme", Array.Empty<string>()),
                Throws.TypeOf<TenantLastRegionException>());
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task SetResidencyAsync_an_empty_set_on_a_non_resident_tenant_is_a_no_op()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(allowed: new[] { "region-a" }));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", Array.Empty<string>());

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.Empty);
            Assert.That(result.RemovedRegions, Is.Empty);
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task SetResidencyAsync_is_idempotent_for_an_already_resident_set()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a" },
            statuses: new[] { ("region-a", TenantRegionStatus.Online) }));
        var admin = Admin(registry);

        var result = await admin.SetResidencyAsync("acme", new[] { "region-a" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.Empty);
            Assert.That(result.RemovedRegions, Is.Empty);
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task SetResidencyAsync_a_tenant_admin_subject_is_authorized_even_when_the_gate_denies()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(allowed: new[] { "region-a" });
        record.AddAdminSubject("tenant-admin", Stamp(100), "seed");
        registry.Seed(record);
        var authorizer = new TenantRegionResidencyAuthorizer(
            new FixedGate(allow: false), registry, new FixedMembershipContext(new LatticeSubject("tenant-admin")));
        var admin = new LatticeTenantRegionAdmin(
            registry, authorizer, new IncrementingClock(), Options.Create(new ClusterOptions { ClusterId = "region-a" }));

        var result = await admin.SetResidencyAsync("acme", new[] { "region-a" });

        Assert.That(result.AddedRegions, Is.EqualTo(new[] { "region-a" }));
    }

    [Test]
    public void SetResidencyAsync_null_region_set_throws()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.SetResidencyAsync("acme", null!),
            Throws.ArgumentNullException);
    }

    // ---- GetTenantRegionStatusAsync --------------------------------------

    [Test]
    public async Task GetTenantRegionStatusAsync_reports_allowed_and_status_regions()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a", "region-b" },
            statuses: new[] { ("region-a", TenantRegionStatus.Online) }));
        var admin = Admin(registry);

        var report = await admin.GetTenantRegionStatusAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Regions, Has.Count.EqualTo(2));
            Assert.That(report.Regions[0].RegionId, Is.EqualTo("region-a"));
            Assert.That(report.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
            Assert.That(report.Regions[0].IsAllowed, Is.True);
            Assert.That(report.Regions[1].RegionId, Is.EqualTo("region-b"));
            Assert.That(report.Regions[1].Status, Is.EqualTo(TenantRegionLifecycleStatus.None));
            Assert.That(report.Regions[1].IsAllowed, Is.True);
        });
    }

    [Test]
    public void GetTenantRegionStatusAsync_a_gate_denial_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord());
        var admin = Admin(registry, authorized: false);

        Assert.That(
            async () => await admin.GetTenantRegionStatusAsync("acme"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task GetTenantRegionStatusAsync_maps_backfilling_and_offline_region_statuses()
    {
        // The remaining lifecycle arms: a region mid-backfill and a fully offline
        // region map to their distinct lifecycle statuses, so the report never
        // collapses a transitional or terminated region into the None default.
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            allowed: new[] { "region-a", "region-b" },
            statuses: new[]
            {
                ("region-a", TenantRegionStatus.Backfilling),
                ("region-b", TenantRegionStatus.Offline),
            }));
        var admin = Admin(registry);

        var report = await admin.GetTenantRegionStatusAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Backfilling));
            Assert.That(report.Regions[1].Status, Is.EqualTo(TenantRegionLifecycleStatus.Offline));
        });
    }

    [Test]
    public void GetTenantRegionStatusAsync_an_invalid_tenant_id_throws()
    {
        // A syntactically invalid id is rejected before any authorization or read,
        // so a malformed request never reaches the registry.
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.GetTenantRegionStatusAsync("BAD_ID"),
            Throws.ArgumentException);
    }
}
