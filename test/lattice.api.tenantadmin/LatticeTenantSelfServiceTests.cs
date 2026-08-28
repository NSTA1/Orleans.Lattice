using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantSelfService"/>, the read-only tenant
/// self-awareness facade. They cover the current-tenant projection, the
/// subject-scoped accessible-tenant enumeration (including the fail-closed empty
/// result for an anonymous caller and the union of the caller's own non-default
/// tenant with the tenants it administers), and the leak-free inspect path where an
/// absent tenant and an inaccessible one are unified into a single
/// <see cref="TenantNotFoundException"/>. All doubles are deterministic - no
/// cluster, no timing, no ordering or wall-clock dependence.
/// </summary>
[TestFixture]
public sealed partial class LatticeTenantSelfServiceTests
{
    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord SeededRecord(
        string tenantId,
        TenantStatus status = TenantStatus.Active,
        IEnumerable<string>? allowed = null,
        IEnumerable<(string Region, TenantRegionStatus Status)>? statuses = null)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId), status, TenantQuotas.Unbounded, TenantPlacement.Shared, Stamp(1), "seed");
        var stamp = 2L;
        foreach (var region in allowed ?? Array.Empty<string>())
        {
            record.AuthorizeRegion(region, Stamp(stamp++), "seed");
        }

        foreach (var (region, regionStatus) in statuses ?? Array.Empty<(string, TenantRegionStatus)>())
        {
            record.SetRegionStatus(region, regionStatus, Stamp(stamp++), "seed");
        }

        return record;
    }

    private static LatticeTenantSelfService Service(
        FakeTenantRegistry registry,
        TenantId current,
        IReadOnlyList<string>? allowedTenants = null,
        LatticeSubject? subject = null)
    {
        var resolver = new FakeTenantContextResolver(current);
        var policyEngine = new FakeTenantPolicyEngine(allowedTenants ?? Array.Empty<string>());
        var membership = subject is null ? null : new FixedMembershipContext(subject.Value);
        return new LatticeTenantSelfService(resolver, policyEngine, registry, membership);
    }

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_resolver_throws()
    {
        var registry = new FakeTenantRegistry();
        Assert.That(
            () => new LatticeTenantSelfService(null!, new FakeTenantPolicyEngine([]), registry),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_policy_engine_throws()
    {
        var registry = new FakeTenantRegistry();
        Assert.That(
            () => new LatticeTenantSelfService(new FakeTenantContextResolver(TenantId.Default), null!, registry),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_registry_throws()
    {
        Assert.That(
            () => new LatticeTenantSelfService(
                new FakeTenantContextResolver(TenantId.Default), new FakeTenantPolicyEngine([]), null!),
            Throws.ArgumentNullException);
    }

    // ---- GetCurrentTenantAsync -------------------------------------------

    [Test]
    public async Task GetCurrentTenantAsync_non_default_tenant_reports_it()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        var service = Service(registry, TenantId.Parse("acme"));

        var current = await service.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(current.TenantId, Is.EqualTo("acme"));
            Assert.That(current.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(current.IsDefault, Is.False);
        });
    }

    [Test]
    public async Task GetCurrentTenantAsync_default_tenant_reports_default_active()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, TenantId.Default);

        var current = await service.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(current.IsDefault, Is.True);
            Assert.That(current.Status, Is.EqualTo(TenantLifecycleStatus.Active));
        });
    }

    [Test]
    public async Task GetCurrentTenantAsync_suspended_tenant_reports_suspended()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme", TenantStatus.Suspended));
        var service = Service(registry, TenantId.Parse("acme"));

        var current = await service.GetCurrentTenantAsync();

        Assert.That(current.Status, Is.EqualTo(TenantLifecycleStatus.Suspended));
    }

    // ---- ListAccessibleTenantsAsync --------------------------------------

    [Test]
    public async Task ListAccessibleTenantsAsync_anonymous_default_caller_gets_empty_list()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        registry.Seed(SeededRecord("beta"));

        // Default current tenant + no membership context => anonymous subject.
        var service = Service(registry, TenantId.Default);

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.That(tenants, Is.Empty);
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_includes_callers_own_non_default_tenant()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));

        // Even with no administered tenants, the caller can always see its own tenant.
        var service = Service(registry, TenantId.Parse("acme"));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "acme" }));
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_returns_administered_tenants_sorted()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("beta"));
        registry.Seed(SeededRecord("alpha"));
        var service = Service(
            registry, TenantId.Default, allowedTenants: ["beta", "alpha"], subject: new LatticeSubject("op"));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_unions_current_tenant_with_administered_deduped()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        registry.Seed(SeededRecord("beta"));
        var service = Service(
            registry, TenantId.Parse("acme"), allowedTenants: ["acme", "beta"], subject: new LatticeSubject("op"));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "acme", "beta" }));
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_reports_per_tenant_status()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("beta", TenantStatus.Suspended));
        var service = Service(
            registry, TenantId.Default, allowedTenants: ["beta"], subject: new LatticeSubject("op"));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenants, Has.Count.EqualTo(1));
            Assert.That(tenants[0].TenantId, Is.EqualTo("beta"));
            Assert.That(tenants[0].Status, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(tenants[0].IsDefault, Is.False);
        });
    }

    // ---- GetTenantAsync --------------------------------------------------

    [Test]
    public async Task GetTenantAsync_own_current_tenant_returns_report_with_regions()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            "acme",
            allowed: ["region-b", "region-a"],
            statuses: [("region-a", TenantRegionStatus.Online)]));
        var service = Service(registry, TenantId.Parse("acme"));

        var report = await service.GetTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(report.IsDefault, Is.False);
            Assert.That(report.Regions.Select(r => r.RegionId), Is.EqualTo(new[] { "region-a", "region-b" }));
            Assert.That(report.Regions.Single(r => r.RegionId == "region-a").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Online));
            Assert.That(report.Regions.Single(r => r.RegionId == "region-a").IsAllowed, Is.True);
        });
    }

    [Test]
    public async Task GetTenantAsync_administered_tenant_returns_report()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("beta"));
        var service = Service(
            registry, TenantId.Default, allowedTenants: ["beta"], subject: new LatticeSubject("op"));

        var report = await service.GetTenantAsync("beta");

        Assert.That(report.TenantId, Is.EqualTo("beta"));
    }

    [Test]
    public async Task GetTenantAsync_surfaces_the_tenants_quotas()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord("acme");
        record.SetQuotas(
            new TenantQuotas { MaxBytes = 1_000_000, MaxKeys = 5_000, BurstPercent = 15 },
            Stamp(99),
            "seed");
        registry.Seed(record);
        var service = Service(registry, TenantId.Parse("acme"));

        var report = await service.GetTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Quotas.MaxBytes, Is.EqualTo(1_000_000));
            Assert.That(report.Quotas.MaxKeys, Is.EqualTo(5_000));
            Assert.That(report.Quotas.MaxMemoryBytes, Is.Null);
            Assert.That(report.Quotas.BurstPercent, Is.EqualTo(15));
            Assert.That(report.Quotas.IsUnbounded, Is.False);
        });
    }

    [Test]
    public async Task GetTenantAsync_reports_unbounded_quotas_when_never_authored()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        var service = Service(registry, TenantId.Parse("acme"));

        var report = await service.GetTenantAsync("acme");

        Assert.That(report.Quotas.IsUnbounded, Is.True);
    }

    [Test]
    public void GetTenantAsync_inaccessible_existing_tenant_throws_not_found()
    {
        var registry = new FakeTenantRegistry();
        // The tenant exists, but the anonymous default caller may not see it, so
        // it must be indistinguishable from an absent tenant (no existence leak).
        registry.Seed(SeededRecord("gamma"));
        var service = Service(registry, TenantId.Default);

        Assert.That(
            async () => await service.GetTenantAsync("gamma"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void GetTenantAsync_accessible_but_absent_tenant_throws_not_found()
    {
        var registry = new FakeTenantRegistry();
        // The caller is authorized for "ghost" but no record exists; this unifies
        // with the inaccessible case into the same fail-closed not-found outcome.
        var service = Service(
            registry, TenantId.Default, allowedTenants: ["ghost"], subject: new LatticeSubject("op"));

        Assert.That(
            async () => await service.GetTenantAsync("ghost"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void GetTenantAsync_null_tenant_id_throws_argument()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, TenantId.Default);

        Assert.That(
            async () => await service.GetTenantAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetTenantAsync_empty_tenant_id_throws_argument()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, TenantId.Default);

        Assert.That(
            async () => await service.GetTenantAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetTenantAsync_invalid_tenant_id_throws_argument()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, TenantId.Default);

        Assert.That(
            async () => await service.GetTenantAsync("BAD"),
            Throws.InstanceOf<ArgumentException>());
    }

    /// <summary>A deterministic <see cref="ITenantContextResolver"/> returning a fixed tenant.</summary>
    private sealed class FakeTenantContextResolver : ITenantContextResolver
    {
        private readonly TenantId _current;

        public FakeTenantContextResolver(TenantId current) => _current = current;

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(_current);
    }

    /// <summary>
    /// A deterministic <see cref="ITenantPolicyEngine"/> whose
    /// <see cref="ResolveAllowedTenants"/> returns a fixed set regardless of the
    /// subject id; the other decision surfaces are unused by the self-service facade.
    /// </summary>
    private sealed class FakeTenantPolicyEngine : ITenantPolicyEngine
    {
        private readonly IReadOnlyList<TenantId> _allowed;

        public FakeTenantPolicyEngine(IReadOnlyList<string> allowed)
            => _allowed = allowed.Select(TenantId.Parse).ToArray();

        public long CurrentEpoch => 0;

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
        {
            ArgumentNullException.ThrowIfNull(subjectId);
            return _allowed;
        }

        public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant)
            => throw new NotSupportedException();

        public TenantAccessDecision ResolveCrossTenantGrant(
            TenantId sourceTenant, TenantId targetTenant, string scope, TenantGrantOperations operation)
            => throw new NotSupportedException();
    }
}
