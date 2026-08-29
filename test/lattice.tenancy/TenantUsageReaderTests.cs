using static Orleans.Lattice.Tenancy.Tests.ObservabilityTestData;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;
using FixedScopeResolver = Orleans.Lattice.Tenancy.Tests.UsageTestData.FixedScopeResolver;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantUsageReader"/>: the by-id join of the warm usage
/// index, the tenant's enforcement scope, and the durable metered-overage seam into
/// a <see cref="TenantUsageReading"/>. Covers the aggregate selected per scope, the
/// reported scope, the metered-overage fold, the absent and uninitialised tenants,
/// and the constructor null guards. Driven entirely against in-memory fakes with
/// fixed samples, so nothing depends on timing, ordering, or the wall clock.
/// </summary>
[TestFixture]
public sealed class TenantUsageReaderTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantUsageReader Create(
        FakeTenantUsageIndex usage,
        FakeTenantOverageBilling billing,
        TenantEnforcementScope scope = TenantEnforcementScope.GlobalConverged) =>
        new(usage, billing, new FixedScopeResolver(scope));

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var index = new FakeTenantUsageIndex();
        var billing = new FakeTenantOverageBilling();
        var resolver = new FixedScopeResolver(TenantEnforcementScope.GlobalConverged);

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantUsageReader(null!, billing, resolver), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageReader(index, null!, resolver), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageReader(index, billing, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task ReadAsync_joins_usage_quotas_and_metered_overage()
    {
        var index = new FakeTenantUsageIndex()
            .With(Acme, View(Quotas(bytes: 1000, burstPercent: 10), Usage(bytes: 500, keys: 5)));
        var billing = new FakeTenantOverageBilling().With(Acme, Overage(bytes: 7));

        var reading = await Create(index, billing).ReadAsync(Acme);

        Assert.That(reading, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(reading!.Value.Snapshot.Tenant, Is.EqualTo(Acme));
            Assert.That(reading.Value.Snapshot.Usage, Is.EqualTo(Usage(bytes: 500, keys: 5)));
            Assert.That(reading.Value.Snapshot.Quotas, Is.EqualTo(Quotas(bytes: 1000, burstPercent: 10)));
            Assert.That(reading.Value.Snapshot.MeteredOverage, Is.EqualTo(Overage(bytes: 7)));
        });
    }

    [Test]
    public async Task ReadAsync_under_global_scope_reports_the_cross_cluster_fold()
    {
        var index = new FakeTenantUsageIndex().With(
            Acme,
            new TenantUsageView(Quotas(bytes: 1000), globalUsage: Usage(bytes: 900), localUsage: Usage(bytes: 300)));

        var reading = await Create(index, new FakeTenantOverageBilling(), TenantEnforcementScope.GlobalConverged)
            .ReadAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(reading!.Value.Snapshot.Usage.Bytes, Is.EqualTo(900), "the global fold is the admitted aggregate");
            Assert.That(reading.Value.Scope, Is.EqualTo(TenantEnforcementScope.GlobalConverged));
        });
    }

    [Test]
    public async Task ReadAsync_under_per_cluster_scope_reports_the_local_slot_and_says_so()
    {
        var index = new FakeTenantUsageIndex().With(
            Acme,
            new TenantUsageView(Quotas(bytes: 1000), globalUsage: Usage(bytes: 900), localUsage: Usage(bytes: 300)));

        var reading = await Create(index, new FakeTenantOverageBilling(), TenantEnforcementScope.PerCluster)
            .ReadAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(reading!.Value.Snapshot.Usage.Bytes, Is.EqualTo(300), "the local slot is the admitted aggregate");
            Assert.That(
                reading.Value.Scope,
                Is.EqualTo(TenantEnforcementScope.PerCluster),
                "a local figure must never be reported without the qualifier that says it is local");
        });
    }

    [Test]
    public async Task ReadAsync_derives_instantaneous_overage_from_the_scoped_aggregate()
    {
        var index = new FakeTenantUsageIndex().With(
            Acme,
            new TenantUsageView(Quotas(bytes: 500), globalUsage: Usage(bytes: 900), localUsage: Usage(bytes: 300)));
        var reader = Create(index, new FakeTenantOverageBilling(), TenantEnforcementScope.PerCluster);

        var reading = await reader.ReadAsync(Acme);

        Assert.That(
            reading!.Value.Snapshot.InstantaneousOverage,
            Is.EqualTo(TenantOverageSample.Empty),
            "the local slot is within quota, so the per-cluster reading is not in overage even though the global fold is");
    }

    [Test]
    public async Task ReadAsync_folds_empty_overage_when_the_tenant_never_breached()
    {
        var index = new FakeTenantUsageIndex().With(Acme, View(Quotas(bytes: 1000), Usage(bytes: 500)));

        var reading = await Create(index, new FakeTenantOverageBilling()).ReadAsync(Acme);

        Assert.That(reading!.Value.Snapshot.MeteredOverage, Is.EqualTo(TenantOverageSample.Empty));
    }

    [Test]
    public async Task ReadAsync_absent_tenant_is_null()
    {
        var reading = await Create(new FakeTenantUsageIndex(), new FakeTenantOverageBilling()).ReadAsync(Acme);

        Assert.That(reading, Is.Null, "a tenant with no warm usage view has no reading");
    }

    [Test]
    public async Task ReadAsync_uninitialised_tenant_is_null_and_reads_nothing()
    {
        var index = new FakeTenantUsageIndex();

        var reading = await Create(index, new FakeTenantOverageBilling()).ReadAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(reading, Is.Null);
            Assert.That(index.WarmCount, Is.EqualTo(0), "the uninitialised tenant short-circuits before warming the index");
        });
    }

    [Test]
    public async Task ReadAsync_warms_the_index_before_reading()
    {
        var index = new FakeTenantUsageIndex().With(Acme, View(Quotas(), Usage()));

        _ = await Create(index, new FakeTenantOverageBilling()).ReadAsync(Acme);

        Assert.That(index.WarmCount, Is.EqualTo(1));
    }

    [Test]
    public void ResolveScope_reports_the_scope_without_touching_the_index()
    {
        var index = new FakeTenantUsageIndex();
        var reader = Create(index, new FakeTenantOverageBilling(), TenantEnforcementScope.PerCluster);

        Assert.Multiple(() =>
        {
            Assert.That(reader.ResolveScope(Acme), Is.EqualTo(TenantEnforcementScope.PerCluster));
            Assert.That(index.WarmCount, Is.EqualTo(0), "resolving a scope is a pure lookup");
        });
    }

    [Test]
    public void TenantUsageReading_carries_the_snapshot_and_scope_it_was_constructed_with()
    {
        var snapshot = new TenantObservabilitySnapshot(
            Acme, Usage(bytes: 5), Quotas(bytes: 10), Overage(bytes: 1));

        var reading = new TenantUsageReading(snapshot, TenantEnforcementScope.PerCluster);

        Assert.Multiple(() =>
        {
            Assert.That(reading.Snapshot, Is.EqualTo(snapshot));
            Assert.That(reading.Scope, Is.EqualTo(TenantEnforcementScope.PerCluster));
            Assert.That(
                reading with { Scope = TenantEnforcementScope.GlobalConverged },
                Is.Not.EqualTo(reading),
                "the reading is a value: changing the scope changes the value");
        });
    }
}
