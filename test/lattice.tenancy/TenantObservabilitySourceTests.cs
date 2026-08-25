using static Orleans.Lattice.Tenancy.Tests.ObservabilityTestData;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilitySource"/>: the composition of the
/// warm usage index and the durable overage-billing seam into
/// <see cref="TenantObservabilitySnapshot"/> projections. Covers the single-tenant
/// read (present, absent, uninitialised), the bulk join (with and without metered
/// overage, and empty), and the constructor null guards. Driven against in-memory
/// fakes, so there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class TenantObservabilitySourceTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static TenantObservabilitySource Create(FakeTenantUsageIndex usage, FakeTenantOverageBilling billing) =>
        new(usage, billing);

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantObservabilitySource(null!, new FakeTenantOverageBilling()), Throws.ArgumentNullException);
            Assert.That(() => new TenantObservabilitySource(new FakeTenantUsageIndex(), null!), Throws.ArgumentNullException);
        });
    }

    // ---- SnapshotOneAsync ----------------------------------------------

    [Test]
    public async Task SnapshotOneAsync_joins_usage_quotas_and_metered_overage()
    {
        var usage = new FakeTenantUsageIndex().With(Acme, View(Quotas(bytes: 1000, burstPercent: 10), Usage(bytes: 500, keys: 5)));
        var billing = new FakeTenantOverageBilling().With(Acme, Overage(bytes: 7));

        var snapshot = await Create(usage, billing).SnapshotOneAsync(Acme);

        Assert.That(snapshot, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(snapshot!.Value.Tenant, Is.EqualTo(Acme));
            Assert.That(snapshot.Value.Usage, Is.EqualTo(Usage(bytes: 500, keys: 5)));
            Assert.That(snapshot.Value.Quotas, Is.EqualTo(Quotas(bytes: 1000, burstPercent: 10)));
            Assert.That(snapshot.Value.MeteredOverage, Is.EqualTo(Overage(bytes: 7)));
        });
    }

    [Test]
    public async Task SnapshotOneAsync_folds_empty_overage_when_the_tenant_never_breached()
    {
        var usage = new FakeTenantUsageIndex().With(Acme, View(Quotas(bytes: 1000), Usage(bytes: 500)));
        var billing = new FakeTenantOverageBilling();

        var snapshot = await Create(usage, billing).SnapshotOneAsync(Acme);

        Assert.That(snapshot!.Value.MeteredOverage, Is.EqualTo(TenantOverageSample.Empty));
    }

    [Test]
    public async Task SnapshotOneAsync_absent_tenant_is_null()
    {
        var snapshot = await Create(new FakeTenantUsageIndex(), new FakeTenantOverageBilling()).SnapshotOneAsync(Acme);

        Assert.That(snapshot, Is.Null, "a tenant with no warm usage view has no snapshot");
    }

    [Test]
    public async Task SnapshotOneAsync_uninitialised_tenant_is_null_and_reads_nothing()
    {
        var usage = new FakeTenantUsageIndex();

        var snapshot = await Create(usage, new FakeTenantOverageBilling()).SnapshotOneAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Is.Null);
            Assert.That(usage.WarmCount, Is.EqualTo(0), "the uninitialised tenant short-circuits before warming the index");
        });
    }

    [Test]
    public async Task SnapshotOneAsync_warms_the_index_before_reading()
    {
        var usage = new FakeTenantUsageIndex().With(Acme, View(Quotas(), Usage()));

        _ = await Create(usage, new FakeTenantOverageBilling()).SnapshotOneAsync(Acme);

        Assert.That(usage.WarmCount, Is.EqualTo(1));
    }

    // ---- SnapshotAllAsync ----------------------------------------------

    [Test]
    public async Task SnapshotAllAsync_projects_every_registered_tenant_joined_to_its_overage()
    {
        var usage = new FakeTenantUsageIndex()
            .With(Acme, View(Quotas(bytes: 1000), Usage(bytes: 500)))
            .With(Beta, View(Quotas(keys: 50), Usage(keys: 5)));
        var billing = new FakeTenantOverageBilling().With(Acme, Overage(bytes: 3));

        var snapshots = await Create(usage, billing).SnapshotAllAsync();

        var byTenant = snapshots.ToDictionary(s => s.Tenant, s => s);
        Assert.Multiple(() =>
        {
            Assert.That(byTenant, Has.Count.EqualTo(2));
            Assert.That(byTenant[Acme].MeteredOverage, Is.EqualTo(Overage(bytes: 3)), "the metered tenant carries its overage");
            Assert.That(byTenant[Beta].MeteredOverage, Is.EqualTo(TenantOverageSample.Empty), "the unmetered tenant folds in empty overage");
            Assert.That(byTenant[Beta].Usage, Is.EqualTo(Usage(keys: 5)));
        });
    }

    [Test]
    public async Task SnapshotAllAsync_over_no_registered_tenants_is_empty()
    {
        var snapshots = await Create(new FakeTenantUsageIndex(), new FakeTenantOverageBilling()).SnapshotAllAsync();

        Assert.That(snapshots, Is.Empty);
    }
}
