using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantOverageBilling"/>: the public billing read
/// seam. Covers that a single-tenant read folds the grow-only counters cross-cluster,
/// an absent tenant reads as empty overage, and the list projection streams every
/// metered tenant's converged overage. Driven against the in-memory store fake, so
/// there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class LatticeTenantOverageBillingTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static (LatticeTenantOverageBilling Billing, OverageTestData.FakeTenantOverageStore Store) Create()
    {
        var store = new OverageTestData.FakeTenantOverageStore();
        return (new LatticeTenantOverageBilling(store), store);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(() => new LatticeTenantOverageBilling(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetMeteredOverageAsync_folds_the_counters_across_clusters()
    {
        var (billing, store) = Create();
        store.Records.Add(OverageRecord(
            "acme",
            ("east", Overage(100, 1, 10, 1)),
            ("west", Overage(200, 2, 20, 2))));

        var overage = await billing.GetMeteredOverageAsync(Acme);

        Assert.That(overage, Is.EqualTo(Overage(300, 3, 30, 3)), "the billing read sums every cluster's metered overage");
    }

    [Test]
    public async Task GetMeteredOverageAsync_for_an_absent_tenant_is_empty()
    {
        var (billing, _) = Create();

        var overage = await billing.GetMeteredOverageAsync(Acme);

        Assert.That(overage, Is.EqualTo(TenantOverageSample.Empty), "a tenant that never breached reads as empty overage");
    }

    [Test]
    public async Task ListMeteredOverageAsync_streams_every_tenants_converged_overage()
    {
        var (billing, store) = Create();
        store.Records.Add(OverageRecord("acme", ("east", Overage(100, 1, 10, 1)), ("west", Overage(200, 2, 20, 2))));
        store.Records.Add(OverageRecord("beta", ("east", Overage(5, 5, 5, 5))));

        var projected = new Dictionary<TenantId, TenantOverageSample>();
        await foreach (var entry in billing.ListMeteredOverageAsync())
        {
            projected[entry.Tenant] = entry.Overage;
        }

        Assert.Multiple(() =>
        {
            Assert.That(projected, Has.Count.EqualTo(2));
            Assert.That(projected[Acme], Is.EqualTo(Overage(300, 3, 30, 3)));
            Assert.That(projected[Beta], Is.EqualTo(Overage(5, 5, 5, 5)));
        });
    }

    [Test]
    public async Task ListMeteredOverageAsync_over_an_empty_store_yields_nothing()
    {
        var (billing, _) = Create();

        var count = 0;
        await foreach (var _ in billing.ListMeteredOverageAsync())
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(0));
    }
}
