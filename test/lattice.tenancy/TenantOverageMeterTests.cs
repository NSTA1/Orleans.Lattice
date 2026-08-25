using Microsoft.Extensions.Options;
using Orleans.Configuration;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantOverageMeter"/>: the cadence-driven accrual of
/// observed overage into this cluster's grow-only counter component. Covers the
/// steady-state-cap projection, the within-quota no-op, the Riemann-sum
/// accumulation across ticks, the cluster-id replica key, and the tenant guard.
/// Every assertion is on the final converged state - no timing or wall-clock.
/// </summary>
[TestFixture]
public sealed class TenantOverageMeterTests
{
    private const string LocalCluster = "local";
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static (TenantOverageMeter Meter, OverageTestData.FakeTenantOverageStore Store) Create()
    {
        var store = new OverageTestData.FakeTenantOverageStore();
        var meter = new TenantOverageMeter(store, Options.Create(new ClusterOptions { ClusterId = LocalCluster }));
        return (meter, store);
    }

    [Test]
    public void ClusterId_is_the_configured_cluster()
    {
        var (meter, _) = Create();

        Assert.That(meter.ClusterId, Is.EqualTo(LocalCluster));
    }

    [Test]
    public void AccrueAsync_with_the_no_tenant_value_throws()
    {
        var (meter, _) = Create();

        Assert.That(
            async () => await meter.AccrueAsync(default, Usage(1_000), Quotas(bytes: 100)),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AccrueAsync_within_quota_is_a_no_op()
    {
        var (meter, store) = Create();

        var metered = await meter.AccrueAsync(Acme, Usage(bytes: 50), Quotas(bytes: 100));

        Assert.Multiple(() =>
        {
            Assert.That(metered, Is.EqualTo(TenantOverageSample.Empty));
            Assert.That(store.Metered, Is.Empty, "a within-quota tick meters nothing");
            Assert.That(store.Records, Is.Empty);
        });
    }

    [Test]
    public async Task AccrueAsync_unbounded_quota_is_a_no_op()
    {
        var (meter, store) = Create();

        var metered = await meter.AccrueAsync(Acme, Usage(1_000, 100, 10_000, 50), TenantQuotas.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That(metered, Is.EqualTo(TenantOverageSample.Empty));
            Assert.That(store.Metered, Is.Empty);
        });
    }

    [Test]
    public async Task AccrueAsync_over_quota_meters_the_excess_into_this_clusters_component()
    {
        var (meter, store) = Create();

        var metered = await meter.AccrueAsync(Acme, Usage(150, 5, 30, 4), Quotas(bytes: 100, keys: 1, memoryBytes: 10, treeCount: 1));

        Assert.Multiple(() =>
        {
            Assert.That(metered, Is.EqualTo(Overage(50, 4, 20, 3)));
            Assert.That(store.Records, Has.Count.EqualTo(1));
            Assert.That(store.Records[0].LocalOverage(LocalCluster), Is.EqualTo(Overage(50, 4, 20, 3)));
        });
    }

    [Test]
    public async Task AccrueAsync_is_a_riemann_sum_across_ticks()
    {
        var (meter, store) = Create();
        var quotas = Quotas(bytes: 100);

        // Three cadence ticks each observe sustained overage above the cap; the meter
        // integrates them into a growing running total. The result is independent of
        // any timing - only the sequence of observations matters.
        await meter.AccrueAsync(Acme, Usage(bytes: 120), quotas); // +20
        await meter.AccrueAsync(Acme, Usage(bytes: 130), quotas); // +30
        await meter.AccrueAsync(Acme, Usage(bytes: 110), quotas); // +10

        Assert.That(store.Records[0].Fold().Bytes, Is.EqualTo(60), "the meter accumulates each tick's overage");
    }

    [Test]
    public async Task AccrueAsync_meters_from_the_base_cap_ignoring_burst()
    {
        var (meter, store) = Create();

        await meter.AccrueAsync(Acme, Usage(bytes: 120), Quotas(bytes: 100, burstPercent: 50));

        Assert.That(store.Records[0].Fold().Bytes, Is.EqualTo(20), "the full excess above the base cap is metered, not the excess above the burst ceiling");
    }
}
