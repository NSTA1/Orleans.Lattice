using System.Diagnostics.Metrics;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilityGaugeSnapshot"/>: the pre-built
/// per-instrument measurement arrays the observable gauges return by reference.
/// Covers the empty snapshot, the per-tenant tag stamping and value projection, that
/// a quota measurement is emitted only for a bounded dimension, and the null guard.
/// Pure value construction, so there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class TenantObservabilityGaugeSnapshotTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantObservabilitySnapshot Snapshot(TenantId tenant, LocalUsageSample usage, TenantQuotas quotas, TenantOverageSample overage) =>
        new(tenant, usage, quotas, overage);

    private static long Single(Measurement<long>[] measurements)
    {
        Assert.That(measurements, Has.Length.EqualTo(1));
        return measurements[0].Value;
    }

    private static string? TenantTag(Measurement<long> measurement)
    {
        foreach (var tag in measurement.Tags)
        {
            if (tag.Key == LatticeTenantMetrics.TagTenant)
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    [Test]
    public void Build_null_tenants_throws()
    {
        Assert.That(() => TenantObservabilityGaugeSnapshot.Build(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Build_empty_list_is_the_empty_snapshot()
    {
        var snapshot = TenantObservabilityGaugeSnapshot.Build(Array.Empty<TenantObservabilitySnapshot>());

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Is.SameAs(TenantObservabilityGaugeSnapshot.Empty));
            Assert.That(snapshot.TenantCount, Is.Zero);
            Assert.That(snapshot.UsageBytes, Is.Empty);
        });
    }

    [Test]
    public void Empty_has_no_series()
    {
        var empty = TenantObservabilityGaugeSnapshot.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(empty.TenantCount, Is.Zero);
            Assert.That(empty.UsageBytes, Is.Empty);
            Assert.That(empty.QuotaBytes, Is.Empty);
            Assert.That(empty.OverageBytes, Is.Empty);
            Assert.That(empty.QuotaBurstPercent, Is.Empty);
        });
    }

    [Test]
    public void Build_projects_usage_burst_and_overage_dimensions_tagged_by_tenant()
    {
        var snapshot = TenantObservabilityGaugeSnapshot.Build(new[]
        {
            Snapshot(
                Acme,
                Usage(bytes: 100, keys: 10, memoryBytes: 50, treeCount: 2),
                Quotas(bytes: 1000, keys: 100, memoryBytes: 500, treeCount: 5, burstPercent: 20),
                Overage(bytes: 7, keys: 1, memoryBytes: 3, treeCount: 0)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TenantCount, Is.EqualTo(1));

            Assert.That(Single(snapshot.UsageBytes), Is.EqualTo(100));
            Assert.That(Single(snapshot.UsageKeys), Is.EqualTo(10));
            Assert.That(Single(snapshot.UsageMemoryBytes), Is.EqualTo(50));
            Assert.That(Single(snapshot.UsageTrees), Is.EqualTo(2));

            Assert.That(Single(snapshot.QuotaBytes), Is.EqualTo(1000));
            Assert.That(Single(snapshot.QuotaKeys), Is.EqualTo(100));
            Assert.That(Single(snapshot.QuotaMemoryBytes), Is.EqualTo(500));
            Assert.That(Single(snapshot.QuotaTrees), Is.EqualTo(5));
            Assert.That(Single(snapshot.QuotaBurstPercent), Is.EqualTo(20));

            Assert.That(Single(snapshot.OverageBytes), Is.EqualTo(7));
            Assert.That(Single(snapshot.OverageKeys), Is.EqualTo(1));
            Assert.That(Single(snapshot.OverageMemoryBytes), Is.EqualTo(3));
            Assert.That(Single(snapshot.OverageTrees), Is.EqualTo(0));

            Assert.That(TenantTag(snapshot.UsageBytes[0]), Is.EqualTo("acme"), "every series carries the tenant tag");
            Assert.That(TenantTag(snapshot.OverageBytes[0]), Is.EqualTo("acme"));
        });
    }

    [Test]
    public void Build_emits_a_quota_series_only_for_a_bounded_dimension()
    {
        var snapshot = TenantObservabilityGaugeSnapshot.Build(new[]
        {
            Snapshot(Acme, Usage(bytes: 10), Quotas(bytes: 1000, burstPercent: 5), TenantOverageSample.Empty),
        });

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.QuotaBytes, Has.Length.EqualTo(1), "the bounded byte quota contributes a series");
            Assert.That(snapshot.QuotaKeys, Is.Empty, "the unbounded key quota contributes no series");
            Assert.That(snapshot.QuotaMemoryBytes, Is.Empty);
            Assert.That(snapshot.QuotaTrees, Is.Empty);
            Assert.That(snapshot.QuotaBurstPercent, Has.Length.EqualTo(1), "burst percent is always emitted");
        });
    }

    [Test]
    public void Build_shares_one_tenant_tag_array_across_a_tenants_dimensions()
    {
        var snapshot = TenantObservabilityGaugeSnapshot.Build(new[]
        {
            Snapshot(Acme, Usage(bytes: 1), Quotas(bytes: 2), Overage(bytes: 3)),
        });

        Assert.That(
            TenantTag(snapshot.UsageBytes[0]),
            Is.EqualTo(TenantTag(snapshot.OverageBytes[0])),
            "the same tenant tag is stamped on every one of that tenant's series");
    }
}
