using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilitySnapshot"/>: the shared per-tenant
/// projection. Covers the constructor round-trip and the derived
/// <see cref="TenantObservabilitySnapshot.InstantaneousOverage"/> signal (the live
/// usage-above-quota fold, distinct from the durable metered overage). Pure value
/// construction, so there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class TenantObservabilitySnapshotTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Constructor_round_trips_every_component()
    {
        var usage = Usage(bytes: 100, keys: 10, memoryBytes: 50, treeCount: 2);
        var quotas = Quotas(bytes: 1000, keys: 100, burstPercent: 20);
        var overage = Overage(bytes: 5, keys: 1);

        var snapshot = new TenantObservabilitySnapshot(Acme, usage, quotas, overage);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Tenant, Is.EqualTo(Acme));
            Assert.That(snapshot.Usage, Is.EqualTo(usage));
            Assert.That(snapshot.Quotas, Is.EqualTo(quotas));
            Assert.That(snapshot.MeteredOverage, Is.EqualTo(overage));
        });
    }

    [Test]
    public void InstantaneousOverage_is_usage_above_the_steady_state_ceilings()
    {
        var usage = Usage(bytes: 1500, keys: 40);
        var quotas = Quotas(bytes: 1000, keys: 100);
        var snapshot = new TenantObservabilitySnapshot(Acme, usage, quotas, TenantOverageSample.Empty);

        Assert.That(
            snapshot.InstantaneousOverage,
            Is.EqualTo(Overage(bytes: 500)),
            "only the bytes dimension is over its cap; keys are under and unbounded dims contribute zero");
    }

    [Test]
    public void InstantaneousOverage_is_empty_when_usage_is_within_quota()
    {
        var snapshot = new TenantObservabilitySnapshot(
            Acme, Usage(bytes: 10), Quotas(bytes: 1000), TenantOverageSample.Empty);

        Assert.That(snapshot.InstantaneousOverage, Is.EqualTo(TenantOverageSample.Empty));
    }

    [Test]
    public void InstantaneousOverage_is_distinct_from_the_durable_metered_overage()
    {
        var snapshot = new TenantObservabilitySnapshot(
            Acme, Usage(bytes: 1500), Quotas(bytes: 1000), Overage(bytes: 999));

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.MeteredOverage, Is.EqualTo(Overage(bytes: 999)), "the durable accrued billing total");
            Assert.That(snapshot.InstantaneousOverage, Is.EqualTo(Overage(bytes: 500)), "the live over-cap amount, derived independently");
        });
    }
}
