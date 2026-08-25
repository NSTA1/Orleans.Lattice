using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantOverageSample"/>: the transient overage value
/// type. Covers the empty identity, emptiness predicate, the commutative /
/// associative <see cref="TenantOverageSample.Add"/> monoid, and the
/// steady-state-cap projection <see cref="TenantOverageSample.Above"/> across
/// bounded, unbounded, at-cap, over-cap, and burst cases.
/// </summary>
[TestFixture]
public sealed class TenantOverageSampleTests
{
    [Test]
    public void Empty_is_zero_on_every_dimension()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantOverageSample.Empty.Bytes, Is.EqualTo(0));
            Assert.That(TenantOverageSample.Empty.Keys, Is.EqualTo(0));
            Assert.That(TenantOverageSample.Empty.MemoryBytes, Is.EqualTo(0));
            Assert.That(TenantOverageSample.Empty.TreeCount, Is.EqualTo(0));
            Assert.That(TenantOverageSample.Empty.IsEmpty, Is.True);
        });
    }

    [Test]
    public void IsEmpty_is_false_when_any_dimension_is_nonzero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Overage(bytes: 1).IsEmpty, Is.False);
            Assert.That(Overage(keys: 1).IsEmpty, Is.False);
            Assert.That(Overage(memoryBytes: 1).IsEmpty, Is.False);
            Assert.That(Overage(treeCount: 1).IsEmpty, Is.False);
        });
    }

    [Test]
    public void Add_sums_each_dimension()
    {
        var sum = Overage(100, 1, 10, 1).Add(Overage(200, 2, 20, 2));

        Assert.That(sum, Is.EqualTo(Overage(300, 3, 30, 3)));
    }

    [Test]
    public void Add_has_empty_as_its_identity()
    {
        var sample = Overage(100, 1, 10, 1);

        Assert.Multiple(() =>
        {
            Assert.That(sample.Add(TenantOverageSample.Empty), Is.EqualTo(sample));
            Assert.That(TenantOverageSample.Empty.Add(sample), Is.EqualTo(sample));
        });
    }

    [Test]
    public void Add_is_commutative_and_associative()
    {
        var a = Overage(100, 1, 10, 1);
        var b = Overage(200, 2, 20, 2);
        var c = Overage(300, 3, 30, 3);

        Assert.Multiple(() =>
        {
            Assert.That(a.Add(b), Is.EqualTo(b.Add(a)), "commutative");
            Assert.That(a.Add(b).Add(c), Is.EqualTo(a.Add(b.Add(c))), "associative");
        });
    }

    [Test]
    public void Above_an_unbounded_quota_is_empty()
    {
        var overage = TenantOverageSample.Above(Usage(1_000, 100, 10_000, 50), TenantQuotas.Unbounded);

        Assert.That(overage, Is.EqualTo(TenantOverageSample.Empty));
    }

    [Test]
    public void Above_within_the_cap_is_empty()
    {
        var overage = TenantOverageSample.Above(Usage(100, 1, 10, 1), Quotas(bytes: 100, keys: 1, memoryBytes: 10, treeCount: 1));

        Assert.That(overage, Is.EqualTo(TenantOverageSample.Empty), "usage exactly at the cap is not overage");
    }

    [Test]
    public void Above_the_cap_is_the_per_dimension_excess()
    {
        var overage = TenantOverageSample.Above(
            Usage(150, 5, 30, 4),
            Quotas(bytes: 100, keys: 1, memoryBytes: 10, treeCount: 1));

        Assert.That(overage, Is.EqualTo(Overage(50, 4, 20, 3)));
    }

    [Test]
    public void Above_meters_from_the_steady_state_cap_ignoring_burst()
    {
        // Burst is admission tolerance; overage is metered from the base cap, so the
        // burst band is itself billable overage.
        var overage = TenantOverageSample.Above(
            Usage(bytes: 120),
            Quotas(bytes: 100, burstPercent: 50));

        Assert.That(overage.Bytes, Is.EqualTo(20), "the full excess above the base cap is metered, not the excess above the burst ceiling");
    }

    [Test]
    public void Above_mixes_bounded_and_unbounded_dimensions()
    {
        var overage = TenantOverageSample.Above(
            Usage(150, 5, 30, 4),
            Quotas(bytes: 100, keys: null, memoryBytes: 10, treeCount: null));

        Assert.That(overage, Is.EqualTo(Overage(bytes: 50, keys: 0, memoryBytes: 20, treeCount: 0)));
    }
}
