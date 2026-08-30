using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantQuotaEvaluator"/>: the pure admit/refuse
/// decision. Covers the unbounded fast path, admission under quota, refusal over
/// quota on each dimension (with the tenant id and dimension surfaced on the
/// exception), the deterministic first-breached-dimension order, and the burst
/// headroom.
/// </summary>
[TestFixture]
public sealed class TenantQuotaEvaluatorTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private const string Tree = "orders";

    [Test]
    public void Unbounded_quota_always_admits()
    {
        Assert.That(
            () => TenantQuotaEvaluator.Admit(Acme, TenantQuotas.Unbounded, Sample(long.MaxValue, long.MaxValue, long.MaxValue, long.MaxValue), Tree),
            Throws.Nothing);
    }

    [Test]
    public void Usage_at_the_ceiling_admits()
    {
        var quotas = new TenantQuotas { MaxBytes = 1_000 };

        Assert.That(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 1_000), Tree),
            Throws.Nothing);
    }

    [Test]
    public void Usage_over_the_bytes_ceiling_refuses_with_the_tenant_and_dimension()
    {
        var quotas = new TenantQuotas { MaxBytes = 1_000 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 1_001), Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Dimension, Is.EqualTo(LatticeQuotaExceededException.BytesDimension));
            Assert.That(ex.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Current, Is.EqualTo(1_001));
            Assert.That(ex.Limit, Is.EqualTo(1_000));
        });
    }

    [Test]
    public void Usage_over_the_keys_ceiling_refuses_on_the_keys_dimension()
    {
        var quotas = new TenantQuotas { MaxKeys = 10 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(keys: 11), Tree));

        Assert.That(ex!.Dimension, Is.EqualTo(LatticeQuotaExceededException.KeysDimension));
    }

    [Test]
    public void Usage_over_the_memory_ceiling_refuses_on_the_memory_dimension()
    {
        var quotas = new TenantQuotas { MaxMemoryBytes = 100 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(memoryBytes: 101), Tree));

        Assert.That(ex!.Dimension, Is.EqualTo(TenantQuotaEvaluator.MemoryDimension));
    }

    [Test]
    public void Usage_over_the_tree_count_ceiling_refuses_on_the_trees_dimension()
    {
        var quotas = new TenantQuotas { MaxTreeCount = 3 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(treeCount: 4), Tree));

        Assert.That(ex!.Dimension, Is.EqualTo(TenantQuotaEvaluator.TreeCountDimension));
    }

    [Test]
    public void The_first_breached_dimension_is_reported_in_stable_order()
    {
        // Both bytes and keys breach; bytes is checked first, so bytes is reported.
        var quotas = new TenantQuotas { MaxBytes = 1_000, MaxKeys = 10 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 5_000, keys: 500), Tree));

        Assert.That(ex!.Dimension, Is.EqualTo(LatticeQuotaExceededException.BytesDimension));
    }

    [Test]
    public void Burst_headroom_admits_a_transient_overage_within_the_burst()
    {
        // 10% burst over a 1,000 ceiling permits up to 1,100.
        var quotas = new TenantQuotas { MaxBytes = 1_000, BurstPercent = 10 };

        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 1_100), Tree),
                Throws.Nothing,
                "usage within the burst-adjusted ceiling admits");
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 1_101), Tree),
                Throws.TypeOf<LatticeQuotaExceededException>(),
                "usage past the burst-adjusted ceiling refuses");
        });
    }

    [Test]
    public void The_refusal_reports_the_base_ceiling_not_the_burst_adjusted_one()
    {
        var quotas = new TenantQuotas { MaxBytes = 1_000, BurstPercent = 10 };

        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(bytes: 2_000), Tree));

        Assert.That(ex!.Limit, Is.EqualTo(1_000), "the surfaced limit is the operator-declared base ceiling");
    }

    [Test]
    public void Burst_headroom_admits_over_a_small_ceiling_that_is_not_a_multiple_of_100()
    {
        // Regression: the burst allowance was computed as ceiling / 100 * burstPercent,
        // which divides before multiplying. For any ceiling below 100 that floors the
        // burst to zero (10 / 100 == 0), so a 50% burst over a ceiling of 10 wrongly
        // resolved to 10 instead of 15 and refused writes the burst should admit.
        var quotas = new TenantQuotas { MaxTreeCount = 10, BurstPercent = 50 };

        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(treeCount: 15), Tree),
                Throws.Nothing,
                "10 + 50% burst permits up to 15 owned trees");
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(treeCount: 16), Tree),
                Throws.TypeOf<LatticeQuotaExceededException>(),
                "usage past the burst-adjusted ceiling still refuses");
        });
    }

    [Test]
    public void Burst_headroom_rounds_a_fractional_allowance_down_without_losing_it()
    {
        // A 15% burst over a ceiling of 10 is 1.5 trees, which floors to 1 (effective
        // ceiling 11) - not to zero, and not up to 2. The pre-fix divide-first math
        // produced 10 / 100 * 15 == 0, losing the allowance entirely.
        var quotas = new TenantQuotas { MaxKeys = 10, BurstPercent = 15 };

        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(keys: 11), Tree),
                Throws.Nothing,
                "10 + floor(15%) burst permits up to 11 keys");
            Assert.That(
                () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(keys: 12), Tree),
                Throws.TypeOf<LatticeQuotaExceededException>(),
                "usage past the floored burst-adjusted ceiling refuses");
        });
    }

    [Test]
    public void A_null_dimension_ceiling_is_never_breached()
    {
        // Only MaxOpsPerSecond (the rate dimension, not this evaluator's concern) is
        // set; every capacity dimension is unbounded, so any usage admits.
        var quotas = new TenantQuotas { MaxOpsPerSecond = 5 };

        Assert.That(
            () => TenantQuotaEvaluator.Admit(Acme, quotas, Sample(long.MaxValue, long.MaxValue, long.MaxValue, long.MaxValue), Tree),
            Throws.Nothing);
    }
}
