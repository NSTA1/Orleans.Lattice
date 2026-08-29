using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit coverage for the public wire model of the tenant usage-against-quota
/// surface: the <see cref="TenantQuotaDimensionUsage"/> value type (its nullable
/// ceiling and usage, the <see cref="TenantQuotaDimensionUsage.Unbounded"/>
/// sentinel, and the <see cref="TenantQuotaDimensionUsage.IsBounded"/> /
/// <see cref="TenantQuotaDimensionUsage.IsMeasured"/> predicates that keep
/// "no ceiling" distinct from "a ceiling of zero"), the
/// <see cref="TenantQuotaUsageReport"/> record a transport binding exchanges, and
/// the <see cref="TenantQuotaEnforcementScope"/> qualifier. These are pure value
/// types with no timing or ordering behaviour.
/// </summary>
[TestFixture]
public sealed class TenantQuotaUsageModelTests
{
    [Test]
    public void Dimension_round_trips_every_member()
    {
        var dimension = new TenantQuotaDimensionUsage
        {
            Usage = 4_100,
            Limit = 10_000,
            BurstLimit = 12_000,
            Overage = 7,
            MeteredOverage = 11,
        };

        Assert.Multiple(() =>
        {
            Assert.That(dimension.Usage, Is.EqualTo(4_100));
            Assert.That(dimension.Limit, Is.EqualTo(10_000));
            Assert.That(dimension.BurstLimit, Is.EqualTo(12_000));
            Assert.That(dimension.Overage, Is.EqualTo(7));
            Assert.That(dimension.MeteredOverage, Is.EqualTo(11));
            Assert.That(dimension.IsBounded, Is.True);
            Assert.That(dimension.IsMeasured, Is.True);
        });
    }

    [Test]
    public void Unbounded_sentinel_has_no_ceiling_and_no_usage()
    {
        var unbounded = TenantQuotaDimensionUsage.Unbounded;

        Assert.Multiple(() =>
        {
            Assert.That(unbounded.Limit, Is.Null);
            Assert.That(unbounded.BurstLimit, Is.Null);
            Assert.That(unbounded.Usage, Is.Null);
            Assert.That(unbounded.Overage, Is.Zero);
            Assert.That(unbounded.MeteredOverage, Is.Zero);
            Assert.That(unbounded.IsBounded, Is.False);
            Assert.That(unbounded.IsMeasured, Is.False);
        });
    }

    [Test]
    public void Default_dimension_is_unbounded_and_unmeasured() =>
        Assert.That(default(TenantQuotaDimensionUsage), Is.EqualTo(TenantQuotaDimensionUsage.Unbounded));

    /// <summary>
    /// The distinction the whole contract exists to preserve: an absent ceiling and
    /// a ceiling of zero are different facts, and conflating them would render a
    /// full bar for a tenant that has no limit at all.
    /// </summary>
    [Test]
    public void A_zero_ceiling_is_bounded_and_distinct_from_unbounded()
    {
        var cappedAtZero = TenantQuotaDimensionUsage.Unbounded with { Limit = 0, BurstLimit = 0 };

        Assert.Multiple(() =>
        {
            Assert.That(cappedAtZero.IsBounded, Is.True);
            Assert.That(TenantQuotaDimensionUsage.Unbounded.IsBounded, Is.False);
            Assert.That(cappedAtZero, Is.Not.EqualTo(TenantQuotaDimensionUsage.Unbounded));
        });
    }

    /// <summary>
    /// The sibling distinction: a measured reading of zero is a fact, an unmeasured
    /// dimension is the absence of one.
    /// </summary>
    [Test]
    public void A_zero_usage_is_measured_and_distinct_from_unmeasured()
    {
        var measuredZero = TenantQuotaDimensionUsage.Unbounded with { Usage = 0 };

        Assert.Multiple(() =>
        {
            Assert.That(measuredZero.IsMeasured, Is.True);
            Assert.That(TenantQuotaDimensionUsage.Unbounded.IsMeasured, Is.False);
            Assert.That(measuredZero, Is.Not.EqualTo(TenantQuotaDimensionUsage.Unbounded));
        });
    }

    [Test]
    public void Report_round_trips_every_dimension_and_qualifier()
    {
        var quotas = new TenantQuotasDescriptor { MaxBytes = 10_000, BurstPercent = 20 };
        var report = new TenantQuotaUsageReport
        {
            TenantId = "acme",
            IsDefault = false,
            EnforcementScope = TenantQuotaEnforcementScope.PerCluster,
            HasUsage = true,
            Bytes = new TenantQuotaDimensionUsage { Usage = 1, Limit = 10 },
            Keys = new TenantQuotaDimensionUsage { Usage = 2, Limit = 20 },
            MemoryBytes = new TenantQuotaDimensionUsage { Usage = 3, Limit = 30 },
            TreeCount = new TenantQuotaDimensionUsage { Usage = 4, Limit = 40 },
            OpsPerSecond = new TenantQuotaDimensionUsage { Limit = 50 },
            BurstPercent = 20,
            Quotas = quotas,
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.IsDefault, Is.False);
            Assert.That(report.EnforcementScope, Is.EqualTo(TenantQuotaEnforcementScope.PerCluster));
            Assert.That(report.HasUsage, Is.True);
            Assert.That(report.Bytes.Usage, Is.EqualTo(1));
            Assert.That(report.Keys.Usage, Is.EqualTo(2));
            Assert.That(report.MemoryBytes.Usage, Is.EqualTo(3));
            Assert.That(report.TreeCount.Usage, Is.EqualTo(4));
            Assert.That(report.OpsPerSecond.Limit, Is.EqualTo(50));
            Assert.That(report.BurstPercent, Is.EqualTo(20));
            Assert.That(report.Quotas, Is.EqualTo(quotas));
        });
    }

    [Test]
    public void Enforcement_scope_defaults_to_the_global_converged_fold() =>
        Assert.That(default(TenantQuotaEnforcementScope), Is.EqualTo(TenantQuotaEnforcementScope.GlobalConverged));

    [Test]
    public void Enforcement_scope_names_both_aggregates() =>
        Assert.That(
            Enum.GetValues<TenantQuotaEnforcementScope>(),
            Is.EquivalentTo(new[]
            {
                TenantQuotaEnforcementScope.GlobalConverged,
                TenantQuotaEnforcementScope.PerCluster,
            }));
}
