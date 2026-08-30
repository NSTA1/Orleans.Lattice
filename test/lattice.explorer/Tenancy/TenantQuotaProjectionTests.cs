using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the wire-to-domain projection and the quota model's own predicates.
/// The two distinctions a naive model would flatten - unbounded is not a ceiling
/// of zero, and unmeasured is not a measured zero - are asserted directly, and
/// every enum translation is checked to fail closed on a value this Explorer
/// does not know.
/// </summary>
[TestFixture]
public class TenantQuotaProjectionTests
{
    [Test]
    public void An_unbounded_ceiling_survives_the_projection_as_absent_not_zero()
    {
        var dimension = TenantProjection.ToDimension(
            new TenantQuotaDimensionUsage { Usage = 3, Limit = null, BurstLimit = null });

        Assert.Multiple(() =>
        {
            Assert.That(dimension.Limit, Is.Null);
            Assert.That(dimension.BurstLimit, Is.Null);
            Assert.That(dimension.IsBounded, Is.False);
            Assert.That(dimension.IsMeasured, Is.True);
            Assert.That(dimension.Utilization, Is.Null, "an unbounded dimension has no fraction to render");
            Assert.That(dimension.IsOverLimit, Is.False, "no ceiling means no breach");
        });
    }

    [Test]
    public void A_ceiling_of_zero_is_a_real_cap_and_stays_distinct_from_unbounded()
    {
        var dimension = TenantProjection.ToDimension(
            new TenantQuotaDimensionUsage { Usage = 64, Limit = 0, Overage = 64 });

        Assert.Multiple(() =>
        {
            Assert.That(dimension.Limit, Is.EqualTo(0));
            Assert.That(dimension.IsBounded, Is.True, "a cap of nothing is still a cap");
            Assert.That(dimension.IsOverLimit, Is.True);
            Assert.That(dimension.Utilization, Is.EqualTo(1d), "any usage fully consumes a ceiling of zero");
            Assert.That(dimension.Overage, Is.EqualTo(64));
        });
    }

    [Test]
    public void A_ceiling_of_zero_with_no_usage_is_not_over_limit()
    {
        var dimension = TenantProjection.ToDimension(
            new TenantQuotaDimensionUsage { Usage = 0, Limit = 0 });

        Assert.Multiple(() =>
        {
            Assert.That(dimension.IsOverLimit, Is.False);
            Assert.That(dimension.Utilization, Is.EqualTo(0d));
        });
    }

    [Test]
    public void An_unmeasured_dimension_survives_the_projection_as_absent_not_zero()
    {
        // The operation-rate dimension reports a ceiling with no usage sample:
        // rendering it as a measured zero would read as "you are using none of
        // your rate limit" when the truth is "we are not measuring this".
        var dimension = TenantProjection.ToDimension(
            new TenantQuotaDimensionUsage { Usage = null, Limit = 900, BurstLimit = 990 });

        Assert.Multiple(() =>
        {
            Assert.That(dimension.Usage, Is.Null);
            Assert.That(dimension.IsMeasured, Is.False);
            Assert.That(dimension.IsBounded, Is.True);
            Assert.That(dimension.Limit, Is.EqualTo(900));
            Assert.That(dimension.Utilization, Is.Null, "an unmeasured dimension has no fraction to render");
            Assert.That(dimension.IsOverLimit, Is.False, "no sample means no breach can be established");
        });
    }

    [Test]
    public void A_measured_zero_is_distinct_from_an_unmeasured_dimension()
    {
        var measured = TenantProjection.ToDimension(new TenantQuotaDimensionUsage { Usage = 0, Limit = 500 });
        var unmeasured = TenantProjection.ToDimension(new TenantQuotaDimensionUsage { Usage = null, Limit = 500 });

        Assert.Multiple(() =>
        {
            Assert.That(measured.IsMeasured, Is.True);
            Assert.That(measured.Utilization, Is.EqualTo(0d));
            Assert.That(unmeasured.IsMeasured, Is.False);
            Assert.That(unmeasured.Utilization, Is.Null);
            Assert.That(measured, Is.Not.EqualTo(unmeasured));
        });
    }

    [Test]
    public void Utilization_is_not_clamped_so_a_breach_renders_as_a_breach()
    {
        var dimension = TenantProjection.ToDimension(
            new TenantQuotaDimensionUsage { Usage = 1_500, Limit = 1_000, Overage = 500 });

        Assert.Multiple(() =>
        {
            Assert.That(dimension.Utilization, Is.EqualTo(1.5d));
            Assert.That(dimension.IsOverLimit, Is.True);
        });
    }

    [Test]
    public void The_default_dimension_asserts_neither_a_ceiling_nor_a_sample() =>
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerTenantQuotaDimension.Unbounded.IsBounded, Is.False);
            Assert.That(ExplorerTenantQuotaDimension.Unbounded.IsMeasured, Is.False);
            Assert.That(default(ExplorerTenantQuotaDimension), Is.EqualTo(ExplorerTenantQuotaDimension.Unbounded));
        });

    [Test]
    public void Quota_ceilings_project_each_dimension_and_keep_unbounded_absent()
    {
        var limits = TenantProjection.ToLimits(SampleTenant.Quotas());

        Assert.Multiple(() =>
        {
            Assert.That(limits.MaxBytes, Is.EqualTo(1_000));
            Assert.That(limits.MaxKeys, Is.EqualTo(500));
            Assert.That(limits.MaxMemoryBytes, Is.EqualTo(0));
            Assert.That(limits.MaxTreeCount, Is.Null);
            Assert.That(limits.MaxOpsPerSecond, Is.Null);
            Assert.That(limits.BurstPercent, Is.EqualTo(10));
            Assert.That(limits.IsUnbounded, Is.False);
            Assert.That(ExplorerTenantQuotaLimits.Unbounded.IsUnbounded, Is.True);
        });
    }

    [Test]
    public void Quota_ceilings_round_trip_back_onto_the_wire_without_inventing_a_zero()
    {
        var limits = TenantProjection.ToLimits(SampleTenant.Quotas());

        var descriptor = TenantProjection.ToDescriptor(limits);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.MaxTreeCount, Is.Null);
            Assert.That(descriptor.MaxOpsPerSecond, Is.Null);
            Assert.That(descriptor.MaxMemoryBytes, Is.EqualTo(0));
            Assert.That(descriptor, Is.EqualTo(SampleTenant.Quotas()));
        });
    }

    [Test]
    public void The_ceilings_indexer_answers_every_dimension()
    {
        var limits = TenantProjection.ToLimits(SampleTenant.Quotas());

        Assert.Multiple(() =>
        {
            Assert.That(limits[ExplorerTenantQuotaDimensionKind.Bytes], Is.EqualTo(1_000));
            Assert.That(limits[ExplorerTenantQuotaDimensionKind.Keys], Is.EqualTo(500));
            Assert.That(limits[ExplorerTenantQuotaDimensionKind.MemoryBytes], Is.EqualTo(0));
            Assert.That(limits[ExplorerTenantQuotaDimensionKind.TreeCount], Is.Null);
            Assert.That(limits[ExplorerTenantQuotaDimensionKind.OpsPerSecond], Is.Null);
            Assert.That(
                () => limits[(ExplorerTenantQuotaDimensionKind)99],
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_usage_indexer_answers_every_dimension_the_shared_list_names()
    {
        var usage = TenantProjection.ToUsage(SampleTenant.UsageReport());

        Assert.That(ExplorerTenantQuotaUsage.Dimensions, Has.Count.EqualTo(5));
        Assert.Multiple(() =>
        {
            Assert.That(usage[ExplorerTenantQuotaDimensionKind.Bytes], Is.EqualTo(usage.Bytes));
            Assert.That(usage[ExplorerTenantQuotaDimensionKind.Keys], Is.EqualTo(usage.Keys));
            Assert.That(usage[ExplorerTenantQuotaDimensionKind.MemoryBytes], Is.EqualTo(usage.MemoryBytes));
            Assert.That(usage[ExplorerTenantQuotaDimensionKind.TreeCount], Is.EqualTo(usage.TreeCount));
            Assert.That(usage[ExplorerTenantQuotaDimensionKind.OpsPerSecond], Is.EqualTo(usage.OpsPerSecond));
            Assert.That(
                () => usage[(ExplorerTenantQuotaDimensionKind)99],
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_shared_dimension_list_is_one_cached_instance_so_iterating_a_reading_allocates_nothing() =>
        Assert.That(
            ExplorerTenantQuotaUsage.Dimensions,
            Is.SameAs(ExplorerTenantQuotaUsage.Dimensions));

    [Test]
    public void A_usage_reading_carries_every_distinction_at_once()
    {
        var usage = TenantProjection.ToUsage(SampleTenant.UsageReport());

        Assert.Multiple(() =>
        {
            Assert.That(usage.Bytes.IsBounded, Is.True);
            Assert.That(usage.Bytes.IsMeasured, Is.True);
            Assert.That(usage.MemoryBytes.Limit, Is.EqualTo(0), "a ceiling of zero is preserved");
            Assert.That(usage.MemoryBytes.MeteredOverage, Is.EqualTo(128));
            Assert.That(usage.TreeCount.IsBounded, Is.False, "an unbounded dimension is preserved");
            Assert.That(usage.TreeCount.Usage, Is.EqualTo(3));
            Assert.That(usage.OpsPerSecond.IsMeasured, Is.False, "an unmeasured dimension is preserved");
            Assert.That(usage.OpsPerSecond.Limit, Is.EqualTo(900), "its authoritative ceiling still crosses");
        });
    }

    [Test]
    public void A_cold_reading_reports_no_usage_rather_than_fabricated_zeros()
    {
        var report = SampleTenant.UsageReport(hasUsage: false) with
        {
            Bytes = new TenantQuotaDimensionUsage { Usage = null, Limit = 1_000 },
            Keys = new TenantQuotaDimensionUsage { Usage = null, Limit = 500 },
        };

        var usage = TenantProjection.ToUsage(report);

        Assert.Multiple(() =>
        {
            Assert.That(usage.HasUsage, Is.False);
            Assert.That(usage.Bytes.IsMeasured, Is.False);
            Assert.That(usage.Bytes.Limit, Is.EqualTo(1_000), "the ceilings stay authoritative when usage is cold");
        });
    }

    [Test]
    public void The_enforcement_scope_crosses_so_a_per_cluster_reading_can_be_captioned()
    {
        var global = TenantProjection.ToUsage(
            SampleTenant.UsageReport(scope: TenantQuotaEnforcementScope.GlobalConverged));
        var local = TenantProjection.ToUsage(
            SampleTenant.UsageReport(scope: TenantQuotaEnforcementScope.PerCluster));

        Assert.Multiple(() =>
        {
            Assert.That(global.EnforcementScope, Is.EqualTo(ExplorerTenantQuotaEnforcement.GlobalConverged));
            Assert.That(local.EnforcementScope, Is.EqualTo(ExplorerTenantQuotaEnforcement.PerCluster));
        });
    }

    [Test]
    public void An_unknown_enforcement_scope_reports_the_weaker_per_cluster_claim()
    {
        var usage = TenantProjection.ToUsage(
            SampleTenant.UsageReport(scope: (TenantQuotaEnforcementScope)99));

        Assert.That(usage.EnforcementScope, Is.EqualTo(ExplorerTenantQuotaEnforcement.PerCluster));
    }

    [Test]
    public void Projection_helpers_reject_null_wire_payloads() =>
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantProjection.ToSummary(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToSummaries(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToDetail(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToCreation(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToStatusChange(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToDeletion(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToResidencyChange(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToAdmins(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToAdminChange(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToGrants(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToGrantChange(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToGrant(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToUsage(null!), Throws.ArgumentNullException);
            Assert.That(() => TenantProjection.ToRegions(null!), Throws.ArgumentNullException);
        });

    [Test]
    public void Empty_wire_collections_project_onto_the_shared_empty_arrays() =>
        Assert.Multiple(() =>
        {
            Assert.That(TenantProjection.ToSummaries([]), Is.Empty);
            Assert.That(TenantProjection.ToRegions([]), Is.Empty);
        });
}
