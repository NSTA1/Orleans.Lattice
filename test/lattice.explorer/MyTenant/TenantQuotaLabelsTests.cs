using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The quota display vocabulary, including the caption that qualifies a whole
/// reading with the scope it was enforced under.
/// </summary>
[TestFixture]
public sealed class TenantQuotaLabelsTests
{
    [Test]
    public void Every_dimension_has_a_label_and_a_unit()
    {
        Assert.Multiple(() =>
        {
            foreach (var kind in ExplorerTenantQuotaUsage.Dimensions)
            {
                Assert.That(TenantQuotaLabels.Label(kind), Is.Not.Empty, kind.ToString());
                Assert.That(TenantQuotaLabels.Unit(kind), Is.Not.Empty, kind.ToString());
            }
        });
    }

    [Test]
    public void An_undeclared_dimension_is_rejected_rather_than_labelled()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantQuotaLabels.Label((ExplorerTenantQuotaDimensionKind)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => TenantQuotaLabels.Unit((ExplorerTenantQuotaDimensionKind)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void A_per_cluster_reading_is_captioned_as_not_a_global_total()
    {
        var caption = TenantQuotaLabels.Caption(ExplorerTenantQuotaEnforcement.PerCluster);

        Assert.Multiple(() =>
        {
            Assert.That(caption, Is.EqualTo(TenantQuotaLabels.PerClusterCaption));
            Assert.That(caption, Does.Contain("not a global total"),
                "a per-cluster reading genuinely is not a global total and must not be presented as one");
        });
    }

    [Test]
    public void A_converged_reading_is_captioned_as_the_whole_consumption() =>
        Assert.That(
            TenantQuotaLabels.Caption(ExplorerTenantQuotaEnforcement.GlobalConverged),
            Is.EqualTo(TenantQuotaLabels.GlobalConvergedCaption));

    [Test]
    public void An_undeclared_enforcement_scope_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantQuotaLabels.Caption((ExplorerTenantQuotaEnforcement)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => TenantQuotaLabels.EnforcementLabel((ExplorerTenantQuotaEnforcement)99),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void A_reading_with_no_usage_is_captioned_as_unmeasured_whatever_its_scope()
    {
        var cold = MyTenantSample.Usage(
            scope: ExplorerTenantQuotaEnforcement.GlobalConverged,
            hasUsage: false);

        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaLabels.Caption(cold), Is.EqualTo(TenantQuotaLabels.NoUsageReadingCaption));
            Assert.That(
                TenantQuotaLabels.NoUsageReadingCaption,
                Does.Contain("not measured"),
                "a cold view still gets authoritative ceilings, and never fabricated zeros");
        });
    }

    [Test]
    public void A_warm_reading_is_captioned_by_its_enforcement_scope() =>
        Assert.That(
            TenantQuotaLabels.Caption(MyTenantSample.Usage(scope: ExplorerTenantQuotaEnforcement.PerCluster)),
            Is.EqualTo(TenantQuotaLabels.PerClusterCaption));

    [Test]
    public void Caption_rejects_a_null_reading() =>
        Assert.That(
            () => TenantQuotaLabels.Caption((ExplorerTenantQuotaUsage)null!),
            Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void A_gauge_that_admits_a_bar_has_no_replacement_text()
    {
        var gauge = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.Bytes,
            new ExplorerTenantQuotaDimension { Usage = 1, Limit = 2 });

        Assert.That(TenantQuotaLabels.WithoutBarText(gauge), Is.Null);
    }

    [Test]
    public void Each_non_bar_case_gets_its_own_words()
    {
        var unbounded = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.TreeCount,
            new ExplorerTenantQuotaDimension { Usage = 3, Limit = null });
        var unmeasured = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.OpsPerSecond,
            new ExplorerTenantQuotaDimension { Usage = null, Limit = 900 });
        var unknown = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.OpsPerSecond,
            new ExplorerTenantQuotaDimension { Usage = null, Limit = null });

        Assert.Multiple(() =>
        {
            Assert.That(TenantQuotaLabels.WithoutBarText(unbounded), Is.EqualTo(TenantQuotaLabels.UnboundedText));
            Assert.That(TenantQuotaLabels.WithoutBarText(unmeasured), Is.EqualTo(TenantQuotaLabels.UnmeasuredText));
            Assert.That(TenantQuotaLabels.WithoutBarText(unknown), Is.EqualTo(TenantQuotaLabels.UnknownText));

            // The three must be distinguishable, or the surface would be saying
            // the same thing about three different situations.
            Assert.That(
                new[]
                {
                    TenantQuotaLabels.UnboundedText,
                    TenantQuotaLabels.UnmeasuredText,
                    TenantQuotaLabels.UnknownText,
                },
                Is.Unique);
        });
    }

    [Test]
    public void The_enforcement_badge_names_each_scope()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantQuotaLabels.EnforcementLabel(ExplorerTenantQuotaEnforcement.GlobalConverged),
                Is.Not.Empty);
            Assert.That(
                TenantQuotaLabels.EnforcementLabel(ExplorerTenantQuotaEnforcement.PerCluster),
                Does.Contain("cluster"));
        });
    }
}
