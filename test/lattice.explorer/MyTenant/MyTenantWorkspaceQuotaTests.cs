using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The Quota surface: the gauges projected from one reading, the caption that
/// qualifies them with the enforcement scope, and the guarantee that no figure
/// is invented when the cluster reported none.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceQuotaTests
{
    private static async Task<MyTenantWorkspaceHarness> OpenAsync(Action<FakeTenancyDomain>? configure = null)
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(configure);
        await harness.OpenAsync(MyTenantSurfaces.Quota);
        return harness;
    }

    [Test]
    public async Task Before_a_reading_arrives_no_gauges_are_rendered()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Usage, Is.Null);
            Assert.That(
                harness.Workspace.Gauges,
                Is.Empty,
                "five gauges of invented zeros would be worse than none");
            Assert.That(harness.Workspace.Caption, Is.Null);
            Assert.That(harness.Workspace.HasBreach, Is.False);
        });
    }

    [Test]
    public async Task Every_dimension_gets_a_gauge_in_the_readings_declared_order()
    {
        var harness = await OpenAsync();

        Assert.That(
            harness.Workspace.Gauges.Select(gauge => gauge.Kind).ToArray(),
            Is.EqualTo(ExplorerTenantQuotaUsage.Dimensions.ToArray()));
    }

    [Test]
    public async Task Each_dimension_keeps_its_own_case_through_the_projection()
    {
        var harness = await OpenAsync();
        var gauges = harness.Workspace.Gauges;

        Assert.Multiple(() =>
        {
            Assert.That(gauges[0].Presentation, Is.EqualTo(TenantQuotaPresentation.Bar), "bytes");
            Assert.That(gauges[0].BarPercent, Is.EqualTo(25));

            Assert.That(gauges[1].Presentation, Is.EqualTo(TenantQuotaPresentation.Bar), "keys, measured zero");
            Assert.That(gauges[1].BarPercent, Is.EqualTo(0));

            Assert.That(gauges[2].IsOverLimit, Is.True, "memory, a ceiling of zero with usage against it");
            Assert.That(gauges[2].BarPercent, Is.EqualTo(100));

            Assert.That(
                gauges[3].Presentation,
                Is.EqualTo(TenantQuotaPresentation.UnboundedWithUsage),
                "trees, no ceiling at all");
            Assert.That(gauges[3].HasBar, Is.False);

            Assert.That(
                gauges[4].Presentation,
                Is.EqualTo(TenantQuotaPresentation.UnmeasuredWithLimit),
                "ops per second, a ceiling nothing is sampling");
            Assert.That(gauges[4].HasBar, Is.False);
            Assert.That(gauges[4].Usage, Is.Null, "an unmeasured dimension is never faked to zero");
        });
    }

    [Test]
    public async Task A_per_cluster_reading_is_captioned_as_not_a_global_total()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Success(
                MyTenantSample.Usage(scope: ExplorerTenantQuotaEnforcement.PerCluster),
                "ok"));

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Workspace.EnforcementScope,
                Is.EqualTo(ExplorerTenantQuotaEnforcement.PerCluster));
            Assert.That(harness.Workspace.Caption, Is.EqualTo(TenantQuotaLabels.PerClusterCaption));
        });
    }

    [Test]
    public async Task A_converged_reading_is_captioned_as_the_whole_consumption()
    {
        var harness = await OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Workspace.EnforcementScope,
                Is.EqualTo(ExplorerTenantQuotaEnforcement.GlobalConverged));
            Assert.That(harness.Workspace.Caption, Is.EqualTo(TenantQuotaLabels.GlobalConvergedCaption));
        });
    }

    [Test]
    public async Task A_cold_reading_reports_authoritative_ceilings_and_no_usage()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Success(
                MyTenantSample.Usage(hasUsage: false),
                "ok"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.HasUsageReading, Is.False);
            Assert.That(harness.Workspace.Caption, Is.EqualTo(TenantQuotaLabels.NoUsageReadingCaption));
            Assert.That(harness.Workspace.Gauges, Is.Not.Empty, "the ceilings are still real");
        });
    }

    [Test]
    public async Task A_breach_is_surfaced_so_the_caller_does_not_have_to_find_it()
    {
        var harness = await OpenAsync();

        Assert.That(harness.Workspace.HasBreach, Is.True, "the sample's memory dimension is over its ceiling");
    }

    [Test]
    public async Task No_breach_is_reported_when_every_bounded_dimension_is_within_its_ceiling()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Success(
                MyTenantSample.Usage() with
                {
                    MemoryBytes = new ExplorerTenantQuotaDimension { Usage = 0, Limit = 100 },
                },
                "ok"));

        Assert.That(harness.Workspace.HasBreach, Is.False);
    }

    [Test]
    public async Task An_unbounded_dimension_alone_never_counts_as_a_breach()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Success(
                MyTenantSample.Usage() with
                {
                    MemoryBytes = new ExplorerTenantQuotaDimension { Usage = 1_000_000, Limit = null },
                    Bytes = new ExplorerTenantQuotaDimension { Usage = 1, Limit = 10 },
                },
                "ok"));

        Assert.That(harness.Workspace.HasBreach, Is.False, "no ceiling means no breach, however large the usage");
    }

    [Test]
    public async Task The_burst_percentage_travels_with_the_reading()
    {
        var harness = await OpenAsync();

        Assert.That(harness.Workspace.BurstPercent, Is.EqualTo(10));
    }

    [Test]
    public async Task A_refused_reading_leaves_no_gauges_and_reports_the_refusal()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Failure(
                TenantOperationStatus.Denied,
                "refused"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Usage, Is.Null);
            Assert.That(harness.Workspace.Gauges, Is.Empty);
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Refreshing_re_reads_the_tenants_consumption()
    {
        var harness = await OpenAsync();
        var before = harness.Service.TenantIdsTouched.Count;

        await harness.Workspace.RefreshQuotaAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.TenantIdsTouched.Count, Is.GreaterThan(before));
            Assert.That(harness.Service.TenantIdsTouched, Has.All.EqualTo(MyTenantSample.TenantId));
        });
    }

    [Test]
    public async Task The_gauge_array_is_reused_across_polls_so_refreshing_allocates_nothing()
    {
        var harness = await OpenAsync();
        var before = harness.Workspace.Gauges;

        await harness.Workspace.RefreshQuotaAsync();

        Assert.That(
            harness.Workspace.Gauges,
            Is.SameAs(before),
            "the polling path refills one array rather than allocating per read");
    }
}
