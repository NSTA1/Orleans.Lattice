using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The area's plain types: its addresses, its words and figures, quota gauges
/// that never coalesce an absence to zero, the quota draft, the residency plan's
/// invariants, and the classification of every fault a tenant facade raises.
/// </summary>
[TestFixture]
public sealed class TenancyModelTests
{
    [Test]
    public void The_addresses_split_the_directory_from_the_tenant_rooted_workspace()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyRoutes.Directory.Format(), Is.EqualTo("/tenancy"));
            Assert.That(TenancyRoutes.Tenant("acme").Format(), Is.EqualTo("/tenancy/acme"));
            Assert.That(TenancyRoutes.TenantSharing("acme").Format(), Is.EqualTo("/tenancy/acme/sharing"));
            Assert.That(TenancyRoutes.TenantMembers("acme").Format(), Is.EqualTo("/tenancy/acme/members"));
            Assert.That(TenancyRoutes.TenantQuota("acme").Format(), Is.EqualTo("/tenancy/acme/quota"));
            Assert.That(TenancyRoutes.TenantRegions("acme").Format(), Is.EqualTo("/tenancy/acme/regions"));
            Assert.That(TenancyRoutes.MyTenant("acme").Format(), Is.EqualTo("/t/acme/tenancy"));
            Assert.That(TenancyRoutes.MyTenant("acme", TenancyRoutes.SharingSegment).Format(), Is.EqualTo("/t/acme/tenancy/sharing"));
            Assert.That(TenancyRoutes.Apps("acme").Format(), Is.EqualTo("/t/acme/apps"));
            Assert.That(TenancyRoutes.IsMyTenantSection("members"), Is.True);
            Assert.That(TenancyRoutes.IsMyTenantSection("grants"), Is.False);
            Assert.That(TenancyRoutes.IsMyTenantSection(null), Is.False);
        });
    }

    [Test]
    public void An_empty_tenant_id_is_refused_by_every_address()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => TenancyRoutes.Tenant(""));
            Assert.Throws<ArgumentException>(() => TenancyRoutes.TenantSharing(""));
            Assert.Throws<ArgumentException>(() => TenancyRoutes.MyTenant(""));
            Assert.Throws<ArgumentException>(() => TenancyRoutes.Apps(""));
        });
    }

    [Test]
    public void The_stylesheet_is_served_from_the_shells_content_base()
    {
        Assert.That(TenancyAssets.Stylesheet, Is.EqualTo("_content/Orleans.Lattice.Explorer.UI/tenancy/lattice-tenancy.css"));
    }

    [Test]
    [TestCase(TenantLifecycleStatus.Active, "Active", LtStateRole.Enabled)]
    [TestCase(TenantLifecycleStatus.Suspended, "Suspended", LtStateRole.Disabled)]
    [TestCase((TenantLifecycleStatus)9, "Unknown", LtStateRole.Unknown)]
    public void Tenant_states_have_a_word_and_a_role(TenantLifecycleStatus status, string label, LtStateRole role)
    {
        Assert.That((TenancyFormat.TenantStateLabel(status), TenancyFormat.TenantStateRole(status)), Is.EqualTo((label, role)));
    }

    [Test]
    [TestCase(TenantGrantLifecycleState.Active, "Active", LtStateRole.Enabled)]
    [TestCase(TenantGrantLifecycleState.Pending, "Pending", LtStateRole.Drift)]
    [TestCase(TenantGrantLifecycleState.Rejected, "Rejected", LtStateRole.Disabled)]
    [TestCase(TenantGrantLifecycleState.Revoked, "Revoked", LtStateRole.Uninstalled)]
    [TestCase((TenantGrantLifecycleState)9, "Unknown", LtStateRole.Unknown)]
    public void Grant_states_have_a_word_and_a_role(TenantGrantLifecycleState state, string label, LtStateRole role)
    {
        Assert.That((TenancyFormat.GrantStateLabel(state), TenancyFormat.GrantStateRole(state)), Is.EqualTo((label, role)));
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.None, "Not resident", LtStateRole.Disabled, false)]
    [TestCase(TenantRegionLifecycleStatus.Provisioning, "Provisioning", LtStateRole.Lagging, true)]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, "Backfilling", LtStateRole.Lagging, true)]
    [TestCase(TenantRegionLifecycleStatus.Online, "Online", LtStateRole.Healthy, true)]
    [TestCase(TenantRegionLifecycleStatus.Draining, "Draining", LtStateRole.Lagging, false)]
    [TestCase(TenantRegionLifecycleStatus.Offline, "Offline", LtStateRole.Stalled, false)]
    [TestCase(TenantRegionLifecycleStatus.Removed, "Removed", LtStateRole.Uninstalled, false)]
    public void Region_states_have_a_word_a_role_and_a_residency(TenantRegionLifecycleStatus status, string label, LtStateRole role, bool resident)
    {
        Assert.That(
            (TenancyFormat.RegionStatusLabel(status), TenancyFormat.RegionStatusRole(status), TenancyFormat.IsResident(status)),
            Is.EqualTo((label, role, resident)));
    }

    [Test]
    public void Access_enforcement_and_dimensions_are_named()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.AccessLabel(TenantGrantAccess.Read), Is.EqualTo("Read"));
            Assert.That(TenancyFormat.AccessLabel(TenantGrantAccess.Write), Is.EqualTo("Write"));
            Assert.That(TenancyFormat.AccessLabel(TenantGrantAccess.ReadWrite), Is.EqualTo("Read and write"));
            Assert.That(TenancyFormat.AccessLabel(TenantGrantAccess.None), Is.EqualTo("Nothing"));
            Assert.That(TenancyFormat.EnforcementLabel(TenantQuotaEnforcementScope.PerCluster), Is.EqualTo("Per cluster"));
            Assert.That(TenancyFormat.EnforcementLabel(TenantQuotaEnforcementScope.GlobalConverged), Is.EqualTo("Across every region (converged)"));
            Assert.That(TenancyFormat.EnforcementLabel((TenantQuotaEnforcementScope)9), Is.EqualTo("Unknown"));
            Assert.That(TenancyFormat.Dimensions.Select(TenancyFormat.DimensionLabel),
                Is.EqualTo(new[] { "Stored bytes", "Keys", "Memory", "Trees", "Operations per second" }));
            Assert.That(TenancyFormat.DimensionLabel((TenancyQuotaDimension)9), Is.EqualTo("9"));
        });
    }

    [Test]
    [TestCase(0L, "0 bytes")]
    [TestCase(1L, "1 byte")]
    [TestCase(1023L, "1,023 bytes")]
    [TestCase(1024L, "1 KiB")]
    [TestCase(1536L, "1.5 KiB")]
    [TestCase(10L * 1024 * 1024, "10 MiB")]
    [TestCase(5L * 1024 * 1024 * 1024 * 1024, "5 TiB")]
    [TestCase(long.MaxValue, "8 EiB")]
    public void Bytes_read_in_binary_units(long value, string expected)
    {
        Assert.That(TenancyFormat.Bytes(value), Is.EqualTo(expected));
    }

    [Test]
    public void Figures_follow_their_dimension_and_region_lists_read_as_one_line()
    {
        var regions = new[]
        {
            new TenantRegionStatusDescriptor { RegionId = "eu-west", Status = TenantRegionLifecycleStatus.Online, IsAllowed = true },
            new TenantRegionStatusDescriptor { RegionId = "us-east", Status = TenantRegionLifecycleStatus.Draining, IsAllowed = true },
            new TenantRegionStatusDescriptor { RegionId = "ap-south", Status = TenantRegionLifecycleStatus.None, IsAllowed = false },
        };

        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.Figure(TenancyQuotaDimension.MemoryBytes, 2048), Is.EqualTo("2 KiB"));
            Assert.That(TenancyFormat.Figure(TenancyQuotaDimension.Keys, 12345), Is.EqualTo("12,345"));
            Assert.That(TenancyFormat.ResidentRegions(regions), Is.EqualTo(new[] { "eu-west" }));
            Assert.That(TenancyFormat.AllowedRegions(regions), Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(TenancyFormat.RegionList(["eu-west", "us-east"]), Is.EqualTo("eu-west, us-east"));
            Assert.That(TenancyFormat.RegionList([]), Is.EqualTo("None"));
            Assert.That(TenancyFormat.RegionList([], "nowhere"), Is.EqualTo("nowhere"));
        });
    }

    [Test]
    public void A_gauge_never_draws_a_bar_the_reading_does_not_support()
    {
        var bar = new TenancyQuotaGauge(TenancyQuotaDimension.Keys, new TenantQuotaDimensionUsage { Usage = 420, Limit = 1000, BurstLimit = 1100 });
        var unbounded = new TenancyQuotaGauge(TenancyQuotaDimension.Keys, new TenantQuotaDimensionUsage { Usage = 5 });
        var unmeasured = new TenancyQuotaGauge(TenancyQuotaDimension.Bytes, new TenantQuotaDimensionUsage { Limit = 2048 });
        var unknown = new TenancyQuotaGauge(TenancyQuotaDimension.TreeCount, TenantQuotaDimensionUsage.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That((bar.Percent, bar.BarPercent, bar.UseText, bar.UsageText, bar.LimitText, bar.BurstText), Is.EqualTo(((int?)42, (int?)42, "42%", "420", "1,000", "1,100")));
            Assert.That((unbounded.Percent, unbounded.UseText, unbounded.LimitText, unbounded.BurstText), Is.EqualTo(((int?)null, "No ceiling", "Unbounded", "None")));
            Assert.That((unmeasured.Percent, unmeasured.UseText, unmeasured.UsageText, unmeasured.LimitText), Is.EqualTo(((int?)null, "Not measured", "Not measured", "2 KiB")));
            Assert.That((unknown.BarPercent, unknown.UseText, unknown.IsOverLimit), Is.EqualTo(((int?)null, "No ceiling, not measured", false)));
        });
    }

    [Test]
    public void Over_its_ceiling_a_gauge_says_by_how_much_and_its_bar_is_full()
    {
        var over = new TenancyQuotaGauge(TenancyQuotaDimension.Bytes, new TenantQuotaDimensionUsage { Usage = 3072, Limit = 2048 });

        Assert.Multiple(() =>
        {
            Assert.That(over.IsOverLimit, Is.True);
            Assert.That(over.Percent, Is.EqualTo(150));
            Assert.That(over.BarPercent, Is.EqualTo(100));
            Assert.That(over.UseText, Is.EqualTo("150%, over by 1 KiB"));
        });
    }

    [Test]
    public void A_ceiling_of_zero_reads_full_once_anything_is_used()
    {
        var used = new TenancyQuotaGauge(TenancyQuotaDimension.Keys, new TenantQuotaDimensionUsage { Usage = 1, Limit = 0 });
        var unused = new TenancyQuotaGauge(TenancyQuotaDimension.Keys, new TenantQuotaDimensionUsage { Usage = 0, Limit = 0 });

        Assert.Multiple(() =>
        {
            Assert.That((used.Percent, used.IsOverLimit), Is.EqualTo(((int?)100, true)));
            Assert.That((unused.Percent, unused.IsOverLimit), Is.EqualTo(((int?)0, false)));
        });
    }

    [Test]
    public void The_headline_names_the_dimension_nearest_its_ceiling()
    {
        var report = Report(bytes: new() { Usage = 10, Limit = 100 }, keys: new() { Usage = 90, Limit = 100 });

        Assert.That(TenancyFormat.QuotaHeadline(report), Is.EqualTo("Keys 90%"));
    }

    [Test]
    public void The_headline_calls_out_a_breach()
    {
        var report = Report(bytes: new() { Usage = 200, Limit = 100 }, keys: new() { Usage = 99, Limit = 100 });

        Assert.That(TenancyFormat.QuotaHeadline(report), Is.EqualTo("Stored bytes over limit"));
    }

    [Test]
    public void The_headline_distinguishes_unbounded_from_unmeasured_and_the_default_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.QuotaHeadline(Report()), Is.EqualTo("Unbounded"));
            Assert.That(TenancyFormat.QuotaHeadline(Report(bytes: new() { Limit = 100 })), Is.EqualTo("Not measured"));
            Assert.That(TenancyFormat.QuotaHeadline(Report(bytes: new() { Usage = 50, Limit = 100 }) with { IsDefault = true }), Is.EqualTo("Unbounded"));
        });
    }

    [Test]
    public void A_draft_loads_unbounded_as_blank_and_zero_as_zero()
    {
        var draft = TenancyQuotaDraft.From(new TenantQuotasDescriptor { MaxBytes = 0, MaxKeys = 1000, BurstPercent = 20 });

        Assert.Multiple(() =>
        {
            Assert.That(draft[TenancyQuotaDimension.Bytes], Is.EqualTo("0"));
            Assert.That(draft[TenancyQuotaDimension.Keys], Is.EqualTo("1000"));
            Assert.That(draft[TenancyQuotaDimension.TreeCount], Is.Empty);
            Assert.That(draft.BurstPercent, Is.EqualTo("20"));
            Assert.That(TenancyQuotaDraft.From(TenantQuotasDescriptor.Unbounded).BurstPercent, Is.Empty);
        });
    }

    [Test]
    public void A_draft_builds_units_separators_and_blanks()
    {
        var draft = new TenancyQuotaDraft
        {
            [TenancyQuotaDimension.Bytes] = " 2 GiB ",
            [TenancyQuotaDimension.MemoryBytes] = "512mib",
            [TenancyQuotaDimension.Keys] = "1,000,000",
            [TenancyQuotaDimension.OpsPerSecond] = "0",
            BurstPercent = "",
        };

        Assert.That(draft.TryBuild(out var quotas, out var errors), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(errors, Is.Empty);
            Assert.That(quotas.MaxBytes, Is.EqualTo(2L * 1024 * 1024 * 1024));
            Assert.That(quotas.MaxMemoryBytes, Is.EqualTo(512L * 1024 * 1024));
            Assert.That(quotas.MaxKeys, Is.EqualTo(1_000_000));
            Assert.That(quotas.MaxTreeCount, Is.Null);
            Assert.That(quotas.MaxOpsPerSecond, Is.EqualTo(0));
            Assert.That(quotas.BurstPercent, Is.EqualTo(0));
            Assert.That(draft.BurstError, Is.Null);
        });
    }

    [Test]
    public void A_draft_reports_each_bad_field_and_sends_nothing()
    {
        var draft = new TenancyQuotaDraft
        {
            [TenancyQuotaDimension.Bytes] = "lots",
            [TenancyQuotaDimension.Keys] = "-1",
            [TenancyQuotaDimension.TreeCount] = "5 GiB",
            BurstPercent = "3000000000",
        };

        Assert.That(draft.TryBuild(out var quotas, out var errors), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(quotas, Is.EqualTo(default(TenantQuotasDescriptor)));
            Assert.That(errors[TenancyQuotaDimension.Bytes], Is.EqualTo(TenancyQuotaDraft.InvalidByteCeilingMessage));
            Assert.That(errors[TenancyQuotaDimension.Keys], Is.EqualTo(TenancyQuotaDraft.InvalidCeilingMessage));
            Assert.That(errors[TenancyQuotaDimension.TreeCount], Is.EqualTo(TenancyQuotaDraft.InvalidCeilingMessage), "units apply only to bytes");
            Assert.That(errors.ContainsKey(TenancyQuotaDimension.MemoryBytes), Is.False);
            Assert.That(draft.BurstError, Is.EqualTo(TenancyQuotaDraft.InvalidBurstMessage));
        });
    }

    [Test]
    public void A_ceiling_that_overflows_with_its_unit_is_refused()
    {
        Assert.That(TenancyQuotaDraft.TryParseCeiling("9223372036854775807 TiB", true, out _), Is.False);
    }

    [Test]
    public void The_residency_plan_starts_at_the_committed_set_and_tracks_changes()
    {
        var plan = Plan(("eu-west", TenantRegionLifecycleStatus.Online, true), ("us-east", TenantRegionLifecycleStatus.None, true), ("ap-south", TenantRegionLifecycleStatus.None, false));

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.False);
            Assert.That(plan.Planned, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(plan.Rows.Select(row => row.Refusal), Is.EqualTo(new[] { TenancyResidencyPlan.LastRegionRefusal, null, TenancyResidencyPlan.NotAllowedRefusal }));
        });

        Assert.That(plan.Toggle("us-east"), Is.Null);
        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.True);
            Assert.That(plan.Planned, Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(plan.Rows[0].Refusal, Is.Null, "eu-west is no longer the last");
        });

        Assert.That(plan.Toggle("eu-west"), Is.Null);
        Assert.That(plan.Planned, Is.EqualTo(new[] { "us-east" }));

        plan.Revert();
        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.False);
            Assert.That(plan.Planned, Is.EqualTo(new[] { "eu-west" }));
        });
    }

    [Test]
    public void The_residency_plan_refuses_the_last_region_an_unallowed_region_and_an_unknown_one()
    {
        var plan = Plan(("eu-west", TenantRegionLifecycleStatus.Online, true), ("ap-south", TenantRegionLifecycleStatus.None, false));

        Assert.Multiple(() =>
        {
            Assert.That(plan.Toggle("eu-west"), Is.EqualTo(TenancyResidencyPlan.LastRegionRefusal));
            Assert.That(plan.Toggle("ap-south"), Is.EqualTo(TenancyResidencyPlan.NotAllowedRefusal));
            Assert.That(plan.Toggle("nowhere"), Is.EqualTo(TenancyResidencyPlan.NotAllowedRefusal));
            Assert.That(plan.IsChanged, Is.False);
            Assert.Throws<ArgumentNullException>(() => plan.Toggle(null!));
            Assert.Throws<ArgumentNullException>(() => plan.Reset(null!));
        });
    }

    [Test]
    public void A_draining_region_is_outside_the_plan_until_added_back()
    {
        var plan = Plan(("eu-west", TenantRegionLifecycleStatus.Online, true), ("us-east", TenantRegionLifecycleStatus.Draining, true));

        Assert.That(plan.Planned, Is.EqualTo(new[] { "eu-west" }));
        Assert.That(plan.Toggle("us-east"), Is.Null);
        Assert.That(plan.Rows[1] is { IsPlanned: true, IsResident: false }, Is.True);
    }

    [Test]
    public void Every_fault_is_classified_and_a_cancellation_is_not_shown()
    {
        var cases = new (Exception Fault, TenancyFailureKind Kind, string Message)[]
        {
            (FakeTenancyCluster.Denied(), TenancyFailureKind.Denied, TenancyFailure.NotPermittedMessage),
            (new TenantNotFoundException("acme"), TenancyFailureKind.NotFound, "Tenant acme does not exist, or you may not see it."),
            (new TenantNotFoundException(""), TenancyFailureKind.NotFound, "That tenant does not exist, or you may not see it."),
            (new TenantGrantNotFoundException("a", "b", "s"), TenancyFailureKind.NotFound, "That grant no longer exists."),
            (new TenantAlreadyExistsException("acme"), TenancyFailureKind.Refused, "A tenant with the id acme already exists."),
            (new ReservedTenantOperationException("default", "delete"), TenancyFailureKind.Refused, "The reserved default tenant cannot be suspended, deleted, given quotas or offered in a grant."),
            (new TenantLastAdminSubjectException("acme", "ops"), TenancyFailureKind.Refused, "A tenant keeps at least one admin subject. Add the replacement before removing the last one."),
            (new TenantLastRegionException("acme"), TenancyFailureKind.Refused, "A tenant stays resident in at least one region."),
            (new TenantRegionNotAllowedException("acme", "ap-south"), TenancyFailureKind.Refused, "Region ap-south is not allowed for this tenant, or the tenant is still resident there."),
            (new TenantGrantTransitionException("a", "b", "s", TenantGrantLifecycleState.Rejected, TenantGrantLifecycleState.Active), TenancyFailureKind.Refused, "The grant is rejected, so it cannot become active."),
            (new ArgumentException("bad id"), TenancyFailureKind.Invalid, "bad id"),
            (new KeyNotFoundException(), TenancyFailureKind.NotFound, "It no longer exists."),
            (new NotSupportedException(), TenancyFailureKind.Unavailable, TenancyFailure.NotServedMessage),
            (new InvalidOperationException(ShellTransportChannel.NotConfiguredMessage), TenancyFailureKind.Unavailable, TenancyFailure.NotConnectedMessage),
            (new InvalidOperationException("the cluster said no"), TenancyFailureKind.Refused, "the cluster said no"),
            (new TimeoutException(), TenancyFailureKind.Unavailable, TenancyFailure.NoAnswerMessage),
        };

        Assert.Multiple(() =>
        {
            foreach (var (fault, kind, message) in cases)
            {
                Assert.That(TenancyFailure.From(fault), Is.EqualTo(new TenancyFailure(kind, message)), fault.GetType().Name);
            }

            Assert.That(TenancyFailure.From(new OperationCanceledException()), Is.Null);
            Assert.That(new TenancyFailure(TenancyFailureKind.Unavailable, "x").IsRetryable, Is.True);
            Assert.That(new TenancyFailure(TenancyFailureKind.Denied, "x").IsRetryable, Is.False);
            Assert.Throws<ArgumentNullException>(() => TenancyFailure.From(null!));
        });
    }

    [Test]
    public void A_standing_names_its_workspace_and_whether_the_area_is_seen()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new TenancyStanding(false, "acme", "globex", true).Workspace, Is.EqualTo("globex"));
            Assert.That(new TenancyStanding(false, "acme", null, true).Workspace, Is.EqualTo("acme"));
            Assert.That(new TenancyStanding(true, "acme", null, false).MaySee, Is.True);
            Assert.That(new TenancyStanding(false, "acme", null, false).MaySee, Is.False);
        });
    }

    private static TenancyResidencyPlan Plan(params (string Id, TenantRegionLifecycleStatus Status, bool Allowed)[] regions)
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([.. regions.Select(region => new TenantRegionStatusDescriptor { RegionId = region.Id, Status = region.Status, IsAllowed = region.Allowed })]);
        return plan;
    }

    private static TenantQuotaUsageReport Report(TenantQuotaDimensionUsage bytes = default, TenantQuotaDimensionUsage keys = default) => new()
    {
        TenantId = "acme",
        HasUsage = true,
        Bytes = bytes,
        Keys = keys,
    };
}
