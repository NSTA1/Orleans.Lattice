using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// A tenant's quota: the rows per dimension with its edge cases (unbounded,
/// unmeasured, over the ceiling, a ceiling of zero), the default tenant, the
/// operator's editor with per-field validation, and the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyQuotaTests : TenancyTestContext
{
    [Test]
    public void Each_dimension_reads_honestly()
    {
        var tenant = Cluster.Tenants["acme"];
        tenant.Quotas = new TenantQuotasDescriptor { MaxBytes = 1024, MaxKeys = 0, MaxTreeCount = 10, BurstPercent = 10 };
        tenant.Usage["bytes"] = 2048;
        tenant.Usage["keys"] = 5;
        tenant.Usage["memory"] = 4096;

        var cut = RenderQuota();

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr").Select(row => row.Children.Select(cell => cell.TextContent.Trim()).ToArray()).ToArray();
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "Stored bytes", "2 KiB", "1 KiB", "1.1 KiB", "200%, over by 1 KiB" },
                new[] { "Keys", "5", "0", "0", "100%, over by 5" },
                new[] { "Memory", "4 KiB", "Unbounded", "None", "No ceiling" },
                new[] { "Trees", "Not measured", "10", "11", "Not measured" },
                new[] { "Operations per second", "Not measured", "Unbounded", "None", "No ceiling, not measured" },
            }));
            Assert.That(cut.FindAll(".lt-tenancy-meter").Select(meter => meter.ClassList.Contains("lt-tenancy-meter--over")), Is.EqualTo(new[] { true, true }));
            Assert.That(cut.FindAll(".lt-tenancy-meter__fill").Select(fill => fill.GetAttribute("style")), Is.EqualTo(new[] { "inline-size: 100%", "inline-size: 100%" }));
            Assert.That(cut.FindAll(".lt-dl__row dd").Select(value => value.TextContent.Trim()), Is.EqualTo(new[]
            {
                "Across every region (converged)",
                "10% above each ceiling",
                "Tenant groups 500 (default), Group membership edges 10,000 (default), Tenant members 5,000 (default), Tenant access rules 1,000 (default)",
            }));
        });
    }

    [Test]
    public void A_tenant_with_no_usage_says_so()
    {
        var cut = RenderQuota();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.EqualTo("No usage has been measured for this tenant yet."));
            Assert.That(cut.FindAll(".lt-dl__row dd")[1].TextContent.Trim(), Is.EqualTo("None"));
        });
    }

    [Test]
    public void The_default_tenant_is_always_unbounded_and_offers_no_editor()
    {
        var cut = RenderSection<TenancyQuota>(parameters => parameters.Add(quota => quota.TenantId, TenantId.DefaultId).Add(quota => quota.CanEdit, true));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.Contains("always unbounded"));
            Assert.That(TenancyForms.HasButton(cut, "Edit quotas"), Is.False);
        });
    }

    [Test]
    public void Without_the_edit_grant_there_is_no_editor()
    {
        var cut = RenderQuota(canEdit: false);

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(5)));
        Assert.That(TenancyForms.HasButton(cut, "Edit quotas"), Is.False);
    }

    [Test]
    public void The_editor_loads_the_quotas_saves_them_and_rereads_the_usage()
    {
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxKeys = 100 };
        var cut = OpenEditor();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.Field(cut, "Keys").GetAttribute("value"), Is.EqualTo("100"));
            Assert.That(TenancyForms.Field(cut, "Stored bytes").GetAttribute("value"), Is.Empty);
        });

        TenancyForms.Type(cut, "Stored bytes", "1 GiB");
        TenancyForms.Type(cut, "Keys", "");
        TenancyForms.Type(cut, "Burst allowance (percent)", "25");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Quotas, Is.EqualTo(new TenantQuotasDescriptor { MaxBytes = 1L << 30, BurstPercent = 25 }));
            Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty);
            Assert.That(cut.FindAll("tbody tr")[0].Children[2].TextContent.Trim(), Is.EqualTo("1 GiB"));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Quotas of tenant acme saved."));
        });
    }

    [Test]
    public void Clearing_every_ceiling_lifts_the_quotas()
    {
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxKeys = 100 };
        var cut = OpenEditor();

        TenancyForms.Type(cut, "Keys", "");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme now has no quota ceilings.")));
    }

    [Test]
    public void Invalid_fields_are_marked_and_nothing_is_sent()
    {
        var cut = OpenEditor();

        TenancyForms.Type(cut, "Trees", "ten");
        TenancyForms.Type(cut, "Burst allowance (percent)", "-5");
        cut.Find("form.lt-tenancy-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Trees"), Is.EqualTo(TenancyQuotaDraft.InvalidCeilingMessage));
            Assert.That(TenancyForms.ErrorOf(cut, "Burst allowance (percent)"), Is.EqualTo(TenancyQuotaDraft.InvalidBurstMessage));
            Assert.That(TenancyForms.ErrorOf(cut, "Keys"), Is.Null);
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetTenantQuotasAsync)));
        });
    }

    [Test]
    public void The_clusters_refusal_is_shown_in_the_form_and_cancel_closes_it()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.SetTenantQuotasAsync), FakeTenancyCluster.Denied());
        var cut = OpenEditor();

        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-form__error").TextContent, Is.EqualTo(TenancyFailure.NotPermittedMessage)));

        TenancyForms.Button(cut, "Cancel").Click();
        Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty);
    }

    [Test]
    public void A_refused_read_is_not_permitted_and_a_failed_one_can_be_retried()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.GetQuotaUsageAsync), FakeTenancyCluster.Denied());
        var denied = RenderQuota();
        denied.WaitUntil(() => Assert.That(denied.Find(".lt-empty h3").TextContent, Is.EqualTo("Not permitted")));

        Cluster.Fail(nameof(FakeTenancyCluster.GetQuotaUsageAsync), new TimeoutException());
        var failed = RenderQuota();
        failed.WaitUntil(() => Assert.That(failed.Find(".lt-empty h3").TextContent, Is.EqualTo("Quota could not be read")));
        Cluster.Heal(nameof(FakeTenancyCluster.GetQuotaUsageAsync));
        TenancyForms.Button(failed, "Try again").Click();
        failed.WaitUntil(() => Assert.That(failed.FindAll("tbody tr"), Has.Count.EqualTo(5)));
    }

    [Test]
    public void While_the_quota_loads_a_skeleton_is_shown()
    {
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.GetQuotaUsageAsync));

        var cut = RenderQuota();

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(5)));
    }

    [Test]
    public void Below_the_small_breakpoint_dimensions_are_rows_with_an_over_limit_pill_and_the_editor_is_a_sheet()
    {
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxKeys = 1 };
        Cluster.Tenants["acme"].Usage["keys"] = 2;
        var cut = RenderQuota(band: LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-compact-row__primary").Select(line => line.TextContent.Trim()).First(), Is.EqualTo("Stored bytes"));
            Assert.That(cut.FindAll(".lt-compact-row__secondary")[1].TextContent, Does.Contain("Over limit").And.Contain("2 of 1"));
        });

        TenancyForms.Button(cut, "Edit quotas").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end")));
    }

    [Test]
    public void The_delegated_access_caps_read_as_their_defaults_when_unset_never_as_unbounded()
    {
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxGroups = 20, MaxTenantRules = 0 };

        var cut = RenderQuota();

        cut.WaitUntil(() => Assert.That(
            cut.FindAll(".lt-dl__row dd")[2].TextContent.Trim(),
            Is.EqualTo("Tenant groups 20, Group membership edges 10,000 (default), Tenant members 5,000 (default), Tenant access rules 0")));
        Assert.That(cut.Markup, Does.Not.Contain("Unbounded groups"));
    }

    [Test]
    public void The_editor_sets_and_clears_the_delegated_access_caps_with_a_blank_meaning_the_default()
    {
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxKeys = 100, MaxMemberSubjects = 40 };
        var cut = OpenEditor();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.Field(cut, "Tenant members").GetAttribute("value"), Is.EqualTo("40"));
            Assert.That(TenancyForms.Field(cut, "Tenant groups").GetAttribute("value"), Is.Empty);
            Assert.That(cut.FindAll(".lt-field__hint").Select(hint => hint.TextContent.Trim()), Has.Some.EqualTo("Blank applies the default cap of 500. It is never unbounded."));
        });

        TenancyForms.Type(cut, "Tenant groups", "50");
        TenancyForms.Type(cut, "Group membership edges", "2,000");
        TenancyForms.Type(cut, "Tenant members", "");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(
            Cluster.Tenants["acme"].Quotas,
            Is.EqualTo(new TenantQuotasDescriptor { MaxKeys = 100, MaxGroups = 50, MaxMembershipEdges = 2000 })));
    }

    [Test]
    public void An_invalid_delegated_access_cap_is_marked_and_nothing_is_sent()
    {
        var cut = OpenEditor();

        TenancyForms.Type(cut, "Tenant access rules", "lots");
        cut.Find("form.lt-tenancy-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Tenant access rules"), Is.EqualTo(TenancyQuotaDraft.InvalidCapMessage));
            Assert.That(TenancyForms.ErrorOf(cut, "Tenant groups"), Is.Null);
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetTenantQuotasAsync)));
        });
    }

    [Test]
    public void The_draft_round_trips_every_delegated_access_cap()
    {
        var quotas = new TenantQuotasDescriptor { MaxGroups = 1, MaxMembershipEdges = 2, MaxMemberSubjects = 3, MaxTenantRules = 4 };

        var draft = TenancyQuotaDraft.From(quotas);

        Assert.Multiple(() =>
        {
            Assert.That(TenancyAccessCaps.All.Select(cap => draft[cap]), Is.EqualTo(new[] { "1", "2", "3", "4" }));
            Assert.That(draft.TryBuild(out var built, out _), Is.True);
            Assert.That(built, Is.EqualTo(quotas));
            Assert.That(draft.CapErrors, Is.Empty);
            Assert.That(TenancyQuotaDraft.From(default).TryBuild(out var unset, out _), Is.True);
            Assert.That(TenancyAccessCaps.All.Select(cap => TenancyAccessCaps.Of(unset, cap)), Is.All.Null, "a blank cap is sent as unset, so the default applies");
            Assert.That(TenancyAccessCaps.All.Select(TenancyAccessCaps.Default), Is.EqualTo(new long[] { 500, 10_000, 5_000, 1_000 }));
        });
    }

    private IRenderedComponent<TenancyQuota> RenderQuota(bool canEdit = true, LtBreakpoint? band = null) =>
        RenderSection<TenancyQuota>(parameters => parameters.Add(quota => quota.TenantId, "acme").Add(quota => quota.CanEdit, canEdit), band);

    private IRenderedComponent<TenancyQuota> OpenEditor()
    {
        var cut = RenderQuota();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Edit quotas"));
        TenancyForms.Button(cut, "Edit quotas").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));
        return cut;
    }
}
