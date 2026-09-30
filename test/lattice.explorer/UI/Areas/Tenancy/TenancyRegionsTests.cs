using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// A tenant's regions: the per-region lifecycle, the residency plan with its two
/// invariants at the control, a confirmed drain, the operator's allowed set with
/// a confirmed revoke, refusals, and the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class TenancyRegionsTests : TenancyTestContext
{
    [Test]
    public void Regions_are_listed_with_their_lifecycle_and_the_invariants_at_each_control()
    {
        var cut = RenderRegions();

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr").Select(row => new[]
            {
                row.Children[0].TextContent.Trim(),
                row.Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim(),
                row.Children[2].TextContent.Trim(),
                row.Children[3].TextContent.Trim(),
            }).ToArray();
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "ap-south", "Not in residency", "Not served", "Not allowed" },
                new[] { "eu-west", "Online", "Served", "Allowed" },
                new[] { "us-east", "Not in residency", "Not served", "Allowed" },
            }));
            Assert.That(Checkbox(cut, "ap-south").HasAttribute("disabled"), Is.True);
            Assert.That(Checkbox(cut, "eu-west").HasAttribute("disabled"), Is.True, "the last resident region");
            Assert.That(Checkbox(cut, "us-east").HasAttribute("disabled"), Is.False);
            Assert.That(cut.FindAll(".lt-check__hint").Select(hint => hint.TextContent.Trim()), Is.EqualTo(new[] { TenancyResidencyPlan.NotAllowedRefusal, TenancyResidencyPlan.LastRegionRefusal }));
            Assert.That(cut.FindAll(".lt-dl__row dd").Select(value => value.TextContent.Trim()), Is.EqualTo(new[] { "eu-west, us-east", "eu-west" }));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Apply residency").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void Adding_a_region_applies_without_confirmation_and_reports_the_change()
    {
        var cut = RenderRegions();
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-actions .lt-tenancy-count").TextContent, Is.EqualTo("Not applied: add us-east.")));
        TenancyForms.Button(cut, "Apply residency").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Regions.Single(region => region.RegionId == "us-east").Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is adding us-east."));
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
        });
    }

    [Test]
    public void Removing_a_region_drains_only_after_a_confirmation()
    {
        var cut = RenderRegions(resident: ["eu-west", "us-east"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(false);
        TenancyForms.Button(cut, "Apply residency").Click();
        Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Remove regions from the residency?")));

        TenancyForms.Button(cut, "Drain and apply").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Regions.Single(region => region.RegionId == "us-east").Status, Is.EqualTo(TenantRegionLifecycleStatus.Draining));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is draining us-east."));
        });
    }

    [Test]
    public void Reset_discards_the_plan()
    {
        var cut = RenderRegions();
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);
        TenancyForms.Button(cut, "Reset").Click();

        Assert.Multiple(() =>
        {
            Assert.That(Checkbox(cut, "us-east").HasAttribute("checked"), Is.False);
            Assert.That(cut.FindAll(".lt-tenancy-actions .lt-tenancy-count"), Is.Empty);
        });
    }

    [Test]
    public void A_refused_residency_change_is_a_toast()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.SetResidencyAsync), new TenantLastRegionException("acme"));
        var cut = RenderRegions();
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);
        TenancyForms.Button(cut, "Apply residency").Click();

        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("A tenant stays resident in at least one region.")));
    }

    [Test]
    public void An_operator_widens_the_allowed_set_without_confirmation()
    {
        var cut = RenderRegions(canAuthorize: true);
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));
        Assert.That(Chips(cut), Is.EqualTo(new[] { "eu-west", "us-east" }), "the allowed set is chosen chips");

        TenancyForms.Type(cut, "Allowed region ids", "ap-south,");
        cut.Find("form.lt-tenancy-allowed").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Regions.Where(region => region.IsAllowed).Select(region => region.RegionId), Is.EquivalentTo(new[] { "eu-west", "us-east", "ap-south" }));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is allowed us-east, eu-west, ap-south."));
        });
    }

    [Test]
    public void An_operator_revokes_an_allowed_region_only_after_a_confirmation()
    {
        var cut = RenderRegions(canAuthorize: true);
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));

        RemoveChip(cut, "us-east");
        cut.Find("form.lt-tenancy-allowed").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Revoke allowed regions?")));
        Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync)));

        TenancyForms.Button(cut, "Revoke and save").Click();

        cut.WaitUntil(() => Assert.That(Cluster.Tenants["acme"].Regions.Single(region => region.RegionId == "us-east").IsAllowed, Is.False));
    }

    [Test]
    public void Revoking_a_resident_region_is_refused_at_the_field()
    {
        var cut = RenderRegions(canAuthorize: true);
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));

        RemoveChip(cut, "eu-west");
        cut.Find("form.lt-tenancy-allowed").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Allowed region ids"), Is.EqualTo("Tenant acme is still resident in eu-west. Remove the residency first."));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync)));
        });
    }

    [Test]
    public void The_clusters_refusal_of_the_allowed_set_is_shown_at_the_field()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync), FakeTenancyCluster.Denied());
        var cut = RenderRegions(canAuthorize: true);
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));

        cut.Find("form.lt-tenancy-allowed").Submit();

        cut.WaitUntil(() => Assert.That(TenancyForms.ErrorOf(cut, "Allowed region ids"), Is.EqualTo(TenancyFailure.NotPermittedMessage)));
    }

    [Test]
    public void Refresh_rereads_a_region_moving_through_its_lifecycle()
    {
        var cut = RenderRegions();
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        var tenant = Cluster.Tenants["acme"];
        var index = tenant.Regions.FindIndex(region => region.RegionId == "us-east");
        tenant.Regions[index] = tenant.Regions[index] with { Status = TenantRegionLifecycleStatus.Backfilling };

        TenancyForms.Button(cut, "Refresh").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[2].Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim(), Is.EqualTo("Backfilling")));
    }

    [Test]
    public void While_a_saved_allowed_set_is_read_again_the_residency_controls_stay_disabled()
    {
        var cut = RenderRegions(canAuthorize: true);
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));
        var reread = Cluster.Hold(nameof(FakeTenancyCluster.GetTenantRegionStatusAsync));

        TenancyForms.Type(cut, "Allowed region ids", "ap-south,");
        cut.Find("form.lt-tenancy-allowed").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Services.GetToasts().Last().Message, Does.StartWith("Tenant acme is allowed"));
            Assert.That(cut.FindAll("input[type=checkbox]").Where(box => !box.HasAttribute("disabled")), Is.Empty, "an edit made now would be replaced by the re-read");
            Assert.That(cut.FindAll("button").Where(button => button.TextContent.Trim() is "Save allowed regions" or "Refresh").Select(button => button.HasAttribute("disabled")), Is.All.True);
        });

        cut.InvokeAsync(reread.SetResult);

        cut.WaitUntil(() =>
        {
            Assert.That(Checkbox(cut, "us-east").HasAttribute("disabled"), Is.False);
            Assert.That(TenancyForms.Button(cut, "Save allowed regions").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void The_allowed_set_and_the_residency_are_two_labelled_parts_each_stating_its_rule()
    {
        var cut = RenderRegions();

        cut.WaitUntil(() =>
        {
            var parts = cut.FindAll("section.lt-tenancy-part").ToArray();
            Assert.That(parts, Has.Length.EqualTo(2));
            Assert.That(parts.Select(part => part.QuerySelector("h3")!.TextContent), Is.EqualTo(new[]
            {
                "Allowed regions (set by a platform operator)",
                "Residency (where the tenant's data is kept)",
            }));
            Assert.That(parts.Select(part => part.GetAttribute("aria-labelledby")), Is.EqualTo(parts.Select(part => part.QuerySelector("h3")!.Id)));
            Assert.That(parts[0].QuerySelector(".lt-tenancy-note")!.TextContent, Does.Contain("A platform operator decides which regions this tenant may use").And.Contain("Only a platform operator can change this set."));
            Assert.That(parts[1].QuerySelector(".lt-tenancy-note")!.TextContent, Does.Contain("served only in regions that are Online"));
            Assert.That(parts[1].QuerySelector("table"), Is.Not.Null, "the residency table sits in the residency part");
            Assert.That(parts[0].QuerySelector("form"), Is.Null, "a tenant admin cannot change the allowed set");
        });
    }

    [Test]
    public void The_operators_allowed_picker_lives_in_the_allowed_part_and_its_hint_matches_the_picker()
    {
        var cut = RenderRegions(canAuthorize: true);

        cut.WaitUntil(() =>
        {
            var allowed = cut.FindAll("section.lt-tenancy-part")[0];
            Assert.That(allowed.QuerySelector("form.lt-tenancy-allowed"), Is.Not.Null);
            Assert.That(allowed.QuerySelector(".lt-tenancy-note")!.TextContent, Does.Not.Contain("Only a platform operator"));
            var hint = TenancyForms.Field(cut, "Allowed region ids").Closest(".lt-field")!.QuerySelector(".lt-field__hint")!.TextContent.Trim();
            Assert.That(hint, Is.EqualTo(TenancyRegions.AllowedHint));
            Assert.That(hint, Does.Not.Contain("comma"), "the picker takes chosen regions, not a comma-separated list");
        });
    }

    [Test]
    public void Each_lifecycle_status_says_what_it_means_and_a_provisioning_region_waits_for_promotion()
    {
        var tenant = Cluster.Tenants["acme"];
        tenant.Regions.Clear();
        var statuses = new[]
        {
            TenantRegionLifecycleStatus.Provisioning, TenantRegionLifecycleStatus.Backfilling, TenantRegionLifecycleStatus.Online,
            TenantRegionLifecycleStatus.Draining, TenantRegionLifecycleStatus.Offline, TenantRegionLifecycleStatus.Removed,
        };
        for (var i = 0; i < statuses.Length; i++)
        {
            tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = $"r{i}", Status = statuses[i], IsAllowed = true });
        }

        var cut = RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme"));

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr").ToArray();
            Assert.That(rows.Select(row => row.Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim()),
                Is.EqualTo(new[] { "Provisioning", "Backfilling", "Online", "Draining", "Offline", "Removed" }));
            Assert.That(rows.Select(row => row.Children[1].QuerySelector(".lt-tenancy-meaning")!.TextContent.Trim()),
                Is.EqualTo(statuses.Select(status => TenancyFormat.RegionStatusMeaning(status))));
            Assert.That(rows[0].Children[1].QuerySelector(".lt-tenancy-meaning")!.TextContent.Trim(),
                Is.EqualTo("Waiting for a platform operator to promote it; this tenant is not served here until it is Online."));
        });
    }

    [Test]
    public void A_region_outside_the_residency_says_it_does_not_serve_the_tenant()
    {
        var cut = RenderRegions();

        cut.WaitUntil(() => Assert.That(
            cut.FindAll("tbody tr")[0].Children[1].QuerySelector(".lt-tenancy-meaning")!.TextContent.Trim(),
            Is.EqualTo("Outside the tenant's residency, so it does not serve this tenant.")));
    }

    [Test]
    public void With_no_residency_set_the_residency_part_says_the_tenant_is_served_in_every_region()
    {
        var cut = RenderRegions(resident: []);

        cut.WaitUntil(() =>
        {
            var residency = cut.FindAll("section.lt-tenancy-part")[1];
            Assert.That(residency.QuerySelector(".lt-tenancy-unset")!.TextContent.Trim(), Is.EqualTo("No residency set: tenant acme is served in every region."));
            Assert.That(residency.QuerySelector(".lt-dl__row dd")!.TextContent.Trim(), Is.EqualTo("Not set: served in every region"));
            Assert.That(cut.FindAll(".lt-tenancy-warning"), Is.Empty);
        });
    }

    [Test]
    public void A_first_residency_is_applied_only_through_the_secondary_path_and_its_confirmation()
    {
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);
        cut.WaitUntil(() => Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.True));
        TenancyForms.Button(cut, "Apply anyway and stop serving acme...").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Stop serving tenant acme?"));
            Assert.That(cut.Find("[role=alertdialog] .lt-tenancy-consequence").TextContent, Does.Contain("is not served anywhere until a platform operator of the")
                .And.Contain("promotes one of us-east to Online"));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
        });

        TenancyForms.Button(cut, "Stop serving acme").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Regions.Single(region => region.RegionId == "us-east").Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is adding us-east. " + TenancyRegions.NotServedYet));
            Assert.That(cut.Find(".lt-tenancy-warning").TextContent, Does.Contain("Tenant acme is not served anywhere."));
        });
    }

    [Test]
    public void Keeping_service_in_the_no_online_region_confirmation_keeps_the_plan_and_writes_nothing()
    {
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(true);
        cut.WaitUntil(() => TenancyForms.Button(cut, "Apply anyway and stop serving acme..."));
        TenancyForms.Button(cut, "Apply anyway and stop serving acme...").Click();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));

        TenancyForms.Button(cut, "Keep serving acme").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
            Assert.That(Checkbox(cut, "us-east").HasAttribute("checked"), Is.True);
        });
    }

    [Test]
    public void Removing_the_only_online_region_states_both_the_drain_and_the_loss_of_service()
    {
        var cut = RenderRegions(resident: ["eu-west"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(true);
        Checkbox(cut, "eu-west").Change(false);

        cut.WaitUntil(() => Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.True));
        TenancyForms.Button(cut, "Apply anyway and stop serving acme...").Click();

        cut.WaitUntil(() =>
        {
            var dialog = cut.Find("[role=alertdialog]");
            Assert.That(dialog.QuerySelector(".lt-dialog__title")!.TextContent, Is.EqualTo("Stop serving tenant acme?"));
            Assert.That(dialog.TextContent, Does.Contain("will start draining").And.Contain("is not served anywhere"));
            Assert.That(TenancyForms.HasButton(cut, "Stop serving acme"), Is.True);
        });
    }

    [Test]
    public void A_tenant_resident_only_where_it_is_not_yet_online_is_said_to_be_served_nowhere()
    {
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        var tenant = Cluster.Tenants["acme"];
        var index = tenant.Regions.FindIndex(region => region.RegionId == "us-east");
        tenant.Regions[index] = tenant.Regions[index] with { Status = TenantRegionLifecycleStatus.Provisioning };

        TenancyForms.Button(cut, "Refresh").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-warning").GetAttribute("role"), Is.EqualTo("status")));
    }

    [Test]
    public void With_no_region_the_table_says_so()
    {
        Cluster.Tenants["acme"].Regions.Clear();

        var cut = RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme"));

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No region is allowed for tenant acme yet.")));
    }

    [Test]
    public void A_refused_read_is_not_permitted_and_a_failed_one_can_be_retried()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.GetTenantRegionStatusAsync), FakeTenancyCluster.Denied());
        var denied = RenderRegions();
        denied.WaitUntil(() => Assert.That(denied.Find(".lt-empty h3").TextContent, Is.EqualTo("Not permitted")));

        Cluster.Fail(nameof(FakeTenancyCluster.GetTenantRegionStatusAsync), new TimeoutException());
        var failed = RenderRegions();
        failed.WaitUntil(() => Assert.That(failed.Find(".lt-empty h3").TextContent, Is.EqualTo("Regions could not be read")));
        Cluster.Heal(nameof(FakeTenancyCluster.GetTenantRegionStatusAsync));
        TenancyForms.Button(failed, "Try again").Click();
        failed.WaitUntil(() => Assert.That(failed.FindAll("tbody tr"), Has.Count.EqualTo(3)));
    }

    [Test]
    public void Below_the_small_breakpoint_regions_are_rows_whose_sheet_carries_the_residency_control()
    {
        var cut = RenderRegions(band: LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-compact-row__primary").Select(line => line.TextContent.Trim()), Is.EqualTo(new[] { "ap-south", "eu-west", "us-east" })));

        cut.FindAll(".lt-table-list__open")[2].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-dialog__actions input[type=checkbox]"), Is.Not.Null));
    }

    private IRenderedComponent<TenancyRegions> RenderRegions(bool canAuthorize = false, string[]? resident = null, LtBreakpoint? band = null)
    {
        var tenant = Cluster.Tenants["acme"];
        tenant.Regions.Clear();
        resident ??= ["eu-west"];
        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "us-east", Status = resident.Contains("us-east") ? TenantRegionLifecycleStatus.Online : TenantRegionLifecycleStatus.None, IsAllowed = true });
        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "eu-west", Status = resident.Contains("eu-west") ? TenantRegionLifecycleStatus.Online : TenantRegionLifecycleStatus.None, IsAllowed = true });
        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "ap-south", Status = TenantRegionLifecycleStatus.None, IsAllowed = false });
        return RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme").Add(regions => regions.CanAuthorize, canAuthorize), band);
    }

    private static AngleSharp.Dom.IElement Checkbox(IRenderedComponent<TenancyRegions> cut, string region) =>
        TenancyForms.Field(cut, $"Resident in {region}");

    private static string[] Chips<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll(".lt-combobox__chip-value").Select(chip => chip.TextContent)];

    private static void RemoveChip<TComponent>(IRenderedComponent<TComponent> cut, string region)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.Find($"button[aria-label='Remove {region}']").Click();
}
