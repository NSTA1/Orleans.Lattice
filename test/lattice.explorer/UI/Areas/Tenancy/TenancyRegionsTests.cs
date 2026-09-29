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
public sealed class TenancyRegionsTests : TenancyTestContext
{
    [Test]
    public void Regions_are_listed_with_their_lifecycle_and_the_invariants_at_each_control()
    {
        var cut = RenderRegions();

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr").Select(row => row.Children.Take(3).Select(cell => cell.TextContent.Trim()).ToArray()).ToArray();
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "ap-south", "Not resident", "Not allowed" },
                new[] { "eu-west", "Online", "Allowed" },
                new[] { "us-east", "Not resident", "Allowed" },
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
        Assert.That(TenancyForms.Field(cut, "Allowed region ids").GetAttribute("value"), Is.EqualTo("eu-west, us-east"));

        TenancyForms.Type(cut, "Allowed region ids", "eu-west us-east; ap-south");
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

        TenancyForms.Type(cut, "Allowed region ids", "eu-west");
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

        TenancyForms.Type(cut, "Allowed region ids", "us-east");
        cut.Find("form.lt-tenancy-allowed").Submit();

        Assert.Multiple(() =>
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

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[2].Children[1].TextContent.Trim(), Is.EqualTo("Backfilling")));
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
        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "eu-west", Status = TenantRegionLifecycleStatus.Online, IsAllowed = true });
        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "ap-south", Status = TenantRegionLifecycleStatus.None, IsAllowed = false });
        return RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme").Add(regions => regions.CanAuthorize, canAuthorize), band);
    }

    private static AngleSharp.Dom.IElement Checkbox(IRenderedComponent<TenancyRegions> cut, string region) =>
        TenancyForms.Field(cut, $"Resident in {region}");
}
