using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// What each region means for the tenant and what a residency change would do
/// to it (issue #4078): with no residency every region serves the tenant, a
/// change is previewed region by region, and one that would leave the tenant
/// served nowhere turns Apply off and says why,
/// and keeps the stop-serving confirmation off the primary button.
/// </summary>
public sealed partial class TenancyRegionsTests
{
    [Test]
    public void With_no_residency_set_every_region_serves_the_tenant_and_none_reads_as_absent()
    {
        var cut = RenderRegions(resident: []);

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr").Select(row => new[]
            {
                row.Children[0].TextContent.Trim(),
                row.Children[1].QuerySelector(".lt-pill__text")!.TextContent.Trim(),
                row.Children[1].QuerySelector(".lt-tenancy-meaning")!.TextContent.Trim(),
                row.Children[2].TextContent.Trim(),
            }).ToArray();
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "ap-south", "No residency set", TenancyFormat.NoResidencyMeaning, "Served" },
                new[] { "eu-west", "No residency set", TenancyFormat.NoResidencyMeaning, "Served" },
                new[] { "us-east", "No residency set", TenancyFormat.NoResidencyMeaning, "Served" },
            }));
            Assert.That(cut.Markup, Does.Not.Contain("Not resident"));
            Assert.That(cut.FindAll("thead th").Select(header => header.TextContent.Trim()), Does.Contain("Served here"));
        });
    }

    [Test]
    public void A_change_that_keeps_the_tenant_served_is_previewed_region_by_region_and_applied_from_the_primary_button()
    {
        var cut = RenderRegions(resident: ["eu-west", "us-east"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(false);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-tenancy-preview__title").TextContent.Trim(), Is.EqualTo("If you apply this residency"));
            Assert.That(Preview(cut), Is.EqualTo(new[]
            {
                "eu-west stays in the residency, and is still served there.",
                "us-east starts draining, and stops being served there.",
            }));
            Assert.That(cut.FindAll(".lt-tenancy-served-nowhere"), Is.Empty);
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.False);
            Assert.That(TenancyForms.HasButton(cut, "Apply anyway and stop serving acme..."), Is.False);
        });
    }

    [Test]
    public void A_first_residency_turns_apply_off_and_explains_backfill()
    {
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);

        cut.WaitUntil(() =>
        {
            Assert.That(Preview(cut), Is.EqualTo(new[]
            {
                "ap-south stops being served, because it is not in the residency.",
                "eu-west stops being served, because it is not in the residency.",
                "us-east joins the residency as Provisioning, then is automatically backfilled before it can serve the tenant.",
            }));
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.True);

            var warning = cut.Find(".lt-tenancy-served-nowhere");
            Assert.That(warning.GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(warning.TextContent, Does.Contain("Applying this leaves tenant acme served nowhere, so Apply is off.")
                .And.Contain("Not Online for it: us-east (added: starts Provisioning).")
                .And.Contain("A tenant with residency is served only in its Online regions")
                .And.Contain("No region is Online for acme yet. The tenant remains unserved until an added region's backfill is verified.")
                .And.Contain("Leave residency unset to keep it served in every region."));
            Assert.That(warning.QuerySelector("a"), Is.Null);
        });

        TenancyForms.Button(cut, "Apply residency").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty, "Apply opens no stop-serving dialog");
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
        });
    }

    [Test]
    public void A_region_kept_while_it_is_not_yet_online_is_named_with_its_state()
    {
        var tenant = Cluster.Tenants["acme"];
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        var index = tenant.Regions.FindIndex(region => region.RegionId == "us-east");
        tenant.Regions[index] = tenant.Regions[index] with { Status = TenantRegionLifecycleStatus.Provisioning };
        TenancyForms.Button(cut, "Refresh").Click();
        cut.WaitUntil(() => Assert.That(Checkbox(cut, "us-east").HasAttribute("checked"), Is.True));

        Checkbox(cut, "eu-west").Change(true);

        cut.WaitUntil(() =>
        {
            // Nothing serves the tenant now, so the change stops nothing: Apply stays the primary action (issue #4114).
            Assert.That(cut.Find(".lt-tenancy-still-unserved").TextContent, Does.Contain("stays unserved").And.Contain("until backfill is verified in one of eu-west, us-east"));
            Assert.That(cut.FindAll(".lt-tenancy-served-nowhere"), Is.Empty);
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.False);
            Assert.That(Preview(cut), Does.Contain("us-east stays in the residency, and is not served there until it is Online."));
        });
    }

    [Test]
    public void Dropping_the_only_online_region_advises_keeping_one_that_is_online()
    {
        var cut = RenderRegions(resident: ["eu-west"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(true);
        Checkbox(cut, "eu-west").Change(false);

        cut.WaitUntil(() =>
        {
            var warning = cut.Find(".lt-tenancy-served-nowhere").TextContent;
            Assert.That(warning, Does.Contain("Keep a region that is already Online, or wait for the new region's backfill to complete."));
            Assert.That(warning, Does.Not.Contain("No region is Online"));
        });
    }

    [Test]
    public void The_stop_serving_confirmation_keeps_serving_on_its_primary_button()
    {
        var cut = RenderRegions(resident: []);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(true);
        cut.WaitUntil(() => TenancyForms.Button(cut, "Apply anyway and stop serving acme..."));

        var secondary = TenancyForms.Button(cut, "Apply anyway and stop serving acme...");
        Assert.That(secondary.ClassList, Does.Contain("lt-btn--quiet"), "the path that stops service is a secondary button");
        secondary.Click();

        cut.WaitUntil(() =>
        {
            var keep = TenancyForms.Button(cut, "Keep serving acme");
            var stop = TenancyForms.Button(cut, "Stop serving acme");
            Assert.That(keep.ClassList, Does.Not.Contain("lt-btn--destructive").And.Not.Contain("lt-btn--quiet"), "keeping service is the primary action");
            Assert.That(stop.ClassList, Does.Contain("lt-btn--destructive"));
            Assert.That(TenancyForms.HasButton(cut, "Apply and stop serving"), Is.False);
        });
    }

    private static string[] Preview(IRenderedComponent<TenancyRegions> cut) =>
        [.. cut.FindAll(".lt-tenancy-preview__list li").Select(item => item.TextContent.Trim())];
}
