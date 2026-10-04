using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// Issue #4114: each region's residency lifecycle as the regions section shows
/// it - the step a transitional region has reached on its path and none for a
/// steady one, following the regions live on the circuit's clock until every one
/// is steady with each stage change announced politely, and a tenant whose
/// regions have all left its residency read as served nowhere, as the tenancy
/// engine serves it. Time moves only on the test's <c>ManualTimeProvider</c>.
/// </summary>
public sealed partial class TenancyRegionsTests
{
    [Test]
    [TestCase(TenantRegionLifecycleStatus.Provisioning, "Adding: Provisioning", "Step 1 of 3", "Next: Backfilling, when a platform operator of the hosting deployment promotes it.")]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, "Adding: Backfilling", "Step 2 of 3", "Next: Online, when a platform operator of the hosting deployment promotes it.")]
    [TestCase(TenantRegionLifecycleStatus.Draining, "Removing: Draining", "Step 1 of 3", "Next: Offline, taken automatically by the region's own silos.")]
    [TestCase(TenantRegionLifecycleStatus.Offline, "Removing: Offline", "Step 2 of 3", "Next: Removed, taken automatically by the region's own silos.")]
    public void A_transitional_region_shows_the_step_it_has_reached_on_its_path(TenantRegionLifecycleStatus status, string phase, string figure, string next)
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", status));

        cut.WaitUntil(() =>
        {
            var progress = LifecycleCell(cut, "us-east").QuerySelector(".lt-progress");
            Assert.That(progress, Is.Not.Null);
            Assert.That(progress!.QuerySelector(".lt-progress__phase")!.TextContent, Is.EqualTo(phase));
            Assert.That(progress.QuerySelector(".lt-progress__figure")!.TextContent, Is.EqualTo(figure));
            Assert.That(progress.QuerySelector(".lt-progress__detail")!.TextContent, Is.EqualTo(next));
            Assert.That(progress.QuerySelector("[role=progressbar]")!.GetAttribute("aria-label"), Is.EqualTo("Residency change in us-east"));
            Assert.That(progress.TextContent, Does.Not.Contain("%"), "the cluster reports no fraction within a stage");
            Assert.That(LifecycleCell(cut, "eu-west").QuerySelector(".lt-progress"), Is.Null, "Online is steady");
        });
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Online)]
    [TestCase(TenantRegionLifecycleStatus.Removed)]
    [TestCase(TenantRegionLifecycleStatus.None)]
    public void A_steady_region_shows_no_step_and_the_section_follows_nothing(TenantRegionLifecycleStatus status)
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", status));

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-progress"), Is.Empty);
            Assert.That(cut.FindAll(".lt-tenancy-following"), Is.Empty);
            Assert.That(Time.ArmedTimers, Is.Zero, "nothing is part-way along a path, so nothing is followed");
        });
    }

    [Test]
    public void A_removed_region_is_followed_through_each_stage_announced_and_the_follow_stops_when_it_is_steady()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-following").TextContent, Is.EqualTo("Updating on its own while a region is part-way through a change.")));
        var reads = Reads();

        SetStatus("us-east", TenantRegionLifecycleStatus.Offline);
        AdvanceFollow();
        cut.WaitUntil(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Offline"));
            Assert.That(LifecycleCell(cut, "us-east").QuerySelector(".lt-progress__figure")!.TextContent, Is.EqualTo("Step 2 of 3"));
            Assert.That(Announcement(cut), Is.EqualTo("Region us-east of tenant acme is now Offline."));
        });

        SetStatus("us-east", TenantRegionLifecycleStatus.Removed);
        AdvanceFollow();
        cut.WaitUntil(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Removed"));
            Assert.That(cut.FindAll(".lt-progress"), Is.Empty, "Removed is steady");
            Assert.That(cut.FindAll(".lt-tenancy-following"), Is.Empty);
            Assert.That(Announcement(cut), Is.EqualTo("Region us-east of tenant acme is now Removed."));
        });

        Assert.Multiple(() =>
        {
            Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 0, TimeSpan.FromSeconds(10)), Is.True, "every region is steady, so the follow stops");
            Assert.That(Reads() - reads, Is.EqualTo(2), "one read per step, and none after");
        });
    }

    [Test]
    public void A_drain_that_does_not_move_is_read_less_and_less_often()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining));
        cut.WaitUntil(() => cut.Find(".lt-tenancy-following"));
        var reads = Reads();

        AdvanceFollow();
        ReadsReach(reads + 1);

        AdvanceFollow(TenancyRegionFollower.Interval);
        Assert.That(Reads(), Is.EqualTo(reads + 1), "a read that brought no change doubles the wait");
        Time.Advance(TenancyRegionFollower.Interval);
        ReadsReach(reads + 2);

        Assert.That(Pill(cut, "us-east"), Is.EqualTo("Draining"));
    }

    [Test]
    public void Leaving_the_section_ends_the_follow()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining));
        cut.WaitUntil(() => cut.Find(".lt-tenancy-following"));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "the section is following before it is left");

        cut.Instance.Dispose();

        Assert.That(Time.ArmedTimers, Is.Zero);
    }

    [Test]
    public void A_reading_taken_for_a_caller_who_has_since_signed_out_is_never_shown()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining));
        cut.WaitUntil(() => cut.Find(".lt-tenancy-following"));
        var reads = Reads();

        Auth.SignIn("someone-else@example.com");
        SetStatus("us-east", TenantRegionLifecycleStatus.Offline);
        AdvanceFollow();
        ReadsReach(reads + 1);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-tenancy-following"), Is.Empty));
        Assert.Multiple(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Draining"));
            Assert.That(Announcement(cut), Is.Empty);
            Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 0, TimeSpan.FromSeconds(10)), Is.True, "a reading taken for a caller who has signed out stops the follow");
        });
    }

    [Test]
    public void Following_keeps_a_residency_edit_in_progress()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining), ("ap-south", TenantRegionLifecycleStatus.None));
        cut.WaitUntil(() => Checkbox(cut, "ap-south"));
        Checkbox(cut, "ap-south").Change(true);

        SetStatus("us-east", TenantRegionLifecycleStatus.Offline);
        AdvanceFollow();

        cut.WaitUntil(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Offline"));
            Assert.That(Checkbox(cut, "ap-south").HasAttribute("checked"), Is.True);
            Assert.That(cut.Find(".lt-tenancy-actions .lt-tenancy-count").TextContent, Is.EqualTo("Not applied: add ap-south."));
        });
    }

    [Test]
    public void Applying_a_removal_starts_following_the_drain()
    {
        var cut = RenderRegions(resident: ["eu-west", "us-east"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Assert.That(Time.ArmedTimers, Is.Zero);

        Checkbox(cut, "us-east").Change(false);
        TenancyForms.Button(cut, "Apply residency").Click();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));
        TenancyForms.Button(cut, "Drain and apply").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Draining"));
            Assert.That(cut.Find(".lt-tenancy-following"), Is.Not.Null);
        });
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "a drain started by a residency edit is followed");
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Offline)]
    [TestCase(TenantRegionLifecycleStatus.Removed)]
    public void A_tenant_whose_regions_have_all_left_its_residency_reads_as_served_nowhere_never_as_unset(TenantRegionLifecycleStatus status)
    {
        var cut = RenderStatuses(("eu-west", status), ("us-east", TenantRegionLifecycleStatus.None));

        cut.WaitUntil(() =>
        {
            var residency = cut.FindAll("section.lt-tenancy-part")[1];
            Assert.That(residency.QuerySelector(".lt-dl__row dd")!.TextContent.Trim(), Is.EqualTo(TenancyFormat.NoResidentRegion));
            Assert.That(cut.FindAll(".lt-tenancy-unset"), Is.Empty, "residency is set, so it is not served in every region");
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[2].TextContent.Trim()), Is.All.EqualTo(TenancyFormat.NotServedLabel));
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Not in residency"));
            Assert.That(cut.Find(".lt-tenancy-warning").TextContent, Does.Contain("is not served anywhere:").And.Contain("every region has left it"));
        });
    }

    [Test]
    public void A_residency_for_a_tenant_already_served_nowhere_is_applied_from_the_primary_button()
    {
        var cut = RenderStatuses(("eu-west", TenantRegionLifecycleStatus.Removed), ("us-east", TenantRegionLifecycleStatus.None));
        cut.WaitUntil(() => Checkbox(cut, "us-east"));

        Checkbox(cut, "us-east").Change(true);

        cut.WaitUntil(() =>
        {
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.False, "nothing serves the tenant now, so nothing stops");
            Assert.That(TenancyForms.HasButton(cut, "Apply anyway and stop serving acme..."), Is.False);
            Assert.That(cut.Find(".lt-tenancy-still-unserved").TextContent, Does.Contain("served nowhere now, so this change stops nothing"));
            Assert.That(cut.FindAll(".lt-tenancy-served-nowhere"), Is.Empty);
        });
        TenancyForms.Button(cut, "Apply residency").Click();

        cut.WaitUntil(() => Assert.That(Cluster.Tenants["acme"].Regions.Single(region => region.RegionId == "us-east").Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning)));
    }

    [Test]
    public void Below_the_small_breakpoint_a_transitional_region_says_its_step_and_its_sheet_carries_the_bar()
    {
        var cut = RenderStatuses(LtBreakpoint.Compact, ("eu-west", TenantRegionLifecycleStatus.Online), ("us-east", TenantRegionLifecycleStatus.Draining));
        cut.WaitUntil(() =>
        {
            var lines = cut.FindAll(".lt-compact-row__secondary").Select(line => line.TextContent.Trim()).ToArray();
            Assert.That(lines, Has.Length.EqualTo(2));
            Assert.That(lines[0], Does.EndWith("Served, allowed"));
            Assert.That(lines[1], Does.EndWith("Not served, allowed, removing, step 1 of 3"));
        });

        cut.FindAll(".lt-table-list__open")[1].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-progress__figure").TextContent, Is.EqualTo("Step 1 of 3")));
    }

    private IRenderedComponent<TenancyRegions> RenderStatuses(params (string Region, TenantRegionLifecycleStatus Status)[] regions) =>
        RenderStatuses(null, regions);

    private IRenderedComponent<TenancyRegions> RenderStatuses(LtBreakpoint? band, params (string Region, TenantRegionLifecycleStatus Status)[] regions)
    {
        var tenant = Cluster.Tenants["acme"];
        tenant.Regions.Clear();
        foreach (var (region, status) in regions)
        {
            tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = region, Status = status, IsAllowed = true });
        }

        return RenderSection<TenancyRegions>(parameters => parameters.Add(section => section.TenantId, "acme"), band);
    }

    private void SetStatus(string region, TenantRegionLifecycleStatus status)
    {
        var regions = Cluster.Tenants["acme"].Regions;
        var index = regions.FindIndex(candidate => candidate.RegionId == region);
        regions[index] = regions[index] with { Status = status };
    }

    private int Reads() => Cluster.Calls.Count(call => call == nameof(FakeTenancyCluster.GetTenantRegionStatusAsync));

    private void ReadsReach(int expected) =>
        FollowBarriers.ReadsReach(Reads, expected, "the section");

    private void AdvanceFollow(TimeSpan? delta = null)
    {
        // The follow arms its next wait on a continuation once a read is taken in;
        // wait for that timer before moving the clock, never for wall-clock time.
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "the follow re-arms");
        Time.Advance(delta ?? TenancyRegionFollower.Interval);
    }

    private static AngleSharp.Dom.IElement Row(IRenderedComponent<TenancyRegions> cut, string region) =>
        cut.FindAll("tbody tr").Single(row => row.Children[0].TextContent.Trim() == region);

    private static AngleSharp.Dom.IElement LifecycleCell(IRenderedComponent<TenancyRegions> cut, string region) => Row(cut, region).Children[1];

    private static string Pill(IRenderedComponent<TenancyRegions> cut, string region) =>
        LifecycleCell(cut, region).QuerySelector(".lt-pill__text")!.TextContent.Trim();

    private static string Announcement(IRenderedComponent<TenancyRegions> cut) =>
        cut.Find(".lt-tenancy-section > .lt-visually-hidden[aria-live=polite]").TextContent.Trim();
}
