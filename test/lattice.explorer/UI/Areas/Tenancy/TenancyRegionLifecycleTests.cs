using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The region lifecycle as the Tenancy area tells it, following
/// <c>ILatticeTenantRegionAdmin</c>: what each status means, when a tenant is
/// served nowhere, a residency plan that would leave no Online region, and the
/// residency picker's allowed-only source.
/// </summary>
[TestFixture]
public sealed class TenancyRegionLifecycleTests
{
    [Test]
    [TestCase(TenantRegionLifecycleStatus.Provisioning, "Waiting for a platform operator to promote it; this tenant is not served here until it is Online.")]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, "The tenant's existing data is being copied in; it is not served here until it is Online.")]
    [TestCase(TenantRegionLifecycleStatus.Online, "Serves this tenant.")]
    [TestCase(TenantRegionLifecycleStatus.Draining, "Being removed: the tenant's data here is draining and the region no longer serves it.")]
    [TestCase(TenantRegionLifecycleStatus.Offline, "Drained; no longer serves this tenant.")]
    [TestCase(TenantRegionLifecycleStatus.Removed, "Removed from the tenant's residency.")]
    public void Every_lifecycle_status_has_a_meaning(TenantRegionLifecycleStatus status, string meaning) =>
        Assert.That(TenancyFormat.RegionStatusMeaning(status), Is.EqualTo(meaning));

    [Test]
    public void A_region_outside_a_set_residency_says_it_does_not_serve_the_tenant()
    {
        Assert.That(TenancyFormat.RegionStatusMeaning(TenantRegionLifecycleStatus.None), Is.EqualTo("Outside the tenant's residency, so it does not serve this tenant."));
        Assert.That(TenancyFormat.RegionStatusMeaning(TenantRegionLifecycleStatus.Provisioning), Is.EqualTo(TenancyFormat.ProvisioningMeaning));
    }

    [Test]
    public void Residency_text_names_the_resident_regions_or_says_none_is_set()
    {
        Assert.That(TenancyFormat.ResidencyText([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.Draining)]), Is.EqualTo("eu-west"));
        Assert.That(TenancyFormat.ResidencyText([Region("eu-west", TenantRegionLifecycleStatus.None)]), Is.EqualTo(TenancyFormat.NoResidency));
        Assert.That(() => TenancyFormat.ResidencyText(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_tenant_is_served_nowhere_only_with_residency_and_no_online_region()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.IsServedNowhere([]), Is.False, "no residency: served everywhere");
            Assert.That(TenancyFormat.IsServedNowhere([Region("eu-west", TenantRegionLifecycleStatus.None)]), Is.False);
            Assert.That(TenancyFormat.IsServedNowhere([Region("eu-west", TenantRegionLifecycleStatus.Provisioning)]), Is.True);
            Assert.That(TenancyFormat.IsServedNowhere([Region("eu-west", TenantRegionLifecycleStatus.Backfilling), Region("us-east", TenantRegionLifecycleStatus.Draining)]), Is.True);
            Assert.That(TenancyFormat.IsServedNowhere([Region("eu-west", TenantRegionLifecycleStatus.Provisioning), Region("us-east", TenantRegionLifecycleStatus.Online)]), Is.False);
            Assert.That(() => TenancyFormat.IsServedNowhere(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_plan_reports_what_it_adds_and_removes_and_whether_residency_is_set()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.None), Region("ap-south", TenantRegionLifecycleStatus.Online)]);

        plan.Toggle("us-east");
        plan.Toggle("ap-south");

        Assert.Multiple(() =>
        {
            Assert.That(plan.HasResidency, Is.True);
            Assert.That(plan.Added, Is.EqualTo(new[] { "us-east" }));
            Assert.That(plan.Removed, Is.EqualTo(new[] { "ap-south" }));
            Assert.That(plan.LeavesNoOnlineRegion, Is.False, "eu-west stays Online");
        });
    }

    [Test]
    public void A_first_residency_leaves_no_online_region_because_an_added_region_starts_provisioning()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.None), Region("us-east", TenantRegionLifecycleStatus.None)]);
        Assert.Multiple(() =>
        {
            Assert.That(plan.HasResidency, Is.False);
            Assert.That(plan.LeavesNoOnlineRegion, Is.False, "an empty plan changes nothing");
        });

        plan.Toggle("eu-west");

        Assert.That((plan.LeavesNoOnlineRegion, plan.HasOnlineRegion), Is.EqualTo((true, false)));
    }

    [Test]
    public void Dropping_the_only_online_region_leaves_none()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.Provisioning)]);

        plan.Toggle("eu-west");

        Assert.Multiple(() =>
        {
            Assert.That(plan.Removed, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(plan.LeavesNoOnlineRegion, Is.True);
        });
    }

    [Test]
    public async Task The_chosen_region_source_suggests_only_the_chosen_regions_as_they_change()
    {
        var chosen = new List<string> { "us-east", "eu-west", "us-east" };
        var source = new TenancyChosenRegionSource(() => chosen);

        var all = await source.SuggestAsync(string.Empty, 10, CancellationToken.None);
        chosen.Remove("eu-west");
        var narrowed = await source.SuggestAsync("us", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(all.IsAvailable, Is.True);
            Assert.That(all.Items.Select(item => item.Value), Is.EquivalentTo(new[] { "us-east", "eu-west" }));
            Assert.That(all.Items.Select(item => item.Detail), Is.All.EqualTo(TenancyChosenRegionSource.Detail));
            Assert.That(narrowed.Items.Select(item => item.Value), Is.EqualTo(new[] { "us-east" }));
        });
        Assert.That(() => source.SuggestAsync(null!, 10, CancellationToken.None), Throws.ArgumentNullException);
    }

    [Test]
    public void A_region_with_no_status_reads_by_whether_the_tenant_has_residency()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.RegionStatusLabel(TenantRegionLifecycleStatus.None, hasResidency: false), Is.EqualTo("No residency set"));
            Assert.That(TenancyFormat.RegionStatusMeaning(TenantRegionLifecycleStatus.None, hasResidency: false), Is.EqualTo(TenancyFormat.NoResidencyMeaning));
            Assert.That(TenancyFormat.RegionStatusMeaning(TenantRegionLifecycleStatus.None), Is.EqualTo("Outside the tenant's residency, so it does not serve this tenant."));
            Assert.That(TenancyFormat.RegionStatusMeaning(TenantRegionLifecycleStatus.Provisioning), Is.EqualTo(TenancyFormat.ProvisioningMeaning));
        });
    }

    [Test]
    public void Every_region_serves_a_tenant_with_no_residency_and_only_an_online_one_serves_a_tenant_with_residency()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.IsServedIn(TenantRegionLifecycleStatus.None, hasResidency: false), Is.True);
            Assert.That(TenancyFormat.IsServedIn(TenantRegionLifecycleStatus.None, hasResidency: true), Is.False);
            Assert.That(TenancyFormat.IsServedIn(TenantRegionLifecycleStatus.Provisioning, hasResidency: true), Is.False);
            Assert.That(TenancyFormat.IsServedIn(TenantRegionLifecycleStatus.Online, hasResidency: true), Is.True);
            Assert.That(TenancyFormat.HasResidency([Region("eu-west", TenantRegionLifecycleStatus.None)]), Is.False);
            Assert.That(TenancyFormat.HasResidency([Region("eu-west", TenantRegionLifecycleStatus.Draining)]), Is.True, "a draining region still counts, as the tenancy engine counts it");
            Assert.That(() => TenancyFormat.HasResidency(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_plan_previews_each_region_and_names_the_regions_not_online_after_it()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.None)]);
        Assert.That((plan.Preview, plan.NotOnlineAfter), Is.EqualTo((Array.Empty<string>(), Array.Empty<string>())), "an unchanged plan previews nothing");

        plan.Toggle("us-east");
        plan.Toggle("eu-west");

        Assert.Multiple(() =>
        {
            Assert.That(plan.Preview, Is.EqualTo(new[]
            {
                "eu-west starts draining, and stops being served there.",
                "us-east joins the residency as Provisioning; it is served there once a platform operator promotes it to Online.",
            }));
            Assert.That(plan.NotOnlineAfter, Is.EqualTo(new[] { "us-east (added: starts Provisioning)" }));
            Assert.That(plan.LeavesNoOnlineRegion, Is.True);
            Assert.That(plan.HasOnlineRegion, Is.True, "eu-west is Online until the plan is applied");
        });
    }

    [Test]
    public void A_residency_survey_carries_its_count_and_whether_it_is_partial()
    {
        var survey = new TenancyResidencySurvey(3, IsPartial: true);

        Assert.That((survey.Unset, survey.IsPartial), Is.EqualTo((3, true)));
    }

    private static TenantRegionStatusDescriptor Region(string id, TenantRegionLifecycleStatus status) =>
        new() { RegionId = id, Status = status, IsAllowed = true };
}
