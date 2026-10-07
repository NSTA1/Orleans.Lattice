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
    [TestCase(TenantRegionLifecycleStatus.Provisioning, "Not served here until it is Online. Nothing in Lattice advances an added region: a platform operator of the hosting deployment promotes it once the tenant's data is in place.")]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, "Not served here until it is Online. Lattice copies no data into an added region; the hosting deployment fills it in, and a platform operator promotes it.")]
    [TestCase(TenantRegionLifecycleStatus.Online, "Serves this tenant.")]
    [TestCase(TenantRegionLifecycleStatus.Draining, "Awaiting confirmation from this region: its silos complete the drain after observing the change in sys-tenant-registry. If it stays Draining, check that the region is running and registry replication works in both directions. This view cannot confirm whether the remote region has observed the change; it may still serve the tenant until it does.")]
    [TestCase(TenantRegionLifecycleStatus.Offline, "Drained; no longer serves this tenant.")]
    [TestCase(TenantRegionLifecycleStatus.Removed, "Left the tenant's residency; does not serve this tenant.")]
    public void Every_lifecycle_status_has_a_meaning(TenantRegionLifecycleStatus status, string meaning) =>
        Assert.That(TenancyFormat.RegionStatusMeaning(status), Is.EqualTo(meaning));

    [Test]
    public void No_stage_claims_that_lattice_copies_or_drains_data()
    {
        foreach (var status in Enum.GetValues<TenantRegionLifecycleStatus>())
        {
            var meaning = TenancyFormat.RegionStatusMeaning(status);
            Assert.That(meaning, Does.Not.Contain("being copied in").And.Not.Contain("data here is draining"), status.ToString());
        }
    }

    [Test]
    public void Draining_explains_pending_confirmation_without_claiming_remote_observation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.DrainingMeaning, Does.Contain("Awaiting confirmation").And.Contain("sys-tenant-registry"));
            Assert.That(TenancyFormat.DrainingMeaning, Does.Contain("both directions").And.Contain("cannot confirm"));
            Assert.That(TenancyFormat.DrainingMeaning, Does.Contain("may still serve"));
        });
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Provisioning, 1, "Adding: Provisioning", "Next: Backfilling, when a platform operator of the hosting deployment promotes it.")]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, 2, "Adding: Backfilling", "Next: Online, when a platform operator of the hosting deployment promotes it.")]
    [TestCase(TenantRegionLifecycleStatus.Draining, 1, "Removing: Draining", "Next: Offline, taken automatically by the region's own silos.")]
    [TestCase(TenantRegionLifecycleStatus.Offline, 2, "Removing: Offline", "Next: Removed, taken automatically by the region's own silos.")]
    public void A_transitional_stage_is_a_step_of_three_on_its_path_with_what_comes_next(TenantRegionLifecycleStatus status, int step, string phase, string next)
    {
        var reached = TenancyRegionStep.For(status);

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.Not.Null);
            Assert.That((reached!.Step, reached.Phase, reached.Next), Is.EqualTo((step, phase, next)));
            Assert.That(TenancyRegionStep.Steps, Is.EqualTo(3));
            Assert.That(TenancyRegionStep.IsTransitional(status), Is.True);
            Assert.That(reached.Short, Is.EqualTo($"{(reached.IsRemoving ? "removing" : "adding")}, step {step} of 3"));
        });
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.None)]
    [TestCase(TenantRegionLifecycleStatus.Online)]
    [TestCase(TenantRegionLifecycleStatus.Removed)]
    public void A_steady_stage_has_no_step(TenantRegionLifecycleStatus status) =>
        Assert.That((TenancyRegionStep.For(status), TenancyRegionStep.IsTransitional(status)), Is.EqualTo(((TenancyRegionStep?)null, false)));

    [Test]
    public void A_step_bar_is_named_after_its_region() =>
        Assert.That(TenancyRegionStep.Label("us-east"), Is.EqualTo("Residency change in us-east"));

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Offline)]
    [TestCase(TenantRegionLifecycleStatus.Removed)]
    public void A_tenant_whose_regions_have_all_left_its_residency_has_residency_and_is_served_nowhere(TenantRegionLifecycleStatus status)
    {
        // TenantRecord.HasResidencyConfiguration counts any non-None status, and
        // TenantResidencyResolver then serves the tenant only where it is exactly
        // Online: such a tenant is served in no region, never in every region.
        IReadOnlyList<TenantRegionStatusDescriptor> regions = [Region("eu-west", status), Region("us-east", TenantRegionLifecycleStatus.None)];

        Assert.Multiple(() =>
        {
            Assert.That(TenancyFormat.HasResidency(regions), Is.True);
            Assert.That(TenancyFormat.IsServedNowhere(regions), Is.True);
            Assert.That(TenancyFormat.IsServedIn(TenantRegionLifecycleStatus.None, TenancyFormat.HasResidency(regions)), Is.False);
            Assert.That(TenancyFormat.ResidencyText(regions), Is.EqualTo(TenancyFormat.NoResidentRegion));
            Assert.That(TenancyFormat.ServedNowhereReason(regions), Does.StartWith("it has residency set and every region has left it."));
        });
    }

    [Test]
    public void A_tenant_waiting_on_a_promotion_is_served_nowhere_until_an_operator_promotes_one() =>
        Assert.That(
            TenancyFormat.ServedNowhereReason([Region("eu-west", TenantRegionLifecycleStatus.Provisioning)]),
            Is.EqualTo("it has residency set and none of its regions is Online yet. It is served again once a platform operator of the hosting deployment promotes one to Online."));

    [Test]
    public void A_plan_for_a_tenant_already_served_nowhere_does_not_stop_serving_it()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.Provisioning), Region("us-east", TenantRegionLifecycleStatus.None)]);

        plan.Toggle("us-east");

        Assert.Multiple(() =>
        {
            Assert.That(plan.LeavesNoOnlineRegion, Is.True);
            Assert.That(plan.StopsServing, Is.False, "nothing serves the tenant now, so nothing stops");
        });
    }

    [Test]
    public void A_plan_stops_serving_a_tenant_served_now_whether_everywhere_or_in_an_online_region()
    {
        var unset = new TenancyResidencyPlan();
        unset.Reset([Region("eu-west", TenantRegionLifecycleStatus.None)]);
        unset.Toggle("eu-west");

        var online = new TenancyResidencyPlan();
        online.Reset([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.None)]);
        online.Toggle("us-east");
        online.Toggle("eu-west");

        Assert.That((unset.StopsServing, online.StopsServing), Is.EqualTo((true, true)));
    }

    [Test]
    public void A_newer_reading_keeps_an_edit_in_progress_and_an_unchanged_plan_follows_it()
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.Draining)]);
        plan.Update([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.Offline)]);
        Assert.That((plan.IsChanged, plan.Rows[1].Status), Is.EqualTo((false, TenantRegionLifecycleStatus.Offline)));

        plan.Toggle("us-east");
        plan.Update([Region("eu-west", TenantRegionLifecycleStatus.Online), Region("us-east", TenantRegionLifecycleStatus.Removed)]);

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.True, "the edit survives the newer reading");
            Assert.That(plan.Added, Is.EqualTo(new[] { "us-east" }));
            Assert.That(plan.Rows[1].Status, Is.EqualTo(TenantRegionLifecycleStatus.Removed));
        });
        Assert.That(() => plan.Update(null!), Throws.ArgumentNullException);
    }

    [Test]
    [TestCase(0, 2)]
    [TestCase(1, 4)]
    [TestCase(3, 16)]
    [TestCase(4, 30)]
    [TestCase(100, 30)]
    public void A_quiet_follow_doubles_its_wait_up_to_a_bound(int quietReads, int seconds) =>
        Assert.That(TenancyRegionFollower.Delay(quietReads), Is.EqualTo(TimeSpan.FromSeconds(seconds)));

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

    [Test]
    public void The_only_region_planned_for_a_tenant_with_no_residency_can_be_unchecked_again()
    {
        // The cluster refuses an empty residency only while the tenant is resident
        // somewhere, so a region checked for a tenant resident nowhere is an edit
        // that can be undone at the checkbox (#4412).
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", TenantRegionLifecycleStatus.None)]);

        Assert.That(plan.Toggle("eu-west"), Is.Null);
        Assert.That((plan.IsChanged, plan.Rows[0].Refusal), Is.EqualTo((true, (string?)null)), "the lone planned region is not locked");

        Assert.Multiple(() =>
        {
            Assert.That(plan.Toggle("eu-west"), Is.Null);
            Assert.That(plan.Planned, Is.Empty);
            Assert.That(plan.IsChanged, Is.False, "the plan is back at the committed, empty residency");
            Assert.That(plan.IsResidentNow, Is.False);
        });
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Offline)]
    [TestCase(TenantRegionLifecycleStatus.Removed)]
    public void A_tenant_whose_regions_have_all_left_may_plan_a_region_and_take_it_back(TenantRegionLifecycleStatus left)
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", left), Region("us-east", TenantRegionLifecycleStatus.None)]);

        plan.Toggle("us-east");
        plan.Toggle("eu-west");

        Assert.Multiple(() =>
        {
            Assert.That(plan.Toggle("us-east"), Is.Null);
            Assert.That(plan.Toggle("eu-west"), Is.Null, "nothing is resident now, so the last planned region is not held");
            Assert.That((plan.Planned.Count, plan.IsChanged), Is.EqualTo((0, false)));
        });
    }

    [Test]
    [TestCase(TenantRegionLifecycleStatus.Provisioning)]
    [TestCase(TenantRegionLifecycleStatus.Backfilling)]
    [TestCase(TenantRegionLifecycleStatus.Online)]
    public void A_tenant_resident_in_a_region_keeps_its_last_planned_region(TenantRegionLifecycleStatus resident)
    {
        var plan = new TenancyResidencyPlan();
        plan.Reset([Region("eu-west", resident), Region("us-east", TenantRegionLifecycleStatus.None)]);
        plan.Toggle("us-east");
        plan.Toggle("eu-west");

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsResidentNow, Is.True);
            Assert.That(plan.Toggle("us-east"), Is.EqualTo(TenancyResidencyPlan.LastRegionRefusal), "emptying a resident tenant is what the cluster refuses");
            Assert.That(plan.Planned, Is.EqualTo(new[] { "us-east" }));
            Assert.That(plan.Rows[1].Refusal, Is.EqualTo(TenancyResidencyPlan.LastRegionRefusal));
        });
    }

    private static TenantRegionStatusDescriptor Region(string id, TenantRegionLifecycleStatus status) =>
        new() { RegionId = id, Status = status, IsAllowed = true };
}
