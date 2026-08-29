using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The residency plan's two invariants, checked in the plan rather than only on
/// the wire: residency stays a subset of the operator-authorized allowed set,
/// and the last resident region can never be removed.
/// </summary>
[TestFixture]
public sealed class TenantResidencyPlanTests
{
    private static TenantResidencyPlan Plan(IReadOnlyList<ExplorerTenantRegion> regions)
    {
        var plan = new TenantResidencyPlan();
        plan.Reset(regions);
        return plan;
    }

    private static TenantRegionRow Row(TenantResidencyPlan plan, string regionId)
    {
        foreach (var row in plan.Rows)
        {
            if (string.Equals(row.RegionId, regionId, StringComparison.Ordinal))
            {
                return row;
            }
        }

        Assert.Fail($"the plan has no row for '{regionId}'");
        return default;
    }

    [Test]
    public void A_fresh_plan_mirrors_the_clusters_reading_and_is_not_dirty()
    {
        var plan = Plan(MyTenantSample.Regions());

        Assert.Multiple(() =>
        {
            Assert.That(plan.Rows, Has.Count.EqualTo(3));
            Assert.That(plan.ResidentCount, Is.EqualTo(1));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(1));
            Assert.That(plan.AllowedCount, Is.EqualTo(2), "only two of the three regions are allowed");
            Assert.That(plan.IsChanged, Is.False);
        });
    }

    [Test]
    public void The_two_sets_are_reported_separately_on_every_row()
    {
        var plan = Plan(MyTenantSample.Regions());

        Assert.Multiple(() =>
        {
            // Allowed but not resident: the caller may add it.
            Assert.That(Row(plan, "eastus").IsAllowed, Is.True);
            Assert.That(Row(plan, "eastus").IsResident, Is.False);

            // Allowed and resident.
            Assert.That(Row(plan, "westeurope").IsAllowed, Is.True);
            Assert.That(Row(plan, "westeurope").IsResident, Is.True);

            // Neither: outside the operator-authorized set entirely.
            Assert.That(Row(plan, "northeurope").IsAllowed, Is.False);
            Assert.That(Row(plan, "northeurope").IsResident, Is.False);
        });
    }

    [Test]
    public void A_region_outside_the_allowed_set_cannot_be_added()
    {
        var plan = Plan(MyTenantSample.Regions());

        var refusal = plan.Toggle("northeurope");

        Assert.Multiple(() =>
        {
            Assert.That(refusal, Is.EqualTo(TenantResidencyRefusal.NotAllowed));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(1), "the plan is untouched by a refused toggle");
            Assert.That(Row(plan, "northeurope").CanToggle, Is.False);
        });
    }

    [Test]
    public void The_last_planned_resident_region_cannot_be_removed()
    {
        var plan = Plan(MyTenantSample.SingleResidencyRegions());

        var refusal = plan.Toggle("westeurope");

        Assert.Multiple(() =>
        {
            Assert.That(refusal, Is.EqualTo(TenantResidencyRefusal.LastRegion));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(1));
            Assert.That(Row(plan, "westeurope").CanToggle, Is.False);
            Assert.That(
                Row(plan, "westeurope").Refusal,
                Is.EqualTo(TenantResidencyRefusal.LastRegion),
                "the row states the invariant before the server ever refuses");
        });
    }

    [Test]
    public void Removing_a_region_becomes_possible_once_a_second_one_is_planned()
    {
        var plan = Plan(MyTenantSample.SingleResidencyRegions());

        Assert.Multiple(() =>
        {
            Assert.That(plan.Toggle("eastus"), Is.EqualTo(TenantResidencyRefusal.None));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(2));
            Assert.That(
                Row(plan, "westeurope").Refusal,
                Is.EqualTo(TenantResidencyRefusal.None),
                "adding a second residency lifts the last-region block on the first");
            Assert.That(plan.Toggle("westeurope"), Is.EqualTo(TenantResidencyRefusal.None));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_resident_region_that_is_no_longer_allowed_can_be_left_but_not_re_entered()
    {
        // The allowed set was narrowed under a live residency, so the two sets
        // have genuinely diverged and the caller's remedy differs from the
        // ordinary not-allowed case.
        var plan = Plan(
        [
            MyTenantSample.Region("eastus", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
            MyTenantSample.Region("westeurope", ExplorerTenantRegionLifecycle.Online, isAllowed: false),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(2));
            Assert.That(plan.Toggle("westeurope"), Is.EqualTo(TenantResidencyRefusal.None), "leaving is allowed");
            Assert.That(
                plan.Refusal("westeurope"),
                Is.EqualTo(TenantResidencyRefusal.ResidentButNoLongerAllowed),
                "re-entering is not, and says so specifically");
        });
    }

    [Test]
    public void An_unknown_region_reports_not_allowed_rather_than_succeeding() =>
        Assert.That(
            Plan(MyTenantSample.Regions()).Refusal("mars"),
            Is.EqualTo(TenantResidencyRefusal.NotAllowed));

    [Test]
    public void A_toggle_marks_the_plan_changed_and_the_row_pending()
    {
        var plan = Plan(MyTenantSample.Regions());

        plan.Toggle("eastus");

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.True);
            Assert.That(Row(plan, "eastus").IsPlannedResident, Is.True);
            Assert.That(Row(plan, "eastus").IsResident, Is.False);
            Assert.That(Row(plan, "eastus").IsChanged, Is.True);
            Assert.That(Row(plan, "westeurope").IsChanged, Is.False);
        });
    }

    [Test]
    public void Revert_discards_the_pending_edit()
    {
        var plan = Plan(MyTenantSample.Regions());
        plan.Toggle("eastus");

        plan.Revert();

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsChanged, Is.False);
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(1));
            Assert.That(Row(plan, "eastus").IsPlannedResident, Is.False);
        });
    }

    [Test]
    public void Revert_on_an_unchanged_plan_is_a_no_op()
    {
        var plan = Plan(MyTenantSample.Regions());

        plan.Revert();

        Assert.That(plan.PlannedResidentCount, Is.EqualTo(1));
    }

    [Test]
    public void Planned_residency_is_materialised_in_the_clusters_row_order()
    {
        var plan = Plan(MyTenantSample.Regions());
        plan.Toggle("eastus");

        Assert.That(plan.PlannedResidency(), Is.EqualTo(new[] { "eastus", "westeurope" }));
    }

    [Test]
    public void Planned_residency_is_empty_only_when_nothing_is_planned()
    {
        var plan = new TenantResidencyPlan();

        Assert.Multiple(() =>
        {
            Assert.That(plan.PlannedResidency(), Is.Empty);
            Assert.That(plan.Rows, Is.Empty);
            Assert.That(plan.IsChanged, Is.False);
        });
    }

    [Test]
    public void Reset_replaces_the_reading_and_clears_a_pending_edit()
    {
        var plan = Plan(MyTenantSample.Regions());
        plan.Toggle("eastus");

        plan.Reset(MyTenantSample.DualResidencyRegions());

        Assert.Multiple(() =>
        {
            Assert.That(plan.Rows, Has.Count.EqualTo(2));
            Assert.That(plan.ResidentCount, Is.EqualTo(2));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(2));
            Assert.That(plan.IsChanged, Is.False);
        });
    }

    [Test]
    public void Reset_to_an_empty_reading_leaves_no_rows()
    {
        var plan = Plan(MyTenantSample.Regions());

        plan.Reset(Array.Empty<ExplorerTenantRegion>());

        Assert.Multiple(() =>
        {
            Assert.That(plan.Rows, Is.Empty);
            Assert.That(plan.AllowedCount, Is.EqualTo(0));
            Assert.That(plan.PlannedResidentCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void The_row_array_is_reused_across_toggles_so_a_render_allocates_nothing()
    {
        var plan = Plan(MyTenantSample.DualResidencyRegions());
        var before = plan.Rows;

        plan.Toggle("eastus");

        Assert.That(plan.Rows, Is.SameAs(before),
            "a toggle refills the existing row array rather than allocating a new one");
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var plan = new TenantResidencyPlan();

        Assert.Multiple(() =>
        {
            Assert.That(() => plan.Reset(null!), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => plan.Toggle(null!), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => plan.Refusal(null!), Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
