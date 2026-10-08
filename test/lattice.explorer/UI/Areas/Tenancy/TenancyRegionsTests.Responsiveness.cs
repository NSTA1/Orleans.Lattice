using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

public sealed partial class TenancyRegionsTests
{
    [Test]
    public void A_slow_region_read_shows_loading_feedback_before_it_answers()
    {
        var read = Cluster.Hold(nameof(FakeTenancyCluster.GetTenantRegionStatusAsync));
        var cut = RenderRegions();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-skeleton[role=status] .lt-visually-hidden").TextContent, Is.EqualTo("Loading regions"));
            Assert.That(cut.FindAll("input[type=checkbox]"), Is.Empty);
        });
        cut.InvokeAsync(read.SetResult);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
    }

    [Test]
    public void A_confirmed_drain_reports_pending_feedback_without_claiming_it_has_started()
    {
        var cut = RenderRegions(resident: ["eu-west", "us-east"]);
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(false);
        TenancyForms.Button(cut, "Apply residency").Click();
        var save = Cluster.Hold(nameof(FakeTenancyCluster.SetResidencyAsync));

        TenancyForms.Button(cut, "Drain and apply").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(cut.Find(".lt-tenancy-section > .lt-progress").TextContent, Does.Contain("Waiting for the cluster"));
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Online"), "the cluster has not yet confirmed a drain");
            Assert.That(Services.GetToasts(), Is.Empty);
        });
        cut.InvokeAsync(save.SetResult);
        cut.WaitUntil(() =>
        {
            Assert.That(Pill(cut, "us-east"), Is.EqualTo("Draining"));
            Assert.That(cut.Find(".lt-tenancy-following + .lt-tenancy-note").TextContent,
                Does.Contain("not their age or a stall reason"));
            Assert.That(cut.FindAll(".lt-tenancy-section > .lt-progress"), Is.Empty);
        });
    }

    [Test]
    public void Editing_a_compact_region_updates_the_open_sheet_and_preview_without_a_cluster_read()
    {
        var cut = RenderRegions(resident: ["eu-west", "us-east"], band: LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(3)));
        var reads = Reads();
        cut.FindAll(".lt-table-list__open")[2].Click();

        cut.Find(".lt-dialog input[type=checkbox]").Change(false);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog input[type=checkbox]"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-dialog input[type=checkbox]").HasAttribute("checked"), Is.False);
            Assert.That(cut.Find(".lt-tenancy-preview").TextContent, Does.Contain("us-east starts draining"));
            Assert.That(Reads(), Is.EqualTo(reads), "a draft is projected locally, never fetched");
        });

        TenancyForms.Button(cut, "Reset").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-dialog input[type=checkbox]").HasAttribute("checked"), Is.True);
            Assert.That(cut.FindAll(".lt-tenancy-preview"), Is.Empty);
        });
    }

    [Test]
    public void Applying_residency_reports_pending_confirmation_then_updates_the_open_detail()
    {
        var cut = RenderRegions(band: LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(3)));
        cut.FindAll(".lt-table-list__open")[2].Click();
        cut.Find(".lt-dialog input[type=checkbox]").Change(true);
        var save = Cluster.Hold(nameof(FakeTenancyCluster.SetResidencyAsync));

        TenancyForms.Button(cut, "Apply residency").Click();

        cut.WaitUntil(() =>
        {
            var progress = cut.Find(".lt-tenancy-section > .lt-progress");
            Assert.That(progress.TextContent, Does.Contain("Applying residency for acme").And.Contain("Waiting for the cluster"));
            Assert.That(progress.QuerySelector("[role=progressbar]")!.HasAttribute("aria-valuenow"), Is.False);
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-dialog input[type=checkbox]").HasAttribute("disabled"), Is.True);
            Assert.That(Services.GetToasts(), Is.Empty, "a pending write is not a success");
        });

        cut.InvokeAsync(save.SetResult);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-section > .lt-progress"), Is.Empty);
            Assert.That(cut.Find(".lt-dialog .lt-pill__text").TextContent, Is.EqualTo("Provisioning"));
            Assert.That(cut.Find(".lt-dialog input[type=checkbox]").HasAttribute("checked"), Is.True);
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is adding us-east."));
        });
    }

    [Test]
    public void A_refused_save_removes_pending_feedback_and_keeps_the_draft()
    {
        var cut = RenderRegions();
        cut.WaitUntil(() => Checkbox(cut, "us-east"));
        Checkbox(cut, "us-east").Change(true);
        Cluster.Fail(nameof(FakeTenancyCluster.SetResidencyAsync), FakeTenancyCluster.Denied());

        TenancyForms.Button(cut, "Apply residency").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo(TenancyFailure.NotPermittedMessage));
            Assert.That(cut.FindAll(".lt-tenancy-section > .lt-progress"), Is.Empty);
            Assert.That(Checkbox(cut, "us-east").HasAttribute("checked"), Is.True);
            Assert.That(TenancyForms.Button(cut, "Apply residency").HasAttribute("disabled"), Is.False);
        });
    }
}
