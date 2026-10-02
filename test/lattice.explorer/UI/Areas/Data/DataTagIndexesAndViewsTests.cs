using Microsoft.Extensions.DependencyInjection;
using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Explorer.UI.Transport;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

// These tests exercise the deprecated blocking tree-administration verbs (LATTICE0002) on purpose:
// they stay supported until the next major version.
#pragma warning disable LATTICE0002

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Tag indexes and Views tabs: status, members and navigation, and the
/// reconcile and rebuild actions - drawn only for a caller the capability probe
/// admits, run only after the destructive confirmation, and run on the cluster as
/// tracked operations (#4124) whose progress the tab follows and picks up again.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTagIndexesAndViewsTests : DataTestContext
{
    [Test]
    public void Tag_indexes_list_their_status_then_covered_trees_tags_and_members()
    {
        SeedIndex();

        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(1));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("by-region").And.Contain("Idle").And.Contain("2"));
        });

        Navigation.NavigateTo(cut.Find("tbody th a").GetAttribute("href")!);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index by-region"));
            Assert.That(cut.FindAll(".lt-data-list a").Select(link => link.TextContent), Is.EqualTo(new[] { "orders" }));
            Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("It also covers 1 tree you cannot see."));
            Assert.That(cut.FindAll(".lt-data-tag").Select(tag => tag.TextContent), Is.EqualTo(new[] { "eu", "us" }));
        });

        Navigation.NavigateTo(cut.FindAll(".lt-data-tag")[0].GetAttribute("href")!);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-data-tag")[0].GetAttribute("aria-current"), Is.EqualTo("true"));
            var memberLinks = cut.FindAll(".lt-data-entry tbody a").Select(link => link.GetAttribute("href")).ToArray();
            Assert.That(memberLinks, Does.Contain("data/orders?key=key%2F0001"));
            Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("A tree you cannot see"));
            Assert.That(cut.Markup, Does.Not.Contain("hidden-physical").And.Not.Contain("tag-by-region"));
        });
    }

    [Test]
    public void Reconcile_is_hidden_without_administrative_authority()
    {
        SeedIndex();

        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index by-region")));
        Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Reconcile"));
    }

    [Test]
    public void Reconcile_runs_only_after_the_destructive_confirmation()
    {
        SeedIndex();
        AdministeredTrees.Add("tag-by-region");
        Tracked.Script(
            TreeAdminOperationKinds.TagIndexReconcile,
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Probing, 1, 2, TreeAdminOperationUnits.Shards),
            Status(
                TreeAdminOperationKinds.TagIndexReconcile,
                LatticeOperationState.Succeeded,
                "Completed",
                result: new Dictionary<string, string> { [TreeAdminOperationResultKeys.KeysScanned] = "40", [TreeAdminOperationResultKeys.OrphanRowsRemoved] = "3" }));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Reconcile"), Is.EqualTo(1)));
        Button(cut, "Reconcile").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this tag index?")));
        Assert.That(Tracked.Started, Is.Empty);

        Confirm(cut, "by-region");

        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Started, Has.Count.EqualTo(1));
            Assert.That(TreeAdminOperationIds.Matches(Tracked.Started[0], TreeAdminOperationKinds.TagIndexReconcile, "by-region"), Is.True);
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Reconciling by-region").And.Contain("1 of 2 shards"));
            Assert.That(Button(cut, "Reconciling").HasAttribute("disabled"), Is.True);
        });

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "Reconciled by-region: scanned 40 keys and removed 3 orphaned rows." }));
            Assert.That(cut.FindAll("[data-lt-data-operation]"), Is.Empty);
        });
        Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == "ReconcileTagIndexAsync"), Is.Zero, "The blocking verb is never called.");
    }

    [Test]
    public void A_reconcile_running_from_another_tab_is_picked_up_and_its_failure_is_said()
    {
        SeedIndex();
        Tracked.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.TagIndexReconcile, "by-region"),
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Repairing, 7, null, TreeAdminOperationUnits.Keys),
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Failed, TreeAdminOperationPhases.Repairing, 7, null, TreeAdminOperationUnits.Keys, failure: "The membership tree is unavailable."));

        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Reconciling by-region").And.Contain("7 keys so far"));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("Reconciling"));
        });

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "The reconcile of by-region failed." }));
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("The membership tree is unavailable."));
        });
    }

    [Test]
    public void A_tree_with_no_tag_index_says_so()
    {
        Client.WithTree("orders");

        var cut = RenderAt("data/orders?tab=tag-indexes");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No tag indexes")));
    }

    [Test]
    public void Views_over_a_tree_show_their_lag_and_never_their_generation_tree()
    {
        SeedViews();

        var cut = RenderAt("data/orders?tab=views");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("by-status").And.Contain("Current"));
            Assert.That(Rows(cut)[1].TextContent, Does.Contain("totals").And.Contain("12 behind"));
            Assert.That(cut.Markup, Does.Not.Contain("view-gen-"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Rebuild"));
        });
    }

    [Test]
    public void Rebuild_and_reconcile_are_offered_to_an_administrator_and_each_is_confirmed()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Tracked.Script(TreeAdminOperationKinds.ViewRebuild, Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Succeeded, "Completed"));
        Tracked.Script(
            TreeAdminOperationKinds.ViewReconcile,
            Status(TreeAdminOperationKinds.ViewReconcile, LatticeOperationState.Succeeded, "Completed", result: new Dictionary<string, string> { [TreeAdminOperationResultKeys.DriftRepaired] = "true" }));
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Rebuild"), Is.EqualTo(2)));

        Rows(cut)[0].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Rebuild").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Rebuild this view?")));
        Confirm(cut, "by-status");
        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Started, Has.Count.EqualTo(1));
            Assert.That(TreeAdminOperationIds.Matches(Tracked.Started[0], TreeAdminOperationKinds.ViewRebuild, "by-status"), Is.True);
        });

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog").Count + cut.FindAll("tbody button[disabled]").Count, Is.Zero));
        Rows(cut)[1].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Reconcile").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this view?")));
        Confirm(cut, "totals");

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "Rebuilt by-status.", "Reconciled totals: drift was found and repaired." }));
            Assert.That(TreeAdminOperationIds.Matches(Tracked.Started[1], TreeAdminOperationKinds.ViewReconcile, "totals"), Is.True);
            Assert.That(cut.Markup, Does.Not.Contain("view-gen-"));
        });
        Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name is "RebuildViewAsync" or "ReconcileViewAsync"), Is.Zero, "The blocking verbs are never called.");
    }

    [Test]
    public void A_rebuild_started_in_another_tab_shows_its_progress_and_can_be_stopped()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        var operationId = TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "by-status");
        Tracked.Running(
            operationId,
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running, TreeAdminOperationPhases.Projecting, 10, 40, TreeAdminOperationUnits.Keys),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running, TreeAdminOperationPhases.Projecting, 20, 40, TreeAdminOperationUnits.Keys),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Cancelled, TreeAdminOperationPhases.Projecting, 20, 40, TreeAdminOperationUnits.Keys));

        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Rebuilding by-status").And.Contain("10 of 40 keys"));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("Rebuilding"));
            Assert.That(Rows(cut).SelectMany(row => row.QuerySelectorAll("button")).All(button => button.HasAttribute("disabled")), Is.True);
        });

        Button(cut, "Stop").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Cancelled, Is.EqualTo(new[] { operationId }));
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Cancelling").And.Contain("20 of 40 keys"));
        });

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "Stopped the rebuild of by-status." }));
            Assert.That(Rows(cut).SelectMany(row => row.QuerySelectorAll("button")).Any(button => button.HasAttribute("disabled")), Is.False);
        });
    }

    [Test]
    public void A_running_operation_for_another_view_is_not_taken_for_this_one()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Tracked.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "elsewhere"),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running, TreeAdminOperationPhases.Projecting, 1, 2, TreeAdminOperationUnits.Keys));

        var cut = RenderAt("data/orders?tab=views");

        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Rebuild" && !button.HasAttribute("disabled")), Is.EqualTo(2)));
        Assert.That(cut.FindAll("[data-lt-data-operation]"), Is.Empty);
    }

    [Test]
    public void A_facade_that_runs_no_tracked_operations_draws_no_action()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        var blockingOnly = Substitute.For<ILatticeTreeAdmin>();
        blockingOnly.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Admin.ProbeCapabilitiesAsync(call.Arg<string>(), call.Arg<CancellationToken>()));
        Services.AddKeyedSingleton(ShellFacades.Key, blockingOnly);

        var cut = RenderAt("data/orders?tab=views");

        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(2)));
        Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Rebuild").And.None.EqualTo("Reconcile"));
    }

    [Test]
    public void A_failed_rebuild_is_reported_in_a_fixed_sentence()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Tracked.Operations.StartViewRebuildAsync("by-status", Arg.Any<string?>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("view-gen-3 on t/acme broke"));
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(2)));
        Rows(cut)[0].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Rebuild").Click();
        Confirm(cut, "by-status");

        cut.WaitUntil(() => Assert.That(Toasts, Is.EqualTo(new[] { "The Explorer could not rebuild this view. Try again." })));
    }

    [Test]
    public void A_view_workspace_shows_its_own_status_and_a_tree_without_views_says_so()
    {
        SeedViews();
        Client.WithTree("plain");

        var cut = RenderAt("data/view-by-status?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("by-status")));

        Navigation.NavigateTo("data/plain?tab=views");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No views")));
    }

    [Test]
    public void Compact_views_carry_their_actions_in_the_detail_sheet()
    {
        SeedViews();
        AdministeredTrees.Add("orders");

        var cut = RenderAt("data/orders?tab=views", compact: true);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-compact-row__primary"), Has.Count.EqualTo(2)));

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog");
            Assert.That(sheet.QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Does.Contain("Rebuild").And.Contain("Reconcile"));
            Assert.That(sheet.QuerySelector("a.lt-btn")!.GetAttribute("href"), Is.EqualTo("data/view-by-status"));
        });
    }

    private IReadOnlyList<string> Toasts => Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message).ToArray();

    private void SeedIndex()
    {
        Client.WithTree("orders", keys: 3);
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders", "hidden-physical"];
        Client.Members[("by-region", "eu")] = [new TagMember { TreeId = "orders", Key = "key/0001" }, new TagMember { TreeId = "hidden-physical", Key = "x" }];
        Client.Members[("by-region", "us")] = [new TagMember { TreeId = "orders", Key = "key/0002" }];
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagIndexStatus { IndexName = "by-region", TreeId = "tag-by-region", ShardCount = 2, CoveredTrees = ["orders", "hidden-physical"], ReconcileIdle = true });
    }

    private void SeedViews()
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Views.Add(new ViewStateSummary { ViewName = "totals", SourceTreeId = "orders", IsAggregation = true });
        Client.Entries["view-by-status"] = new(StringComparer.Ordinal);
        Admin.GetViewStatusAsync("by-status", Arg.Any<CancellationToken>())
            .Returns(new TreeViewStatus { ViewName = "by-status", SourceTreeId = "orders", ApplyLag = 0, ActiveTreeId = "view-gen-1" });
        Admin.GetViewStatusAsync("totals", Arg.Any<CancellationToken>())
            .Returns(new TreeViewStatus { ViewName = "totals", SourceTreeId = "orders", ApplyLag = 12, ActiveTreeId = "view-gen-2", IsAggregation = true });
    }
}
