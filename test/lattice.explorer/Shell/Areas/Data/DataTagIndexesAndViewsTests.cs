using Microsoft.Extensions.DependencyInjection;
using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// The Tag indexes and Views tabs: status, members and navigation, and the
/// reconcile and rebuild actions - drawn only for a caller the capability probe
/// admits, and run only after the destructive confirmation.
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
        Admin.ReconcileTagIndexAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagReconcileReport { IndexName = "by-region", TreeId = "tag-by-region", KeysScanned = 40, OrphanRowsRemoved = 3 });
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Reconcile"), Is.EqualTo(1)));
        Button(cut, "Reconcile").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this tag index?")));
        Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.ReconcileTagIndexAsync)), Is.Zero);

        Confirm(cut, "by-region");

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.ReconcileTagIndexAsync)), Is.EqualTo(1));
            Assert.That(Services.GetRequiredService<Orleans.Lattice.Explorer.Shell.Design.Components.LtToastService>().Toasts.Single().Message,
                Is.EqualTo("Reconciled by-region: scanned 40 keys and removed 3 orphaned rows."));
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
        Admin.RebuildViewAsync("by-status", Arg.Any<CancellationToken>())
            .Returns(new TreeViewStatus { ViewName = "by-status", SourceTreeId = "orders", ApplyLag = 0, ActiveTreeId = "view-gen-9" });
        Admin.ReconcileViewAsync("totals", Arg.Any<CancellationToken>())
            .Returns(new TreeViewReconcileResult { ViewName = "totals", SourceTreeId = "orders", DriftRepaired = true });
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Rebuild"), Is.EqualTo(2)));

        Rows(cut)[0].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Rebuild").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Rebuild this view?")));
        Confirm(cut, "by-status");
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.RebuildViewAsync)), Is.EqualTo(1)));

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog").Count + cut.FindAll("tbody button[disabled]").Count, Is.Zero));
        Rows(cut)[1].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Reconcile").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this view?")));
        Confirm(cut, "totals");

        cut.WaitUntil(() =>
        {
            var toasts = Services.GetRequiredService<Orleans.Lattice.Explorer.Shell.Design.Components.LtToastService>().Toasts.Select(toast => toast.Message).ToArray();
            Assert.That(toasts, Is.EqualTo(new[] { "Rebuilt by-status.", "Reconciled totals: drift was found and repaired." }));
            Assert.That(cut.Markup, Does.Not.Contain("view-gen-9"));
        });
    }

    [Test]
    public void A_failed_rebuild_is_reported_in_a_fixed_sentence()
    {
        SeedViews();
        AdministeredTrees.Add("orders");
        Admin.RebuildViewAsync("by-status", Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("view-gen-3 on t/acme broke"));
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(2)));
        Rows(cut)[0].QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Rebuild").Click();
        Confirm(cut, "by-status");

        cut.WaitUntil(() => Assert.That(
            Services.GetRequiredService<Orleans.Lattice.Explorer.Shell.Design.Components.LtToastService>().Toasts.Single().Message,
            Is.EqualTo("The Explorer could not rebuild this view. Try again.")));
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
