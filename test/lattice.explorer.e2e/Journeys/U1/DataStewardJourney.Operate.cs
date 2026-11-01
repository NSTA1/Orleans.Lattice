using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>Steps 12 and 13: the view and the tag index over the orders, the metrics, and what is not hers to do.</summary>
public sealed partial class DataStewardJourney
{
    /// <summary>The status a tag index's keys are shown for.</summary>
    private const string ShippedTag = "shipped";

    private async Task CheckViewsTagIndexesAndMetricsAsync()
    {
        var orders = Names.Trees.FactoryOrders;
        var view = Names.Views.OrdersByStatus;
        var content = _priya.Shell.Content;

        // The view over the orders reflects her fixes: order 50 shipped, order 101 never lost.
        await OpenViewEntryAsync(view, Names.Orders.Order50);
        await Expect(_tree.EntryValue).ToContainTextAsync("shipped", new() { Timeout = 60_000 });
        await OpenViewEntryAsync(view, Names.Orders.Order101);
        await Expect(_tree.EntryValue).ToContainTextAsync(StoryData.SeededStatus(101));
        await Expect(_tree.EntryValue).Not.ToContainTextAsync(StoryData.InvalidStatus);
        await CheckpointAsync("12", "view-reflects-fixes", "orders-by-status reflects her edits, and not the refused one");

        await _priya.Shell.GotoAsync($"/data/{orders}?tab=views");
        await Expect(_tree.Tab("Views")).ToHaveAttributeAsync("aria-selected", "true");
        var row = content.Locator("tbody tr.lt-table__row").Filter(new() { HasText = view });
        await row.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        var confirm = await _tree.TypeConfirmationAsync("Reconcile this view?", view);
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        await Expect(_priya.Shell.Toasts.Filter(new() { HasTextRegex = new Regex("^\\s*Reconciled " + Regex.Escape(view)) }))
            .Not.ToHaveCountAsync(0, new() { Timeout = 180_000 });
        Coverage.Hit("tab:data/views");
        Coverage.Hit("use-case:data/view-reconcile");
        await CheckpointAsync("12", "view-reconciled", "Reconcile... runs on the cluster and reports its outcome");

        // The tag index the line feed owns: the shipped orders, a page at a time.
        await _tree.OpenTabAsync("Tag indexes", "tag-indexes");
        await content.GetByRole(AriaRole.Link, new() { Name = Names.TagIndexes.OrderStatus, Exact = true }).First.ClickAsync();
        await Expect(_priya.Page).ToHaveURLAsync(new Regex("[?&]index=" + Regex.Escape(Names.TagIndexes.OrderStatus) + "(&|$)"));
        Coverage.Hit("query:data.index");
        var tag = content.Locator("a.lt-data-tag", new() { HasTextRegex = Exactly(ShippedTag) });
        await tag.ClickAsync();
        await Expect(_priya.Page).ToHaveURLAsync(new Regex("[?&]tag=" + ShippedTag + "(&|$)"));
        Coverage.Hit("query:data.tag");
        var members = content.GetByRole(AriaRole.Table, new() { Name = "Keys tagged " + ShippedTag }).Locator("tbody tr.lt-table__row");
        await Expect(members).ToHaveCountAsync(50, new() { Timeout = 30_000 });
        var loadMore = content.GetByRole(AriaRole.Button, new() { Name = "Load more keys", Exact = true });
        await loadMore.ClickAsync();
        await Expect(members).Not.ToHaveCountAsync(50, new() { Timeout = 30_000 });
        Coverage.Hit("use-case:data/tag-index-keys");
        await CheckpointAsync("12", "tag-members", "order-status: the shipped orders, and Load more keys");

        await content.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        confirm = await _tree.TypeConfirmationAsync("Reconcile this tag index?", Names.TagIndexes.OrderStatus);
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        await new KnownDefect("#5613", "a tag-index reconcile by a caller with Admin on the index tree fails in its repair phase: Delete denied on tag-order-status").ReproducesAsync(async () =>
        {
            var operation = content.Locator("[data-lt-data-operation] .lt-operation-progress[data-lt-operation-state=Failed]");
            await Expect(operation).ToContainTextAsync("is not authorized to perform Delete on tree 'tag-order-status'", new() { Timeout = 120_000 });
            await Expect(_priya.Shell.Toasts.Filter(new() { HasTextRegex = Exactly($"The reconcile of {Names.TagIndexes.OrderStatus} failed.") }))
                .Not.ToHaveCountAsync(0, new() { Timeout = 30_000 });
        });
        await CheckpointAsync("12", "reconcile-refused", "A tag-index reconcile fails in its repair phase: Delete denied on the index tree (#5613)");

        await _tree.OpenTabAsync("Metrics", "metrics");
        var hotness = content.GetByRole(AriaRole.Table, new() { Name = "Shard hotness" });
        await Expect(hotness).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(hotness.GetByRole(AriaRole.Columnheader, new() { Name = "Live keys" })).ToBeVisibleAsync();
        await Expect(hotness.GetByRole(AriaRole.Columnheader, new() { Name = "Operations per second" })).ToBeVisibleAsync();
        await Expect(hotness.Locator("tbody tr").First).ToBeVisibleAsync();
        Coverage.Hit("use-case:data/metrics");
        await CheckpointAsync("12", "metrics", "Metrics: each shard's live keys and operations per second");
    }

    private async Task SeeWhatIsNotOfferedAsync()
    {
        // Cluster admits her on her archive lifecycle grant (#5588), but she holds no tree
        // lifecycle on the orders, so their resize page explains why and offers no Resize.
        await _priya.Shell.GotoAsync($"/cluster/trees/{Names.Trees.FactoryOrders}/resize");
        await Expect(_priya.Shell.Content.GetByText("Resizing needs the TreeLifecycle grant, which your identity does not hold over this tree."))
            .ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(_priya.Shell.Content.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^Resize") })).ToHaveCountAsync(0);
        await CheckpointAsync("13", "no-resize", "Cluster offers her no resize of the orders");

        // Backups: whatever she may see there, nothing offers her a restore.
        await _priya.Shell.GotoAsync("/backups");
        await Expect(_priya.Shell.Heading).ToBeVisibleAsync();
        await Expect(_priya.Shell.Content.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^Restore") })).ToHaveCountAsync(0);
        await Expect(_priya.Shell.Content.GetByRole(AriaRole.Link, new() { NameRegex = new Regex("^Restore") })).ToHaveCountAsync(0);
        await CheckpointAsync("13", "no-restore", "Backups offers her no restore");
    }

    /// <summary>Opens <paramref name="key"/> in the view <paramref name="view"/>'s workspace.</summary>
    private async Task OpenViewEntryAsync(string view, string key)
    {
        await _priya.Shell.GotoAsync($"/data/view-{view}?key={Uri.EscapeDataString(key)}");
        await Expect(_tree.Heading).ToHaveTextAsync(view);
        await Expect(_tree.Entry.Locator(".lt-data-entry__key")).ToHaveTextAsync(key, new() { Timeout = 60_000 });
    }
}
