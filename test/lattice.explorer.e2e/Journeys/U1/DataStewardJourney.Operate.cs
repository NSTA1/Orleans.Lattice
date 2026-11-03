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

    private static readonly KnownDefect ViewDrainLags = new KnownDefect("#5569", "under replay-permit starvation the orders-by-status view's drain lags its source by minutes, so the view does not yet reflect priya's edit of order/000050");

    /// <summary>
    /// Waits for priya's newest tree-administration operation of <paramref name="kind"/> (one
    /// she has just started from the panel) to succeed, while it advances: it fails when the
    /// operation stalls for three minutes, ends other than Succeeded, or runs past fifteen
    /// minutes. The panel reports the outcome once the operation ends.
    /// </summary>
    private async Task WaitForOperationAsync(string kind, string what)
    {
        var admin = ContractFacades.Of(_context, Cast.Personas.Priya.Id).TreeAdmin;
        string? operationId = null;
        await ContractWait.UntilAsync($"priya's {what} to be listed", async () =>
        {
            var page = await admin.ListTreeAdminOperationsAsync(new Api.Operations.LatticeOperationListRequest { PageSize = 20 });
            operationId = page.Operations.FirstOrDefault(operation => operation.Kind == kind)?.OperationId;
            return operationId is not null;
        }, PanelSettled.Bound);

        await ContractWait.SucceededWhileAdvancingAsync(
            what,
            () => admin.GetTreeAdminOperationStatusAsync(operationId!),
            stall: TimeSpan.FromMinutes(3),
            ceiling: TimeSpan.FromMinutes(15));
    }

    /// <summary>
    /// Waits for the open view entry to contain <paramref name="text"/>. The view materialises its
    /// source asynchronously, so when the panel does not show it the server decides: once the
    /// view's own tree holds it, the page is reloaded and the panel must show it; if the view still
    /// lacks it after <see cref="PanelSettled.Bound"/>, that is #5569's drain lag, recorded as a
    /// known defect, and this returns <see langword="false"/>.
    /// </summary>
    private async Task<bool> ViewEntryReflectsAsync(string view, string key, string text)
    {
        try
        {
            await Expect(_tree.EntryValue).ToContainTextAsync(text, new() { Timeout = 60_000 });
            return true;
        }
        catch (PlaywrightException)
        {
        }

        var admin = ContractFacades.Of(_context);
        string held = string.Empty;
        try
        {
            await ContractWait.UntilAsync($"the {view} view to hold {text} at {key}", async () =>
            {
                var status = await admin.TreeAdmin.GetViewStatusAsync(view);
                var value = await ContractCalls.GetAsync(admin.Data, status.ActiveTreeId, key);
                held = value is null ? string.Empty : System.Text.Encoding.UTF8.GetString(value);
                return held.Contains(text, StringComparison.Ordinal);
            }, PanelSettled.Bound);
        }
        catch (System.TimeoutException)
        {
            await ViewDrainLags.ReproducesAsync(() =>
            {
                Assert.That(held, Does.Not.Contain(text), $"the {view} view still lacks the change at {key}");
                return Task.CompletedTask;
            });
            return false;
        }

        await _priya.Page.ReloadAsync();
        await _tree.ExpectEntryAsync(key);
        await Expect(_tree.EntryValue).ToContainTextAsync(text);
        return true;
    }

    private async Task CheckViewsTagIndexesAndMetricsAsync()
    {
        var orders = Names.Trees.FactoryOrders;
        var view = Names.Views.OrdersByStatus;
        var content = _priya.Shell.Content;

        // The view over the orders reflects her fixes: order 50 shipped, order 101 never lost.
        await OpenViewEntryAsync(view, Names.Orders.Order50);
        var reflected = await ViewEntryReflectsAsync(view, Names.Orders.Order50, "shipped");
        await OpenViewEntryAsync(view, Names.Orders.Order101);
        await Expect(_tree.EntryValue).ToContainTextAsync(StoryData.SeededStatus(101));
        await Expect(_tree.EntryValue).Not.ToContainTextAsync(StoryData.InvalidStatus);
        await CheckpointAsync("12", reflected ? "view-reflects-fixes" : "view-lagging", reflected
            ? "orders-by-status reflects her edits, and not the refused one"
            : "orders-by-status has not yet materialised her edit of order/000050 (#5569), and never holds the refused one");

        await _priya.Shell.GotoAsync($"/data/{orders}?tab=views");
        await Expect(_tree.Tab("Views")).ToHaveAttributeAsync("aria-selected", "true");
        var row = content.Locator("tbody tr.lt-table__row").Filter(new() { HasText = view });
        await row.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        var confirm = await _tree.TypeConfirmationAsync("Reconcile this view?", view);
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();

        // The reconcile runs on the cluster; the panel toasts once it ends. Under #5569 it can run
        // for minutes, so the wait follows the operation itself while it advances.
        await WaitForOperationAsync(Api.TreeAdmin.TreeAdminOperationKinds.ViewReconcile, $"the reconcile of {view}");
        await Expect(_priya.Shell.Toasts.Filter(new() { HasTextRegex = new Regex("^\\s*Reconciled " + Regex.Escape(view)) }))
            .Not.ToHaveCountAsync(0, new() { Timeout = 60_000 });
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
        await PanelSettled.ExpectDrawnAsync(members.Nth(49), _tree.PanelFailed, $"the keys tagged {ShippedTag}");
        await Expect(members).ToHaveCountAsync(50);
        var loadMore = content.GetByRole(AriaRole.Button, new() { Name = "Load more keys", Exact = true });
        await loadMore.ClickAsync();
        await PanelSettled.ExpectDrawnAsync(members.Nth(50), _tree.PanelFailed, $"the next keys tagged {ShippedTag}");
        Coverage.Hit("use-case:data/tag-index-keys");
        await CheckpointAsync("12", "tag-members", "order-status: the shipped orders, and Load more keys");

        await content.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        confirm = await _tree.TypeConfirmationAsync("Reconcile this tag index?", Names.TagIndexes.OrderStatus);
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();

        // Admin on the index tree admits the reconcile end to end: its repairs are index
        // maintenance under system origin, whatever repair work the estate presents (#5613).
        await WaitForOperationAsync(Api.TreeAdmin.TreeAdminOperationKinds.TagIndexReconcile, $"the reconcile of {Names.TagIndexes.OrderStatus}");
        var reconciled = _priya.Shell.Toasts.Filter(new() { HasTextRegex = new Regex("^Reconciled " + Regex.Escape(Names.TagIndexes.OrderStatus) + ": scanned ") });
        await Expect(reconciled.First).ToBeVisibleAsync(new() { Timeout = 60_000 });
        Coverage.Hit("use-case:data/tag-index-reconcile");
        await CheckpointAsync("12", "reconciled", "The tag-index reconcile runs on the cluster and reports what it scanned and removed");

        await _tree.OpenTabAsync("Metrics", "metrics");
        var hotness = content.GetByRole(AriaRole.Table, new() { Name = "Shard hotness" });
        await PanelSettled.ExpectDrawnAsync(hotness, _tree.PanelFailed, "the metrics");
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
            .ToBeVisibleAsync(new() { Timeout = PanelSettled.BoundMs });
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
        await _tree.ExpectEntryAsync(key);
    }
}
