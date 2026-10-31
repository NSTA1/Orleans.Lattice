using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>C3.8 and C3.9: the snapshot trees are purged, and the status view is rebuilt, stopped and dropped.</summary>
public sealed partial class OffboardingJourney
{
    private static readonly string[] SnapshotTrees =
        [Names.Trees.FactoryOrdersSnapshot, Names.Trees.TelemetrySnapOnline, Names.Trees.LinesSnapOffline];

    /// <summary>
    /// C3.8: from each snapshot tree's Lifecycle tab olivia deletes it and then purges it at
    /// once; the purge's progress reaches every shard, and the trees leave Data.
    /// </summary>
    private static async Task PurgeSnapshotTreesAsync(PersonaSession olivia)
    {
        using var step = C3Step.Begin(8, "olivia deletes and purges the three snapshot trees");
        var content = olivia.Shell.Content;
        var first = true;
        foreach (var tree in SnapshotTrees)
        {
            await olivia.Shell.GotoAsync($"/cluster/trees/{tree}");
            await olivia.Shell.ExpectHeadingAsync(tree);
            await content.GetByRole(AriaRole.Tab, new() { Name = "Lifecycle", Exact = true }).ClickAsync();
            var delete = content.GetByRole(AriaRole.Button, new() { Name = "Delete tree...", Exact = true });
            await Expect(delete).ToBeVisibleAsync();
            if (first)
            {
                await step.ShotAsync(olivia, "lifecycle", $"{tree}'s Lifecycle tab: the tree is live");
            }

            await delete.ClickAsync();
            var confirmDelete = new ConfirmDialog(olivia.Page, "Delete tree");
            await confirmDelete.TypeAsync(tree);
            if (first)
            {
                await step.ShotAsync(olivia, "confirm-delete", $"Delete {tree}: olivia types its name");
            }

            await confirmDelete.ConfirmAsync("Delete");
            await Expect(olivia.Shell.Toasts.Filter(new() { HasText = "Tree deleted." }).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
            var purge = content.GetByRole(AriaRole.Button, new() { Name = "Purge now...", Exact = true });
            await Expect(purge).ToBeVisibleAsync();
            if (first)
            {
                await step.ShotAsync(olivia, "deleted", $"{tree} is soft-deleted and still recoverable; Purge now... is offered");
            }

            await purge.ClickAsync();
            var confirmPurge = new ConfirmDialog(olivia.Page, "Purge tree");
            await confirmPurge.TypeAsync(tree);
            if (first)
            {
                await step.ShotAsync(olivia, "confirm-purge", $"Purge {tree}: irreversible; olivia types its name");
            }

            await confirmPurge.ConfirmAsync("Purge");
            var bar = content.GetByRole(AriaRole.Progressbar, new() { Name = "Purge progress" });
            await Expect(bar).ToHaveAttributeAsync("aria-valuenow", "100", new() { Timeout = (float)OperationPatience.TotalMilliseconds });
            await Expect(content.Locator(".lt-progress__detail")).ToHaveTextAsync(new Regex(@"^(\d+) of \1 shards? purged\.$"));
            await Expect(purge).ToHaveCountAsync(0);
            await step.ShotAsync(olivia, $"purged-{tree}", $"{tree} is purged: every shard accounted for, and it cannot be recovered");
            first = false;
        }

        await olivia.Shell.GotoAsync("/data");
        await Expect(olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = Names.Trees.FactoryOrders, Exact = true }).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
        foreach (var tree in SnapshotTrees)
        {
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true })).ToHaveCountAsync(0);
        }

        await step.ShotAsync(olivia, "data-without-snapshots", "Data lists none of the purged snapshot trees");
        Coverage.Hit("use-case:cluster/purge");
        step.Complete();
    }

    /// <summary>
    /// C3.9: on <c>factory-orders</c>' Views tab olivia rebuilds <c>orders-by-status</c>, stops the
    /// rebuild while it runs, and then drops the view (G5, #5369).
    /// </summary>
    private static async Task RebuildStopAndDropViewAsync(PersonaSession olivia)
    {
        using var step = C3Step.Begin(9, "olivia rebuilds orders-by-status, stops the rebuild, and drops the view");
        var view = Names.Views.OrdersByStatus;
        var content = olivia.Shell.Content;
        await olivia.Shell.GotoAsync($"/data/{Names.Trees.FactoryOrders}?tab=views");
        var row = content.Locator("tbody tr").Filter(new() { HasText = view });
        await Expect(row).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await step.ShotAsync(olivia, "views-tab", "factory-orders' Views tab lists orders-by-status");

        await row.GetByRole(AriaRole.Button, new() { Name = "Rebuild", Exact = true }).ClickAsync();
        var confirmRebuild = new ConfirmDialog(olivia.Page, "Rebuild this view?");
        await confirmRebuild.TypeAsync(view);
        await step.ShotAsync(olivia, "confirm-rebuild", "Rebuild this view?: a new generation is built from every source key; olivia types its name");
        await confirmRebuild.ConfirmAsync("Rebuild");

        var progress = content.Locator("[data-lt-data-operation]");
        var stop = progress.GetByRole(AriaRole.Button, new() { Name = "Stop", Exact = true });
        await Expect(progress).ToContainTextAsync($"Rebuilding {view}", new() { Timeout = 60_000 });
        await Expect(stop).ToBeEnabledAsync();
        await step.ShotAsync(olivia, "rebuilding", "The rebuild runs on the cluster as a tracked operation, with Stop");
        await stop.ClickAsync();
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"Stopped the rebuild of {view}." })).ToHaveCountAsync(1, new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await step.ShotAsync(olivia, "rebuild-stopped", "olivia stops the rebuild; the view keeps its current generation");

        var drop = row.GetByRole(AriaRole.Button, new() { Name = "Drop view...", Exact = true });
        await Expect(drop).ToBeEnabledAsync(new() { Timeout = 60_000 });
        await drop.ClickAsync();
        var confirmDrop = new ConfirmDialog(olivia.Page, "Drop this view?");
        await confirmDrop.TypeAsync(view);
        await step.ShotAsync(olivia, "confirm-drop", "Drop this view?: its maintenance stops and every generation is deleted; the source is untouched");
        await confirmDrop.ConfirmAsync("Drop view");
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"Dropped view {view}." })).ToHaveCountAsync(1, new() { Timeout = 60_000 });
        await Expect(content.Locator("tbody tr").Filter(new() { HasText = view })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "view-dropped", "orders-by-status is dropped: factory-orders has no view");
        Coverage.Hit("use-case:data/view-rebuild-stop-drop");
        step.Complete();
    }
}
