using System.Globalization;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// A tree's resumable Reshard and Snapshot pages as A12 drives them: stage, review, confirm
/// by typing the tree's name, and follow the operation to its end.
/// </summary>
/// <param name="shell">The shell of the session driving it.</param>
internal sealed class ClusterOperationPages(ShellPage shell)
{
    /// <summary>The reshard or snapshot page's status line, which reads "No reshard is running." once idle.</summary>
    /// <remarks>The stage line only: a running operation's progress bar carries its own live region in the same section.</remarks>
    public ILocator Stage => shell.Content.Locator("section[aria-label=\"Reshard status\"] > div.lt-cluster-stage, section[aria-label=\"Snapshot status\"] > div.lt-cluster-stage");

    /// <summary>The review section.</summary>
    public ILocator Review => shell.Content.GetByRole(AriaRole.Region, new() { Name = "Review" });

    /// <summary>Waits for the reshard page of <paramref name="tree"/> to show it idle at <paramref name="shards"/> physical shards.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="shards">Its physical shard count.</param>
    /// <remarks>
    /// The idle sentence is awaited first: a shrink reaches its target shard count before it
    /// completes (it still releases the retired shards' storage), and the staging form only
    /// renders once no reshard is in progress.
    /// </remarks>
    public async Task ExpectReshardIdleAsync(string tree, int shards)
    {
        await Expect(Stage).ToContainTextAsync("No reshard is running.", ClusterParts.PatientlyContaining);
        await Expect(ClusterParts.Term(shell, "Physical shards")).ToHaveTextAsync(Count(shards), ClusterParts.Patiently);
        await Expect(shell.Content.GetByLabel("Target physical shards")).ToBeVisibleAsync();
        await Expect(shell.Content).ToContainTextAsync(tree);
        Coverage.Hit("subroute:cluster/trees/{tree}/reshard");
    }

    /// <summary>Stages a reshard of <paramref name="tree"/> from <paramref name="from"/> to <paramref name="target"/> shards and asserts its review.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="from">The current shard count.</param>
    /// <param name="target">The target shard count.</param>
    public async Task ReviewReshardAsync(string tree, int from, int target)
    {
        await shell.Content.GetByLabel("Target physical shards").FillAsync(target.ToString(CultureInfo.InvariantCulture));
        await shell.Content.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        var verb = target > from ? "Grow" : "Shrink";
        await Expect(Review).ToContainTextAsync($"{verb} {tree} from {Count(from)} physical shards to {Count(target)} physical shards.");
        if (target < from)
        {
            await Expect(Review).ToContainTextAsync("Fewer shards lower the tree's write and point-read throughput ceiling");
        }
    }

    /// <summary>Opens the reshard confirmation and types the tree's name.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ComposeReshardConfirmationAsync(string tree)
    {
        await Review.GetByRole(AriaRole.Button, new() { Name = "Reshard..." }).ClickAsync();
        await ClusterParts.TypeConfirmationAsync(shell, "Reshard tree", tree, "Reshard");
    }

    /// <summary>Confirms the reshard and follows it until the tree is idle at <paramref name="target"/> shards.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="target">The target shard count.</param>
    public async Task RunReshardAsync(string tree, int target)
    {
        await ClusterParts.ConfirmAsync(shell, "Reshard tree", "Reshard");
        await Expect(ClusterParts.Term(shell, "Physical shards")).ToHaveTextAsync(Count(target), ClusterParts.Patiently);
        await ExpectReshardIdleAsync(tree, target);
    }

    /// <summary>Opens the snapshot page of <paramref name="tree"/> idle.</summary>
    /// <param name="tree">The source tree.</param>
    public async Task OpenSnapshotAsync(string tree)
    {
        await shell.GotoAsync($"/cluster/trees/{tree}/snapshot");
        await Expect(Stage).ToContainTextAsync("No snapshot is running.", ClusterParts.PatientlyContaining);
        await Expect(shell.Content.GetByLabel("Destination tree")).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/trees/{tree}/snapshot");
    }

    /// <summary>Stages a snapshot into <paramref name="destination"/> and asserts its review.</summary>
    /// <param name="tree">The source tree.</param>
    /// <param name="destination">The new tree.</param>
    /// <param name="offline">Whether the snapshot is offline.</param>
    /// <param name="leafKeys">Keys per leaf, or <see langword="null"/> for the library default.</param>
    /// <param name="children">Children per internal node, or <see langword="null"/>.</param>
    public async Task ComposeSnapshotAsync(string tree, string destination, bool offline, int? leafKeys, int? children)
    {
        await shell.Content.GetByLabel("Destination tree").FillAsync(destination);
        await shell.Content.GetByLabel("Mode").SelectOptionAsync(new SelectOptionValue { Label = offline ? "Offline" : "Online" });
        if (leafKeys is { } leaf)
        {
            await shell.Content.GetByLabel("Keys per leaf (optional)").FillAsync(leaf.ToString(CultureInfo.InvariantCulture));
        }

        if (children is { } internalChildren)
        {
            await shell.Content.GetByLabel("Children per internal node (optional)").FillAsync(internalChildren.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Reviews the composed snapshot.</summary>
    /// <param name="tree">The source tree.</param>
    /// <param name="destination">The new tree.</param>
    /// <param name="offline">Whether the snapshot is offline.</param>
    public async Task ReviewSnapshotAsync(string tree, string destination, bool offline)
    {
        await shell.Content.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Expect(Review).ToContainTextAsync($"Copy {tree} into the new tree {destination}, " + (offline
            ? "offline: the source stops serving reads and writes until the copy finishes."
            : "online: the source keeps serving, and live writes are forwarded to the copy."));
        await Review.GetByRole(AriaRole.Button, new() { Name = "Snapshot..." }).ClickAsync();
        await ClusterParts.TypeConfirmationAsync(shell, "Snapshot tree", tree, "Snapshot");
    }

    /// <summary>Confirms the snapshot and follows it until the page is idle again.</summary>
    public async Task RunSnapshotAsync()
    {
        await ClusterParts.ConfirmAsync(shell, "Snapshot tree", "Snapshot");
        await Expect(ClusterParts.Toast(shell, "Snapshot started.")).ToHaveCountAsync(1);
        await Expect(Stage).ToContainTextAsync("No snapshot is running.", ClusterParts.PatientlyContaining);
    }

    /// <summary>Opens <paramref name="tree"/>'s Configuration tab and asserts its leaf and node capacity.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="leafKeys">Keys per leaf.</param>
    /// <param name="children">Children per internal node.</param>
    public async Task ExpectSizingAsync(string tree, int leafKeys, int children)
    {
        await shell.GotoAsync($"/cluster/trees/{tree}?tab=configuration");
        await shell.ExpectHeadingAsync(tree);
        await Expect(ClusterParts.Term(shell, "Keys per leaf")).ToHaveTextAsync(Count(leafKeys));
        await Expect(ClusterParts.Term(shell, "Children per internal node")).ToHaveTextAsync(Count(children));
        Coverage.Hit("query:cluster.tab");
    }

    private static string Count(int value) => value.ToString("N0", CultureInfo.GetCultureInfo("en-US"));
}
