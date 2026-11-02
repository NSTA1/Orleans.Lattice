using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// A tree's reshard page at <c>/cluster/trees/{tree}/reshard</c>: target, review, typed
/// confirmation, then the cluster's status followed until the shard count settles.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
/// <param name="tree">The tree.</param>
internal sealed class ClusterReshardPage(ShellPage shell, string tree) : A8PageObject(shell)
{
    /// <summary>The status line: a pill and a sentence.</summary>
    public ILocator Stage => Content.Locator(".lt-cluster-stage");

    /// <summary>The review section.</summary>
    public ILocator Review => Content.Locator(".lt-cluster-review");

    /// <summary>Sees the page idle, and reads the tree's physical shard count.</summary>
    public async Task<int> ExpectIdleAsync()
    {
        await Expect(Stage).ToContainTextAsync("No reshard is running.");
        await Expect(Term("Physical shards")).ToHaveTextAsync(new Regex(@"^[\d,]+$"));
        Coverage.Hit("subroute:cluster/trees/{tree}/reshard");
        return (int)ParseCount(await Term("Physical shards").TextContentAsync())!.Value;
    }

    /// <summary>Enters the target and reviews it.</summary>
    /// <param name="from">The shard count now.</param>
    /// <param name="target">The target shard count.</param>
    public async Task ReviewAsync(int from, int target)
    {
        await Content.GetByLabel("Target physical shards").FillAsync(target.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Button("Review").ClickAsync();
        var verb = target < from ? "Shrink" : "Grow";
        await Expect(Review).ToContainTextAsync($"{verb} {tree} from {Count(from)} physical shards to {Count(target)} physical shards.");
        await Expect(Review).ToContainTextAsync("A reshard runs to its target once started.");
    }

    /// <summary>Presses Reshard... and types the tree's name.</summary>
    public async Task<ConfirmDialog> AskToReshardAsync()
    {
        await Button("Reshard...").ClickAsync();
        var dialog = new ConfirmDialog(Page, "Reshard tree");
        await dialog.TypeAsync(tree, "Reshard");
        return dialog;
    }

    /// <summary>Confirms, and sees the reshard start toward <paramref name="target"/>.</summary>
    /// <param name="dialog">The open confirmation.</param>
    /// <param name="target">The target shard count.</param>
    public async Task StartAsync(ConfirmDialog dialog, int target)
    {
        await dialog.ConfirmAsync("Reshard");
        await Expect(Toast("Reshard started.").First).ToBeVisibleAsync();
        await Expect(Stage).ToContainTextAsync($"Resharding to {Count(target)} physical shards.");
    }

    /// <summary>Follows the reshard until the page shows the tree at <paramref name="target"/> shards and idle.</summary>
    /// <param name="target">The target shard count.</param>
    public async Task ExpectCompleteAsync(int target)
    {
        await Expect(Term("Physical shards")).ToHaveTextAsync(Count(target), new() { Timeout = (float)Structural.TotalMilliseconds });
        await Expect(Stage).ToContainTextAsync("No reshard is running.", new() { Timeout = (float)Structural.TotalMilliseconds });
        Coverage.Hit("use-case:cluster/reshard");
    }
}
