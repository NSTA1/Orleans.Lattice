using System.Globalization;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// A tree's resize page at <c>/cluster/trees/{tree}/resize</c>: capacity, review, typed
/// confirmation and the followed copy; and its undo, accept-then-poll.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
/// <param name="tree">The tree.</param>
internal sealed class ClusterResizePage(ShellPage shell, string tree) : A8PageObject(shell)
{
    /// <summary>The status line: a pill and a sentence.</summary>
    public ILocator Stage => Content.Locator(".lt-cluster-stage");

    /// <summary>The review section.</summary>
    public ILocator Review => Content.Locator(".lt-cluster-review");

    /// <summary>The "Undo the last resize..." button.</summary>
    public ILocator Undo => Content.Locator("button[data-lt-cluster=\"undo\"]");

    /// <summary>Sees the page idle, and reads the tree's capacity.</summary>
    public async Task<(int Leaf, int Internal)> ExpectIdleAsync()
    {
        await Expect(Stage).ToContainTextAsync("No resize is running.");
        Coverage.Hit("subroute:cluster/trees/{tree}/resize");
        return (await NumberAsync("Keys per leaf"), await NumberAsync("Children per internal node"));
    }

    /// <summary>Enters the new capacity and reviews it.</summary>
    /// <param name="from">The capacity now.</param>
    /// <param name="to">The new capacity.</param>
    public async Task ReviewAsync((int Leaf, int Internal) from, (int Leaf, int Internal) to)
    {
        var form = Content.GetByRole(AriaRole.Form, new() { Name = "Stage a resize" });
        await form.GetByLabel("Keys per leaf").FillAsync(to.Leaf.ToString(CultureInfo.InvariantCulture));
        await form.GetByLabel("Children per internal node").FillAsync(to.Internal.ToString(CultureInfo.InvariantCulture));
        await Button("Review").ClickAsync();
        await Expect(Review).ToContainTextAsync(
            $"Rebuild {tree} with {Count(to.Leaf)} keys per leaf and {Count(to.Internal)} children per internal node, from {Count(from.Leaf)} and {Count(from.Internal)}.");
    }

    /// <summary>Presses Resize... and types the tree's name.</summary>
    public async Task<ConfirmDialog> AskToResizeAsync()
    {
        await Button("Resize...").ClickAsync();
        var dialog = new ConfirmDialog(Page, "Resize tree");
        await dialog.TypeAsync(tree, "Resize");
        return dialog;
    }

    /// <summary>Confirms, and sees the copy start.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task StartAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Resize");
        await Expect(Toast("Resize started.").First).ToBeVisibleAsync();
    }

    /// <summary>Sees the copy running, its target named.</summary>
    /// <param name="to">The new capacity.</param>
    public Task ExpectRunningAsync((int Leaf, int Internal) to) =>
        Expect(Stage).ToContainTextAsync($"Resizing to {Count(to.Leaf)} keys per leaf and {Count(to.Internal)} children per node.");

    /// <summary>Follows the copy until the page shows the tree at <paramref name="to"/>, idle, with the undo offered.</summary>
    /// <param name="to">The new capacity.</param>
    public async Task ExpectSizeAsync((int Leaf, int Internal) to)
    {
        await Expect(Stage).ToContainTextAsync("No resize is running.", new() { Timeout = (float)Structural.TotalMilliseconds });
        await Expect(Term("Keys per leaf")).ToHaveTextAsync(Count(to.Leaf), new() { Timeout = (float)Structural.TotalMilliseconds });
        await Expect(Term("Children per internal node")).ToHaveTextAsync(Count(to.Internal));
        await Expect(Undo).ToBeVisibleAsync();
    }

    /// <summary>Presses "Undo the last resize..." and types the tree's name.</summary>
    public async Task<ConfirmDialog> AskToUndoAsync()
    {
        await Undo.ClickAsync();
        var dialog = new ConfirmDialog(Page, "Undo resize");
        await dialog.TypeAsync(tree, "Undo resize");
        return dialog;
    }

    /// <summary>Confirms the undo, and sees it accepted.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task UndoAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Undo resize");
        await Expect(Toast("Undo accepted.").Or(Toast("Resize undone.")).First).ToBeVisibleAsync();
    }

    private async Task<int> NumberAsync(string term)
    {
        await Expect(Term(term)).ToHaveTextAsync(new System.Text.RegularExpressions.Regex(@"^[\d,]+$"));
        return (int)ParseCount(await Term(term).TextContentAsync())!.Value;
    }
}
