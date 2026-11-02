using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// A tree's snapshot page at <c>/cluster/trees/{tree}/snapshot</c>: destination, mode and
/// optional sizing, review, typed confirmation, then the capture followed until it is idle.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
/// <param name="tree">The source tree.</param>
internal sealed class ClusterSnapshotPage(ShellPage shell, string tree) : A8PageObject(shell)
{
    /// <summary>The status line: a pill and a sentence.</summary>
    public ILocator Stage => Content.Locator(".lt-cluster-stage");

    /// <summary>The review section.</summary>
    public ILocator Review => Content.Locator(".lt-cluster-review");

    /// <summary>Sees the page idle.</summary>
    public async Task ExpectIdleAsync()
    {
        await Expect(Stage).ToContainTextAsync("No snapshot is running.");
        Coverage.Hit("subroute:cluster/trees/{tree}/snapshot");
    }

    /// <summary>Names the destination and chooses the online mode.</summary>
    /// <param name="destination">The new tree to copy into.</param>
    public async Task ComposeOnlineAsync(string destination)
    {
        var form = Content.GetByRole(AriaRole.Form, new() { Name = "Stage a snapshot" });
        await form.GetByLabel("Destination tree").FillAsync(destination);
        await form.GetByLabel("Mode").SelectOptionAsync(new SelectOptionValue { Label = "Online" });
        await Expect(form.GetByLabel("Destination tree")).ToHaveValueAsync(destination);
        await Expect(form.GetByLabel("Mode")).ToHaveValueAsync("Online");
    }

    /// <summary>Reviews the capture.</summary>
    /// <param name="destination">The new tree to copy into.</param>
    public async Task ReviewAsync(string destination)
    {
        await Button("Review").ClickAsync();
        await Expect(Review).ToContainTextAsync($"Copy {tree} into the new tree {destination}, online: the source keeps serving, and live writes are forwarded to the copy.");
    }

    /// <summary>Presses Snapshot... and types the source tree's name.</summary>
    public async Task<ConfirmDialog> AskToSnapshotAsync()
    {
        await Button("Snapshot...").ClickAsync();
        var dialog = new ConfirmDialog(Page, "Snapshot tree");
        await dialog.TypeAsync(tree, "Snapshot");
        return dialog;
    }

    /// <summary>Confirms, sees the capture start, and follows it until the page is idle again.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task SnapshotAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Snapshot");
        await Expect(Toast("Snapshot started.").First).ToBeVisibleAsync();
        await Expect(Stage).ToContainTextAsync("No snapshot is running.", new() { Timeout = (float)Structural.TotalMilliseconds });
        await Expect(Content.GetByRole(AriaRole.Form, new() { Name = "Stage a snapshot" })).ToBeVisibleAsync();
    }
}
