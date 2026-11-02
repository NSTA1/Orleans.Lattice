using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// The tree list at <c>/cluster/trees</c>, which owns the visible control of the
/// <c>cluster.reshard-tree</c> palette command and its "Reshard a tree" dialog.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class ClusterTreesPage(ShellPage shell) : A8PageObject(shell)
{
    /// <summary>The "Reshard tree..." button.</summary>
    public ILocator ReshardButton => Content.Locator("button[data-lt-command=\"cluster.reshard-tree\"]");

    /// <summary>The "Reshard a tree" dialog.</summary>
    public ILocator ReshardDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "Reshard a tree", Exact = true });

    /// <summary>Sees the list, with <paramref name="tree"/> in it.</summary>
    /// <param name="tree">A tree the list must show.</param>
    public async Task ExpectListedAsync(string tree)
    {
        await Expect(Page).ToHaveURLAsync(new Regex("/cluster/trees(\\?|$)"));
        await Shell.ExpectHeadingAsync("Trees");
        await Expect(Content.Locator("[data-lt-cluster-count]")).ToContainTextAsync("System trees are not listed.");
        await Expect(Content.GetByText(tree, new() { Exact = true }).First).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/trees");
    }

    /// <summary>Opens <paramref name="tree"/>'s administration page from its row (its address when the list is compact).</summary>
    /// <param name="tree">The tree.</param>
    public async Task OpenTreeAsync(string tree)
    {
        if (await Shell.IsCompactAsync())
        {
            await Shell.GotoAsync("/cluster/trees/" + tree);
        }
        else
        {
            await Content.GetByLabel("Filter trees").FillAsync(tree);
            await Link(tree).ClickAsync();
        }

        await Shell.ExpectHeadingAsync(tree);
    }

    /// <summary>Opens the "Reshard a tree" dialog from the list's button, and names <paramref name="tree"/> in it.</summary>
    /// <param name="tree">The tree to reshard.</param>
    public async Task NameTreeToReshardAsync(string tree)
    {
        await ReshardButton.ClickAsync();
        await Expect(ReshardDialog).ToBeVisibleAsync();
        var box = ReshardDialog.GetByRole(AriaRole.Combobox, new() { Name = "Tree" });
        await box.FillAsync(tree);
        await Expect(box).ToHaveValueAsync(tree);
    }

    /// <summary>Continues from the dialog to the tree's reshard page.</summary>
    /// <param name="tree">The tree named in the dialog.</param>
    public async Task ContinueToReshardAsync(string tree)
    {
        await ReshardDialog.GetByRole(AriaRole.Button, new() { Name = "Continue", Exact = true }).ClickAsync();
        await Expect(ReshardDialog).ToBeHiddenAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/cluster/trees/" + Regex.Escape(tree) + "/reshard(\\?|$)"));
        await Shell.ExpectHeadingAsync("Reshard " + tree);
        Coverage.Hit("command:cluster.reshard-tree");
    }
}
