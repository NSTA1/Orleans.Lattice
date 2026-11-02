using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// <c>/cluster/trees</c> as A12 drives it: the filter, <b>New tree...</b> with explicit sizing
/// and the created tree's Configuration tab, and <b>Reshard tree...</b>.
/// </summary>
/// <param name="shell">The shell of the session driving it.</param>
internal sealed class ClusterTreesPage(ShellPage shell)
{
    /// <summary>The page's address.</summary>
    public const string Path = "/cluster/trees";

    /// <summary>The filter box.</summary>
    public ILocator Filter => shell.Content.GetByRole(AriaRole.Searchbox, new() { Name = "Filter trees" });

    /// <summary>The New tree dialog.</summary>
    public ILocator CreateDialog => shell.Page.GetByRole(AriaRole.Dialog, new() { Name = "New tree" });

    /// <summary>Opens the list and waits for its rows.</summary>
    public async Task OpenAsync()
    {
        await shell.GotoAsync(Path);
        await shell.ExpectHeadingAsync("Trees");
        await Expect(shell.Content.Locator("[data-lt-cluster-count]")).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/trees");
    }

    /// <summary>Types <paramref name="text"/> into the filter.</summary>
    /// <param name="text">The filter text.</param>
    public Task FilterAsync(string text) => Filter.FillAsync(text);

    /// <summary>Asserts the filtered list shows each of <paramref name="listed"/> and none of <paramref name="hidden"/>.</summary>
    /// <param name="listed">Trees the filter keeps.</param>
    /// <param name="hidden">Trees the filter drops.</param>
    public async Task ExpectListedAsync(IEnumerable<string> listed, IEnumerable<string> hidden)
    {
        foreach (var tree in listed)
        {
            await Expect(ClusterParts.Row(shell, tree)).ToHaveCountAsync(1);
        }

        foreach (var tree in hidden)
        {
            await Expect(ClusterParts.Row(shell, tree)).ToHaveCountAsync(0);
        }
    }

    /// <summary>Asserts the filter matched nothing, and says so.</summary>
    public async Task ExpectNoMatchAsync()
    {
        await Expect(shell.Content.GetByRole(AriaRole.Heading, new() { Name = "No tree matches" })).ToBeVisibleAsync();
        await Expect(ClusterParts.Rows(shell)).ToHaveCountAsync(0);
    }

    /// <summary>Opens <b>New tree...</b> and fills its name and sizing.</summary>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count.</param>
    /// <param name="leafKeys">Keys per leaf.</param>
    /// <param name="children">Children per internal node.</param>
    public async Task ComposeTreeAsync(string tree, int shards, int leafKeys, int children)
    {
        var create = shell.Content.Locator("[data-lt-command=\"cluster.create-tree\"]");
        await Expect(create).ToHaveTextAsync("New tree...");
        await create.ClickAsync();
        await Expect(CreateDialog.GetByLabel("Physical shards")).ToHaveValueAsync("64");
        await CreateDialog.GetByLabel("Tree name").FillAsync(tree);
        await CreateDialog.GetByLabel("Physical shards").FillAsync(Text(shards));
        await CreateDialog.GetByLabel("Keys per leaf").FillAsync(Text(leafKeys));
        await CreateDialog.GetByLabel("Children per internal node").FillAsync(Text(children));
    }

    /// <summary>Reviews the composed tree; the review names it and its sizing.</summary>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count.</param>
    /// <param name="leafKeys">Keys per leaf.</param>
    /// <param name="children">Children per internal node.</param>
    public async Task ReviewTreeAsync(string tree, int shards, int leafKeys, int children)
    {
        await CreateDialog.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Expect(CreateDialog.Locator("[data-lt-create-tree=review]")).ToContainTextAsync(
            $"Create the empty tree {tree} with {shards} physical shards, {leafKeys} keys per leaf and {children} children per internal node.");
    }

    /// <summary>Creates the reviewed tree and lands on its Summary.</summary>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count.</param>
    /// <param name="leafKeys">Keys per leaf.</param>
    /// <param name="children">Children per internal node.</param>
    public async Task CreateTreeAsync(string tree, int shards, int leafKeys, int children)
    {
        await CreateDialog.GetByRole(AriaRole.Button, new() { Name = "Create tree" }).ClickAsync();
        await shell.ExpectHeadingAsync(tree);
        await Expect(shell.Content.Locator("[data-lt-tree-created]")).ToContainTextAsync(
            $"{tree} is new and empty, with {shards} physical shards, {leafKeys} keys per leaf and {children} children per internal node.");
        await Expect(ClusterParts.Toast(shell, $"Tree {tree} created.")).ToHaveCountAsync(1);
        Coverage.Hit("command:cluster.create-tree");
    }

    /// <summary>Follows the created tree's link to its Configuration tab, which shows the sizing.</summary>
    /// <param name="shards">The physical shard count.</param>
    /// <param name="leafKeys">Keys per leaf.</param>
    /// <param name="children">Children per internal node.</param>
    public async Task ExpectConfigurationAsync(int shards, int leafKeys, int children)
    {
        await shell.Content.Locator("[data-lt-tree-created-next=configuration]").ClickAsync();
        await Expect(shell.Content.GetByRole(AriaRole.Tab, new() { Name = "Configuration", Selected = true })).ToHaveCountAsync(1);
        await Expect(ClusterParts.Term(shell, "Physical shards")).ToHaveTextAsync(Text(shards));
        await Expect(ClusterParts.Term(shell, "Keys per leaf")).ToHaveTextAsync(Text(leafKeys));
        await Expect(ClusterParts.Term(shell, "Children per internal node")).ToHaveTextAsync(Text(children));
        Coverage.Hit("tab:cluster/configuration");
        Coverage.Hit("use-case:cluster/create-tree-sizing");
    }

    /// <summary>Opens <b>Reshard tree...</b>, picks <paramref name="tree"/> and continues to its reshard page.</summary>
    /// <param name="tree">The tree.</param>
    public async Task OpenReshardAsync(string tree)
    {
        await shell.Content.Locator("[data-lt-command=\"cluster.reshard-tree\"]").ClickAsync();
        var dialog = shell.Page.GetByRole(AriaRole.Dialog, new() { Name = "Reshard a tree" });
        await Expect(dialog).ToBeVisibleAsync();
        var picker = dialog.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await picker.FillAsync(tree);
        await picker.PressAsync("Tab");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Continue" }).ClickAsync();
        await Expect(shell.Page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex($"/cluster/trees/{System.Text.RegularExpressions.Regex.Escape(tree)}/reshard$"));
        Coverage.Hit("command:cluster.reshard-tree");
    }

    private static string Text(int value) => value.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
