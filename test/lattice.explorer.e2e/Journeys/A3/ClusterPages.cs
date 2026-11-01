using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3;

/// <summary>
/// The Cluster area as A3 drives it: the overview, the tree list and its <b>New tree...</b>
/// dialog (G4), a tree's tabs (Summary, Configuration, Shards, Storage, Lifecycle) and its
/// admin tools. Each method records its coverage hit only after its own assertion.
/// </summary>
/// <param name="shell">The chrome of olivia's page.</param>
internal sealed class ClusterPages(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The New tree dialog.</summary>
    public ILocator NewTreeDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "New tree" });

    /// <summary>Opens the Cluster overview.</summary>
    public async Task OpenOverviewAsync()
    {
        await shell.GotoAsync("/cluster");
        await shell.ExpectHeadingAsync("Cluster");
        Coverage.Hit("route:/cluster");
    }

    /// <summary>Opens the tree list and finds <b>New tree...</b> on it.</summary>
    /// <returns>The control.</returns>
    public async Task<ILocator> OpenTreesAsync()
    {
        await shell.GotoAsync("/cluster/trees");
        await shell.ExpectHeadingAsync("Trees");
        var create = Content.Locator("[data-lt-command=\"cluster.create-tree\"]");
        await Expect(create).ToHaveTextAsync("New tree...");
        Coverage.Hit("subroute:cluster/trees");
        return create;
    }

    /// <summary>Opens <b>New tree...</b> and fills the name and, when given, the shard count.</summary>
    /// <param name="create">The control.</param>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count, or <see langword="null"/> for the form's default.</param>
    public async Task FillNewTreeAsync(ILocator create, string tree, int? shards)
    {
        await create.ClickAsync();
        await Expect(NewTreeDialog).ToBeVisibleAsync();
        await Expect(NewTreeDialog.GetByLabel("Physical shards")).ToHaveValueAsync("64");
        await NewTreeDialog.GetByLabel("Tree name").FillAsync(tree);
        if (shards is { } count)
        {
            await NewTreeDialog.GetByLabel("Physical shards").FillAsync(count.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Reviews the staged tree and waits for the review to name it.</summary>
    /// <param name="dialog">The New tree (or New tree from file) dialog.</param>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count the review names.</param>
    public static async Task ReviewAsync(ILocator dialog, string tree, int shards)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Review", Exact = true }).ClickAsync();
        await Expect(dialog.Locator("[data-lt-create-tree=review]")).ToContainTextAsync(
            $"Create the empty tree {tree} with {shards} physical shards");
    }

    /// <summary>Creates the reviewed tree and lands on its Summary with the Tree created section.</summary>
    /// <param name="tree">The tree name.</param>
    /// <param name="shards">The physical shard count.</param>
    public async Task CreateAndLandOnSummaryAsync(string tree, int shards)
    {
        await NewTreeDialog.GetByRole(AriaRole.Button, new() { Name = "Create tree", Exact = true }).ClickAsync();
        await Expect(shell.Heading).ToHaveTextAsync(tree, new() { Timeout = 30_000 });
        await Expect(Content.Locator("[data-lt-tree-created]")).ToContainTextAsync($"{tree} is new and empty, with {shards} physical shards");
        await Expect(Content.GetByRole(AriaRole.Tab, new() { Name = "Summary", Selected = true })).ToHaveCountAsync(1);
        await A3Ui.ExpectToastAsync(shell, $"Tree {tree} created.");
        Coverage.Hit("command:cluster.create-tree");
        Coverage.Hit("route:/cluster/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}/{p7?}/{p8?}");
        Coverage.Hit("subroute:cluster/trees/{tree}/overview");
        Coverage.Hit("tab:cluster/summary");
    }

    /// <summary>Opens a tree's page on <paramref name="tab"/> (<see langword="null"/> for Summary).</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="tab">The tab's <c>?tab=</c> value, or <see langword="null"/>.</param>
    public async Task OpenTreeAsync(string tree, string? tab = null)
    {
        await shell.GotoAsync(tab is null ? $"/cluster/trees/{tree}" : $"/cluster/trees/{tree}?tab={tab}");
        await Expect(shell.Heading).ToHaveTextAsync(tree, new() { Timeout = 30_000 });
        var title = tab is null ? "Summary" : char.ToUpperInvariant(tab[0]) + tab[1..];
        await Expect(Content.GetByRole(AriaRole.Tab, new() { Name = title, Selected = true })).ToHaveCountAsync(1);
        if (tab is not null)
        {
            Coverage.Hit("query:cluster.tab");
        }
    }

    /// <summary>The value of the open tab's definition row <paramref name="term"/>.</summary>
    /// <param name="term">The term.</param>
    public ILocator Term(string term) => A3Ui.Term(Content, term);

    /// <summary>Saves history retention on the open Configuration tab.</summary>
    /// <param name="mode">The Retention mode option's text.</param>
    /// <param name="days">The window in days, or <see langword="null"/> to leave it as it is.</param>
    public async Task FillRetentionAsync(string mode, int? days)
    {
        var form = Content.Locator("form[aria-label='Change history retention']");
        await form.GetByLabel("Retention mode", new() { Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = mode });
        if (days is { } value)
        {
            await form.GetByLabel("Window, days", new() { Exact = true }).FillAsync(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Saves the retention form and waits for the confirmation.</summary>
    public async Task SaveRetentionAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save retention", Exact = true }).ClickAsync();
        await A3Ui.ExpectToastAsync(shell, "History retention saved.");
    }

    /// <summary>Chooses a value of a configuration switch on the open Configuration tab.</summary>
    /// <param name="label">The switch's label, such as <c>Publish events</c>.</param>
    /// <param name="value">The option's text, such as <c>On</c>.</param>
    public Task ChooseConfigurationAsync(string label, string value) =>
        Content.Locator("form[aria-label='Change configuration']").GetByLabel(label, new() { Exact = true })
            .SelectOptionAsync(new SelectOptionValue { Label = value });

    /// <summary>Saves the configuration form and waits for the confirmation.</summary>
    public async Task SaveConfigurationAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save configuration", Exact = true }).ClickAsync();
        await A3Ui.ExpectToastAsync(shell, "Configuration saved.");
    }

    /// <summary>The Lifecycle tab's form that gives the tree another name.</summary>
    public ILocator AddNameForm => Content.Locator("form[aria-label='Add a name']");

    /// <summary>Opens a tree's admin tools.</summary>
    /// <param name="tree">The tree.</param>
    public async Task OpenToolsAsync(string tree)
    {
        await shell.GotoAsync($"/cluster/trees/{tree}/tools");
        await Expect(shell.Heading).ToHaveTextAsync($"Admin tools {tree}", new() { Timeout = 30_000 });
        Coverage.Hit("subroute:cluster/trees/{tree}/tools");
    }

    /// <summary>The tools page's bulk-load text area.</summary>
    public ILocator BulkInput => Content.Locator("#lt-cluster-bulk-input");

    /// <summary>The tools page's bulk-load section.</summary>
    public ILocator BulkSection => Content.Locator("section[aria-labelledby='lt-cluster-bulk']");

    /// <summary>The tools page's compaction form.</summary>
    public ILocator CompactionForm => Content.Locator("form[aria-label='Trigger compaction']");

    /// <summary>The tools page's projection-digest section.</summary>
    public ILocator DigestSection => Content.Locator("section[aria-labelledby='lt-cluster-digest']");

    /// <summary>The tools page's compaction section.</summary>
    public ILocator CompactionSection => Content.Locator("section[aria-labelledby='lt-cluster-compaction']");
}
