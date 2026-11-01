using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3;

/// <summary>
/// The Data area as A3 drives it: the directory and its <b>New tree from file...</b> (G1), a
/// tree's workspace with its Keys, Views and Tag indexes tabs, the <b>Import...</b> dialog (G1)
/// and the <b>New view...</b> dialog (G5). Each method records its coverage hit only after its
/// own assertion.
/// </summary>
/// <param name="shell">The chrome of olivia's page.</param>
internal sealed class DataPages(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The Import dialog of <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree's name as the workspace shows it.</param>
    public ILocator ImportDialog(string tree) => Page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}" });

    /// <summary>The New view dialog.</summary>
    public ILocator NewViewDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "New view" });

    /// <summary>The rows of the open tab's table.</summary>
    public ILocator Rows => Content.Locator("tbody tr");

    /// <summary>Opens the Data directory.</summary>
    public async Task OpenDirectoryAsync()
    {
        await shell.GotoAsync("/data");
        await shell.ExpectHeadingAsync("Data");
        Coverage.Hit("route:/data");
    }

    /// <summary>The directory's New tree from file control.</summary>
    public ILocator NewTreeFromFile => Content.Locator("[data-lt-data-new-from-file]");

    /// <summary>Opens a tree's workspace on <paramref name="tab"/> (<see langword="null"/> for Keys).</summary>
    /// <param name="tree">The tree, or a name that reaches it.</param>
    /// <param name="query">Further query, such as <c>prefix=order/1</c>, or <see langword="null"/>.</param>
    /// <param name="tab">The tab's <c>?tab=</c> value, or <see langword="null"/>.</param>
    public async Task OpenTreeAsync(string tree, string? tab = null, string? query = null)
    {
        var parts = new[] { tab is null ? null : "tab=" + tab, query }.Where(part => part is not null).ToArray();
        await shell.GotoAsync(parts.Length == 0 ? $"/data/{tree}" : $"/data/{tree}?{string.Join('&', parts)}");
        await Expect(shell.Heading).ToHaveTextAsync(tree, new() { Timeout = 30_000 });
        var title = tab switch
        {
            null => "Keys",
            "views" => "Views",
            "tag-indexes" => "Tag indexes",
            _ => tab,
        };
        await Expect(Content.GetByRole(AriaRole.Tab, new() { Name = title, Selected = true })).ToHaveCountAsync(1);
        Coverage.Hit("route:/data/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
        Coverage.Hit(tab switch
        {
            null => "tab:data/keys",
            "views" => "tab:data/views",
            "tag-indexes" => "tab:data/tag-indexes",
            _ => throw new ArgumentException($"A3 does not open the {tab} tab.", nameof(tab)),
        });
    }

    /// <summary>The workspace's Import control.</summary>
    public ILocator ImportButton => Content.Locator("[data-lt-command=\"data.import\"]");

    /// <summary>Chooses <paramref name="path"/> in an open Import dialog, in Seed mode, keyed by <paramref name="keyTemplate"/>.</summary>
    /// <param name="dialog">The Import dialog.</param>
    /// <param name="path">The file.</param>
    /// <param name="keyTemplate">The key template, such as <c>order/{/id}</c>.</param>
    public static async Task ChooseSeedFileAsync(ILocator dialog, string path, string keyTemplate)
    {
        await Expect(dialog.Locator(".lt-data-import")).ToHaveAttributeAsync("data-lt-import-state", "choose");
        await dialog.GetByLabel("Mode", new() { Exact = true }).SelectOptionAsync("Seed");
        await dialog.Locator("#data-import-file").SetInputFilesAsync(path);
        await Expect(dialog.GetByLabel("Format", new() { Exact = true })).ToHaveValueAsync("Ndjson");
        await dialog.GetByLabel("Key", new() { Exact = true }).FillAsync(keyTemplate);
        await Expect(dialog.GetByLabel("Value", new() { Exact = true })).ToHaveValueAsync("Record");
    }

    /// <summary>Starts a seed in an open Import dialog and waits for it to finish in <paramref name="state"/>.</summary>
    /// <param name="dialog">The Import dialog.</param>
    /// <param name="state">The finished state: <c>succeeded</c> or <c>failed</c>.</param>
    public static async Task SeedAsync(ILocator dialog, string state)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Seed tree", Exact = true }).ClickAsync();
        await Expect(dialog.Locator(".lt-data-import")).ToHaveAttributeAsync("data-lt-import-state", state, new() { Timeout = 120_000 });
    }

    /// <summary>The Views tab's New view control.</summary>
    public ILocator NewViewButton => Content.Locator("button[data-lt-command=\"data.create-view\"]");

    /// <summary>The row of view <paramref name="view"/> on the Views tab.</summary>
    /// <param name="view">The view.</param>
    public ILocator ViewRow(string view) => Rows.Filter(new() { HasText = view });

    /// <summary>Refreshes the Views tab until <paramref name="view"/> reads Current.</summary>
    /// <param name="view">The view.</param>
    public async Task WaitForCurrentAsync(string view)
    {
        var row = ViewRow(view);
        await Expect(row).ToBeVisibleAsync(new() { Timeout = 30_000 });
        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (!(await row.InnerTextAsync()).Contains("Current", StringComparison.Ordinal))
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"View {view} did not read Current within two minutes: {await row.InnerTextAsync()}");
            }

            await Page.WaitForTimeoutAsync(1_000);
            await Content.GetByRole(AriaRole.Button, new() { Name = "Refresh", Exact = true }).ClickAsync();
        }
    }

    /// <summary>The operation section a tab shows while a tracked operation runs and after it ends.</summary>
    public ILocator Operation => Content.Locator("[data-lt-data-operation]");
}
