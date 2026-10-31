using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>The Schema directory: governed trees, every tree, the filter, and the compliance picker.</summary>
internal sealed partial class SchemaPage
{
    /// <summary>The directory's table rows.</summary>
    public ILocator DirectoryRows => Content.Locator("table tbody tr");

    /// <summary>The directory row of <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree.</param>
    public ILocator DirectoryRow(string tree) =>
        DirectoryRows.Filter(new() { Has = Page.Locator("a.lt-schema-tree-link", new() { HasTextRegex = new Regex($"^{Regex.Escape(tree)}$") }) });

    /// <summary>Opens the directory as it opens by default: the trees under schema.</summary>
    public async Task OpenDirectoryAsync()
    {
        await Shell.GotoAsync("/schema");
        await Shell.ExpectHeadingAsync("Schema");
        await Expect(Content.Locator("p.lt-schema-status[role=status]")).ToContainTextAsync("under schema");
        await Expect(Content.GetByRole(AriaRole.Link, new() { Name = "Under schema", Exact = true })).ToHaveAttributeAsync("aria-current", "page");
        Coverage.Hit("route:/schema");
    }

    /// <summary>
    /// Asserts <paramref name="tree"/>'s directory row: its Policy and Versioning columns
    /// contain the given text, and its Compliance column reads exactly <paramref name="compliance"/>.
    /// </summary>
    /// <param name="tree">The tree.</param>
    /// <param name="policy">Text the Policy column contains.</param>
    /// <param name="versioning">Text the Versioning column contains.</param>
    /// <param name="compliance">The Compliance column's text.</param>
    public async Task ExpectDirectoryRowAsync(string tree, string policy, string versioning, string compliance)
    {
        var cells = DirectoryRow(tree).Locator("td, th");
        await Expect(cells.Nth(1)).ToContainTextAsync(policy);
        await Expect(cells.Nth(2)).ToContainTextAsync(versioning);
        await Expect(cells.Nth(3)).ToHaveTextAsync(compliance);
    }

    /// <summary>Follows the <b>All trees</b> link (<c>schema.all-trees</c>): the directory lists every tree, governed or not.</summary>
    public async Task ShowAllTreesAsync()
    {
        var link = Content.Locator("a[data-lt-command=\"schema.all-trees\"]");
        await Expect(link).ToHaveTextAsync("All trees");
        await link.ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex(@"/schema\?show=all$"));
        await Expect(link).ToHaveAttributeAsync("aria-current", "page");
        Coverage.Hit("command:schema.all-trees");
        Coverage.Hit("query:schema.show");
        Coverage.Hit("query-value:schema.show=all");
    }

    /// <summary>Filters the directory by tree id, and asserts the address carries the filter and every row matches it.</summary>
    /// <param name="text">The filter text.</param>
    public async Task FilterDirectoryAsync(string text)
    {
        await Content.GetByLabel("Filter trees by id", new() { Exact = true }).FillAsync(text);
        await Expect(Page).ToHaveURLAsync(new Regex($@"/schema\?show=all&filter={Regex.Escape(text)}$"));
        await Expect(DirectoryRows.Filter(new() { HasNotText = text })).ToHaveCountAsync(0);
        Coverage.Hit("query:schema.filter");
    }

    /// <summary>
    /// Opens the <b>Scan compliance...</b> picker (<c>schema.scan-compliance</c>) and asserts it
    /// offers <paramref name="tree"/>, the one tree with a policy the caller can scan.
    /// </summary>
    /// <param name="tree">The tree it offers.</param>
    /// <returns>The picker dialog.</returns>
    public async Task<ILocator> OpenScanPickerAsync(string tree)
    {
        await Content.Locator("button[data-lt-command=\"schema.scan-compliance\"]").ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Scan compliance", Exact = true });
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.GetByLabel("Tree", new() { Exact = true }).SelectOptionAsync(tree);
        await Expect(dialog.GetByLabel("Tree", new() { Exact = true })).ToHaveValueAsync(tree);
        Coverage.Hit("command:schema.scan-compliance");
        return dialog;
    }
}
