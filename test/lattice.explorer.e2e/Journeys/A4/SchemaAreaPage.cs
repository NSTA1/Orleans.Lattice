using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A4;

/// <summary>
/// The Schema area as A4 drives it: the directory at <c>/schema</c> and a tree's schema
/// workspace at <c>/schema/{tree}</c> with its Policy and Dead letters tabs. Each method records
/// its coverage hits only after its own assertions pass.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class SchemaAreaPage(ShellPage shell)
{
    /// <summary>The directory's heading.</summary>
    public const string DirectoryHeading = "Schema";

    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The directory's (or any area table's) rows, at either width.</summary>
    private ILocator Rows => Content.Locator("tr.lt-table__row, li.lt-table-list__row");

    /// <summary>The directory's status line, once the trees have been read.</summary>
    private ILocator DirectoryStatus => Content.GetByRole(AriaRole.Status).Filter(new() { HasTextRegex = new Regex("under schema") });

    /// <summary>The directory row of <paramref name="tree"/>, matched on its link's exact name.</summary>
    /// <param name="tree">The tree id.</param>
    public ILocator DirectoryRow(string tree) =>
        Rows.Filter(new() { Has = Page.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true }) });

    /// <summary>Opens the directory, showing the trees under schema, and waits until it has read them.</summary>
    public async Task OpenDirectoryAsync()
    {
        await shell.GotoAsync("/schema");
        await shell.ExpectHeadingAsync(DirectoryHeading);
        await Expect(DirectoryStatus).ToBeVisibleAsync();
        await Expect(Content.GetByRole(AriaRole.Link, new() { Name = "Under schema", Exact = true })).ToHaveAttributeAsync("aria-current", "page");
        Coverage.Hit("route:/schema");
    }

    /// <summary>Asserts that <paramref name="tree"/> is not among the trees under schema.</summary>
    /// <param name="tree">The tree id.</param>
    public Task ExpectNotUnderSchemaAsync(string tree) => Expect(DirectoryRow(tree)).ToHaveCountAsync(0);

    /// <summary>Switches the directory to every tree, then narrows it to ids containing <paramref name="filter"/>.</summary>
    /// <param name="filter">The filter text.</param>
    public async Task ShowAllTreesAsync(string filter)
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = "All trees", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex(@"[?&]show=all(&|$)"));
        await Expect(Content.GetByRole(AriaRole.Link, new() { Name = "All trees", Exact = true })).ToHaveAttributeAsync("aria-current", "page");
        await Expect(DirectoryStatus).ToBeVisibleAsync();
        Coverage.Hit("query:schema.show");
        Coverage.Hit("query-value:schema.show=all");

        var search = Content.GetByLabel("Filter trees by id", new() { Exact = true });
        await search.FillAsync(filter);
        await Expect(Page).ToHaveURLAsync(new Regex(@"[?&]filter=" + Regex.Escape(Uri.EscapeDataString(filter)) + "(&|$)"));
        Coverage.Hit("query:schema.filter");
    }

    /// <summary>Asserts the directory row of <paramref name="tree"/> reads <paramref name="policy"/> and <paramref name="versioning"/>.</summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="policy">The Policy column's text, such as <c>None</c>.</param>
    /// <param name="versioning">The Versioning column's text, such as <c>Unversioned</c>.</param>
    public async Task ExpectRowAsync(string tree, string policy, string versioning)
    {
        var row = DirectoryRow(tree);
        await Expect(row).ToHaveCountAsync(1);
        await Expect(row).ToContainTextAsync(policy);
        await Expect(row).ToContainTextAsync(versioning);
    }

    /// <summary>Follows the directory row's link into <paramref name="tree"/>'s schema workspace, on its Policy tab.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenTreeFromDirectoryAsync(string tree)
    {
        await DirectoryRow(tree).GetByRole(AriaRole.Link, new() { Name = tree, Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/schema/" + Regex.Escape(tree) + @"(\?|$)"));
        await shell.ExpectHeadingAsync(tree);
        await Expect(Tab("Policy")).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Content.GetByRole(AriaRole.Tablist, new() { Name = $"{tree} schema", Exact = true }).GetByRole(AriaRole.Tab))
            .ToHaveTextAsync(["Policy", "Versions", "Compliance", "Remediation", "Dead letters"]);
        Coverage.Hit("route:/schema/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
        Coverage.Hit("use-case:schema/tree-workspace");
    }

    /// <summary>The workspace's tab named <paramref name="title"/>.</summary>
    /// <param name="title">The tab's title.</param>
    public ILocator Tab(string title) => Content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });

    /// <summary>Selects the workspace's tab <paramref name="title"/>, whose address is <c>?tab=<paramref name="query"/></c>.</summary>
    /// <param name="title">The tab's title.</param>
    /// <param name="query">The tab's <c>?tab=</c> value.</param>
    public async Task SelectTabAsync(string title, string query)
    {
        await Tab(title).ClickAsync();
        await Expect(Tab(title)).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Page).ToHaveURLAsync(new Regex(@"[?&]tab=" + Regex.Escape(query) + "(&|$)"));
        Coverage.Hit("query:schema.tab");
    }

    /// <summary>Asserts the Policy tab shows no policy, and offers to set one.</summary>
    public async Task ExpectNoPolicyAsync()
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "No policy", Exact = true })).ToBeVisibleAsync();
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Set a policy", Exact = true })).ToBeVisibleAsync();
        Coverage.Hit("tab:schema/policy");
    }

    /// <summary>Opens the rule builder from the Policy tab's <b>Set a policy</b>.</summary>
    public async Task<SchemaRuleBuilderPage> SetAPolicyAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Set a policy", Exact = true }).ClickAsync();
        var builder = new SchemaRuleBuilderPage(shell);
        await builder.ExpectOpenAsync();
        return builder;
    }

    /// <summary>
    /// Asserts the Policy tab shows a saved policy of <paramref name="rules"/> rules with strict
    /// ingest on, its rules listed one per row.
    /// </summary>
    /// <param name="rules">The rule count.</param>
    public async Task ExpectStrictPolicyAsync(int rules)
    {
        await Expect(Definition("Rules")).ToHaveTextAsync($"{rules} rules, all of which a value must satisfy");
        await Expect(Definition("Strict ingest")).ToContainTextAsync("On: replicated CRDT and atomic-batch entries are checked too");
        await Expect(Content.GetByRole(AriaRole.Tabpanel).Locator("tr.lt-table__row, li.lt-table-list__row")).ToHaveCountAsync(rules);
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Edit policy", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Asserts the workspace heading carries the policy pill and the version pill <paramref name="version"/>.</summary>
    /// <param name="version">The target version the pill names.</param>
    public async Task ExpectHeadingPillsAsync(uint version)
    {
        var meta = Content.Locator(".lt-schema-meta");
        await Expect(meta).ToContainTextAsync("Policy: 2 rules, strict ingest");
        await Expect(meta).ToContainTextAsync($"Versioning: family 1 at version {version}");
    }

    /// <summary>Asserts the Dead letters tab's empty state: strict ingest has diverted nothing.</summary>
    public async Task ExpectNoDeadLettersAsync()
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "No dead letters", Exact = true })).ToBeVisibleAsync();
        await Expect(Content.GetByText("Strict ingest has diverted no value written to this tree.")).ToBeVisibleAsync();
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Load dead letters", Exact = true })).ToBeDisabledAsync();
        Coverage.Hit("tab:schema/dead-letters");
        Coverage.Hit("use-case:schema/dead-letters");
    }

    /// <summary>The definition term <paramref name="term"/>'s value in the visible tab.</summary>
    /// <param name="term">The term.</param>
    public ILocator Definition(string term) =>
        Content.GetByRole(AriaRole.Tabpanel).Locator("dt").Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }).Locator("xpath=following-sibling::dd[1]");
}
