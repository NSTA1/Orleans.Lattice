using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// The Access rule list (<c>/access/rules</c>): its search, ownership filter, count line,
/// <b>Load more rules</b>, <b>New rule</b>, and the rule table's links.
/// </summary>
/// <param name="shell">The session's shell.</param>
internal sealed class RulesListPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The count line, such as <c>200 rules, more to load</c>.</summary>
    public ILocator Count => Content.Locator(".lt-access-count");

    /// <summary>The <b>Load more rules</b> button.</summary>
    public ILocator LoadMore => Content.GetByRole(AriaRole.Button, new() { Name = "Load more rules", Exact = true });

    /// <summary>The rule search box.</summary>
    public ILocator Search => Content.GetByLabel("Search rules", new() { Exact = true });

    /// <summary>The table's empty line.</summary>
    public ILocator Empty => Content.GetByText("No rule matches.", new() { Exact = true });

    /// <summary>The access posture banner's value for <paramref name="term"/>, such as <c>Rules</c>.</summary>
    /// <param name="term">The banner term.</param>
    public ILocator PostureValue(string term) =>
        Content.Locator("dl.lt-access-banner .lt-access-banner__item")
            .Filter(new() { Has = Page.Locator("dt", new() { HasTextRegex = Pickers.Exactly(term) }) })
            .Locator("dd");

    /// <summary>Opens the cluster-wide rule list through the Access area's own navigation.</summary>
    public async Task OpenAsync()
    {
        await Content.GetByRole(AriaRole.Navigation, new() { Name = "Access" }).GetByRole(AriaRole.Link, new() { Name = "Rules", Exact = true }).ClickAsync();
        await WaitForListAsync();
    }

    /// <summary>The line on a tenant-rooted rule list that links every rule on the cluster.</summary>
    public ILocator ClusterWideLink => Content.Locator("[data-lt-cluster-wide-rules]").GetByRole(AriaRole.Link, new() { Name = "See every rule on the cluster", Exact = true });

    /// <summary>
    /// Opens Access from the spine. With tenancy on (the estate runs the default tenant) the
    /// spine roots it at the active tenant, whose rule list shows only that tenant's rules
    /// and links every rule on the cluster.
    /// </summary>
    /// <returns>Whether the page opened is tenant-rooted.</returns>
    public async Task<bool> OpenFromSpineAsync()
    {
        await shell.GoToAreaAsync("access");
        await shell.ExpectHeadingAsync("Access");
        await Expect(Count).ToBeVisibleAsync(new() { Timeout = 60_000 });
        if (!new System.Text.RegularExpressions.Regex(@"/t/[^/]+/access").IsMatch(Page.Url))
        {
            await WaitForListAsync();
            return false;
        }

        await Expect(Count).ToContainTextAsync("rules of tenant");
        await Expect(ClusterWideLink).ToBeVisibleAsync();
        return true;
    }

    /// <summary>Follows the tenant-rooted page's link to the cluster-wide rule list.</summary>
    public async Task OpenClusterWideAsync()
    {
        await ClusterWideLink.ClickAsync();
        await Expect(Page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex(@"://[^/]+/access/rules$"));
        await WaitForListAsync();
    }

    /// <summary>Waits until the cluster-wide rule list has read its first page.</summary>
    public async Task WaitForListAsync()
    {
        await shell.ExpectHeadingAsync("Access");
        await Expect(Page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex(@"://[^/]+/access(/rules)?(\?.*)?$"));
        await Expect(Count).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(Count).Not.ToContainTextAsync("of tenant");
    }

    /// <summary>Clicks <b>Load more rules</b> until every rule is loaded, expecting <paramref name="total"/> in all.</summary>
    /// <param name="total">The rules the cluster holds.</param>
    public async Task LoadAllAsync(int total)
    {
        await LoadEverythingAsync();
        await Expect(Count).ToHaveTextAsync($"{total} rules");
    }

    /// <summary>Clicks <b>Load more rules</b> until it is gone.</summary>
    public async Task LoadEverythingAsync()
    {
        for (var page = 0; page < 20 && await LoadMore.CountAsync() > 0; page++)
        {
            var before = await Count.InnerTextAsync();
            await LoadMore.ClickAsync();
            await Expect(Count).Not.ToHaveTextAsync(before);
        }

        await Expect(LoadMore).ToHaveCountAsync(0);
    }

    /// <summary>Types <paramref name="term"/> into the rule search.</summary>
    /// <param name="term">The search; empty clears it.</param>
    public Task SearchAsync(string term) => Search.FillAsync(term);

    /// <summary>Chooses the ownership filter labelled <paramref name="label"/> (<b>All rules</b>, <b>Authored</b> or <b>App-owned</b>).</summary>
    /// <param name="label">The filter's label.</param>
    public async Task FilterAsync(string label)
    {
        if (await shell.IsCompactAsync())
        {
            await Content.GetByLabel("Show", new() { Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = label });
            return;
        }

        var button = Content.GetByRole(AriaRole.Group, new() { Name = "Show" }).GetByRole(AriaRole.Button, new() { Name = label, Exact = true });
        await button.ClickAsync();
        await Expect(button).ToHaveAttributeAsync("aria-pressed", "true");
    }

    /// <summary>The table row (or, on a phone, the list row) of rule <paramref name="ruleId"/> whose scope reads <paramref name="scope"/>.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="scope">The scope as the table names it, such as <c>factory-telemetry</c>.</param>
    public ILocator Row(string ruleId, string scope) => Tables.Rows(Page, Content, ruleId, scope);

    /// <summary>Loads every rule, searches for <paramref name="ruleId"/> and opens its details page from the table.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="scope">The scope as the table names it.</param>
    public async Task OpenRuleAsync(string ruleId, string scope)
    {
        await LoadEverythingAsync();
        await SearchAsync(ruleId);
        var row = Row(ruleId, scope);
        await Expect(row).ToHaveCountAsync(1);
        if (await shell.IsCompactAsync())
        {
            await row.GetByRole(AriaRole.Button).ClickAsync();
            await Page.GetByRole(AriaRole.Dialog, new() { Name = ruleId }).GetByRole(AriaRole.Link, new() { Name = "Open rule" }).ClickAsync();
        }
        else
        {
            await row.GetByRole(AriaRole.Link, new() { Name = ruleId, Exact = true }).ClickAsync();
        }

        await shell.ExpectHeadingAsync(ruleId);
    }

    /// <summary>Opens the <b>New rule</b> dialog.</summary>
    public async Task<RuleEditorDialog> NewRuleAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "New rule", Exact = true }).ClickAsync();
        var dialog = new RuleEditorDialog(Page, Page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" }));
        await Expect(dialog.Form).ToBeVisibleAsync();
        return dialog;
    }
}
