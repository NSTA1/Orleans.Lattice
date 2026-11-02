using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// The Access explain page (<c>/access/explain</c>): its question, read from and written to
/// the address, the verdict with the group closure and the matched rules, and
/// <b>Effective permissions</b>.
/// </summary>
/// <param name="shell">The session's shell.</param>
internal sealed class ExplainPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    private ILocator Form => Content.Locator("form.lt-access-form");

    /// <summary>The subject kind choice.</summary>
    public ILocator SubjectKind => Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true });

    /// <summary>The subject picker.</summary>
    public ILocator Subject => Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true });

    /// <summary>The operation choice.</summary>
    public ILocator Operation => Form.GetByRole(AriaRole.Combobox, new() { Name = "Operation", Exact = true });

    /// <summary>The scope choice.</summary>
    public ILocator Scope => Form.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true });

    /// <summary>The tree picker.</summary>
    public ILocator Tree => Form.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });

    /// <summary>The verdict heading: <c>Allowed</c> or <c>Denied</c>.</summary>
    public ILocator Verdict => Content.Locator("#lt-access-verdict");

    /// <summary>The answer's section: the verdict or the effective permissions.</summary>
    public ILocator Answer => Content.Locator("section.lt-access-section");

    /// <summary>The group closure links of the answer.</summary>
    public ILocator Groups => Answer.Locator("ul.lt-access-links a");

    /// <summary>The answer's rule rows.</summary>
    public ILocator RuleRows => Answer.Locator("tbody tr, .lt-table-list li");

    /// <summary>The effective-permissions heading.</summary>
    public ILocator PermissionsHeading => Content.Locator("#lt-access-permissions");

    /// <summary>Opens the page at <paramref name="address"/>, as a link or a typed address would.</summary>
    /// <param name="address">The address, with its query.</param>
    public async Task OpenAtAsync(string address)
    {
        await shell.GotoAsync(address);
        await shell.ExpectHeadingAsync("Access");
        await Expect(Form).ToBeVisibleAsync();
    }

    /// <summary>Asks the cluster with <b>Explain</b>.</summary>
    public async Task ExplainAsync()
    {
        await Form.GetByRole(AriaRole.Button, new() { Name = "Explain", Exact = true }).ClickAsync();
        await Expect(Verdict).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    /// <summary>Asks the cluster for <b>Effective permissions</b>.</summary>
    public async Task EffectivePermissionsAsync()
    {
        await Form.GetByRole(AriaRole.Button, new() { Name = "Effective permissions", Exact = true }).ClickAsync();
        await Expect(PermissionsHeading).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    /// <summary>The answer's row for rule <paramref name="ruleId"/> on <paramref name="scope"/>.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="scope">The scope as the table names it.</param>
    public ILocator RuleRow(string ruleId, string scope) => Tables.Rows(Page, Answer, ruleId, scope);

    /// <summary>The closure link to group <paramref name="group"/>.</summary>
    /// <param name="group">The group id.</param>
    public ILocator GroupLink(string group) => Answer.GetByRole(AriaRole.Link, new() { Name = $"group:{group}", Exact = true });
}
