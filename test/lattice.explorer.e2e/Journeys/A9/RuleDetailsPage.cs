using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>One rule's details page (<c>/access/rules/{id}?tree=</c>): its definition, edit and delete.</summary>
/// <param name="shell">The session's shell.</param>
internal sealed class RuleDetailsPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The rule's condition, shown verbatim.</summary>
    public ILocator Condition => Content.Locator("[data-lt-condition]");

    /// <summary>The not-evaluated warning beside the condition.</summary>
    public ILocator ConditionWarning => Content.Locator("dd [data-lt-condition-warning]");

    /// <summary>The lede shown when the id is used under more than one tree.</summary>
    public ILocator Ambiguity => Content.Locator("p.lt-shell-page-lede");

    /// <summary>The value of the definition named <paramref name="term"/>, such as <c>Scope</c>.</summary>
    /// <param name="term">The term.</param>
    public ILocator Definition(string term) =>
        Content.Locator(".lt-dl__row").Filter(new() { Has = Page.Locator("dt", new() { HasTextRegex = Pickers.Exactly(term) }) }).Locator("dd");

    /// <summary>Expects the page to show <paramref name="ruleId"/> with this scope and these operations.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="subject">The subject as the page names it, such as <c>group:factory-staff</c>.</param>
    /// <param name="scope">The scope as the page names it.</param>
    /// <param name="operations">The operations as the page names them.</param>
    public async Task ExpectAsync(string ruleId, string subject, string scope, string operations)
    {
        await shell.ExpectHeadingAsync(ruleId);
        await Expect(Definition("Effect")).ToHaveTextAsync("Allow");
        await Expect(Definition("Subject")).ToHaveTextAsync(subject);
        await Expect(Definition("Scope")).ToHaveTextAsync(scope);
        await Expect(Definition("Operations")).ToHaveTextAsync(operations);
        await Expect(Definition("Owner")).ToHaveTextAsync("Authored");
    }

    /// <summary>Opens the <b>Edit rule</b> dialog.</summary>
    public async Task<RuleEditorDialog> EditAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Edit rule", Exact = true }).ClickAsync();
        var dialog = new RuleEditorDialog(Page, Page.GetByRole(AriaRole.Dialog, new() { Name = "Edit rule" }));
        await Expect(dialog.Form).ToBeVisibleAsync();
        return dialog;
    }

    /// <summary>Opens the <b>Delete this rule</b> confirmation and types the rule's id into it.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <returns>The confirmation dialog, ready to confirm.</returns>
    public async Task<ILocator> ConfirmDeleteAsync(string ruleId)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true }).ClickAsync();
        var confirm = Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Delete this rule" })
            .Or(Page.GetByRole(AriaRole.Dialog, new() { Name = "Delete this rule" }));
        await Expect(confirm).ToBeVisibleAsync();
        await confirm.GetByRole(AriaRole.Textbox, new() { Name = "Rule name", Exact = true }).FillAsync(ruleId);
        await Expect(confirm.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true })).ToBeEnabledAsync();
        return confirm;
    }

    /// <summary>Confirms the deletion, expecting the rule list again.</summary>
    /// <param name="confirm">The confirmation dialog.</param>
    /// <param name="ruleId">The rule id.</param>
    public async Task DeleteAsync(ILocator confirm, string ruleId)
    {
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true }).ClickAsync();
        await Expect(shell.Toasts.Filter(new() { HasText = $"Rule {ruleId} deleted." }).Last).ToBeVisibleAsync();
        await shell.ExpectHeadingAsync("Access");
    }

    /// <summary>The candidate rows offered for an id used under more than one tree.</summary>
    public ILocator Candidates => Content.Locator("tbody tr, .lt-table-list li");
}
