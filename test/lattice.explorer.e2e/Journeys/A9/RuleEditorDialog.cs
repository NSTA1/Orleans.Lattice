using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>The rule editor, in its <b>New rule</b> or <b>Edit rule</b> dialog.</summary>
/// <param name="page">The page.</param>
/// <param name="dialog">The dialog holding the editor.</param>
internal sealed class RuleEditorDialog(IPage page, ILocator dialog)
{
    /// <summary>The scope value for a whole tree.</summary>
    public const string TreeScope = "tree";

    /// <summary>The scope value for a key prefix in a tree.</summary>
    public const string PrefixScope = "prefix";

    /// <summary>The scope value for a single key in a tree.</summary>
    public const string KeyScope = "key";

    /// <summary>The scope value for all trees (cluster-wide).</summary>
    public const string ClusterScope = "cluster";

    /// <summary>The scope value for access administration (delegation), offered when delegation is on.</summary>
    public const string AccessAdministrationScope = "access-administration";

    /// <summary>The dialog.</summary>
    public ILocator Dialog => dialog;

    /// <summary>The editor's form.</summary>
    public ILocator Form => dialog.Locator("form.lt-access-form");

    /// <summary>The <b>Tree</b> picker.</summary>
    public ILocator Tree => Form.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });

    /// <summary>Every control whose label mentions a condition.</summary>
    public ILocator ConditionFields => Form.GetByLabel("Condition");

    /// <summary>The read-only condition an API-authored rule carries.</summary>
    public ILocator Condition => Form.GetByRole(AriaRole.Textbox, new() { Name = "Condition (not evaluated)", Exact = true });

    /// <summary>The warning that conditions are not evaluated.</summary>
    public ILocator ConditionWarning => Form.Locator("[data-lt-condition-warning]");

    /// <summary>Types the rule id.</summary>
    /// <param name="ruleId">The id.</param>
    public Task RuleIdAsync(string ruleId) => Form.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync(ruleId);

    /// <summary>Chooses a stored or directory group as the subject, from the subject picker.</summary>
    /// <param name="group">The group id.</param>
    public async Task SubjectGroupAsync(string group)
    {
        await Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("group");
        await Pickers.PickAsync(page, Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }), group);
        await Expect(Form.Locator(".lt-access-provenance")).ToContainTextAsync(group);
    }

    /// <summary>Chooses a directory user as the subject, from the subject picker.</summary>
    /// <param name="user">The user id.</param>
    public async Task SubjectUserAsync(string user)
    {
        await Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("user");
        await Pickers.PickAsync(page, Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }), user);
    }

    /// <summary>Chooses the scope kind: one of the <c>*Scope</c> constants.</summary>
    /// <param name="scope">The scope value.</param>
    public Task ScopeAsync(string scope) => Form.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true }).SelectOptionAsync(scope);

    /// <summary>Chooses an existing tree from the Tree picker.</summary>
    /// <param name="tree">The tree id.</param>
    public Task PickTreeAsync(string tree) => Pickers.PickAsync(page, Tree, tree);

    /// <summary>
    /// Types a tree the cluster does not hold yet into the Tree picker and leaves the field,
    /// expecting it to be accepted with the not-yet-created flag rather than refused.
    /// </summary>
    /// <param name="tree">The tree id.</param>
    public async Task NewTreeAsync(string tree)
    {
        await Tree.FillAsync(tree);
        await Tree.PressAsync("Tab");
        await Expect(Pickers.FlagOf(Tree)).ToHaveTextAsync($"No tree named {tree} exists yet. The rule applies to it once it is created.");
        await Expect(Pickers.ErrorOf(Tree)).ToHaveCountAsync(0);
    }

    /// <summary>Types the key, or the key prefix.</summary>
    /// <param name="label"><c>Key</c> or <c>Key prefix</c>.</param>
    /// <param name="value">The key or prefix.</param>
    public Task KeyOrPrefixAsync(string label, string value) => Form.GetByRole(AriaRole.Textbox, new() { Name = label, Exact = true }).FillAsync(value);

    /// <summary>Ticks each operation in <paramref name="labels"/>, such as <c>Read</c> and <c>Range read</c>.</summary>
    /// <param name="labels">The operations' labels.</param>
    public async Task OperationsAsync(params string[] labels)
    {
        foreach (var label in labels)
        {
            await Form.GetByLabel(label, new() { Exact = true }).CheckAsync();
        }
    }

    /// <summary>Saves the rule, expecting the dialog to close.</summary>
    public async Task SaveAsync()
    {
        await Form.GetByRole(AriaRole.Button, new() { Name = "Save rule", Exact = true }).ClickAsync();
        await Expect(dialog).ToHaveCountAsync(0, new() { Timeout = 30_000 });
    }

    /// <summary>Cancels, expecting the dialog to close.</summary>
    public async Task CancelAsync()
    {
        await Form.GetByRole(AriaRole.Button, new() { Name = "Cancel", Exact = true }).ClickAsync();
        await Expect(dialog).ToHaveCountAsync(0);
    }
}
