using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// The Access area's rule pages (<c>/access</c>, <c>/access/rules</c> and
/// <c>/access/rules/{id}</c>) as olivia drives them: the posture banner, the New rule
/// editor, and one rule's Edit rule and Delete rule. Each method records its coverage hits
/// only after its assertion holds.
/// </summary>
/// <param name="session">The session driving the pages.</param>
internal sealed class RulesPage(PersonaSession session)
{
    /// <summary>The checklist's label of each operation the story grants, in the editor's order.</summary>
    private static readonly (LatticeOperation Flag, string Label)[] OperationLabels =
    [
        (LatticeOperation.Read, "Read"),
        (LatticeOperation.Write, "Write"),
        (LatticeOperation.Delete, "Delete"),
        (LatticeOperation.RangeRead, "Range read"),
        (LatticeOperation.RangeDelete, "Range delete"),
        (LatticeOperation.CrdtApply, "CRDT apply"),
        (LatticeOperation.AtomicWrite, "Atomic write"),
        (LatticeOperation.BulkLoad, "Bulk load"),
        (LatticeOperation.Admin, "Admin"),
        (LatticeOperation.Backup, "Backup"),
        (LatticeOperation.Restore, "Restore"),
        (LatticeOperation.SchemaAdmin, "Schema admin"),
        (LatticeOperation.Replication, "Replication"),
        (LatticeOperation.TreeLifecycle, "Tree lifecycle"),
        (LatticeOperation.Telemetry, "Telemetry"),
        (LatticeOperation.AppInstall, "App install"),
    ];

    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    /// <summary>The access posture banner.</summary>
    public ILocator Banner => Content.Locator("dl.lt-access-banner");

    /// <summary>The New rule editor.</summary>
    public ILocator NewRuleDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" });

    /// <summary>The Edit rule editor.</summary>
    public ILocator EditRuleDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "Edit rule" });

    /// <summary>The rule list (or, on a rule's page, nothing).</summary>
    public ILocator Table => Content.Locator(".lt-table-frame, .lt-table-list").First;

    /// <summary>
    /// Arrives in Access from the spine: it opens on Rules, under the posture banner, which
    /// says rules are enforced and names the identity directory.
    /// </summary>
    public async Task ArriveAsync()
    {
        await session.Shell.GoToAreaAsync("access");
        await Expect(Page).ToHaveURLAsync(new Regex("/access$"));
        await session.Shell.ExpectHeadingAsync("Access");
        await Expect(Banner).ToBeVisibleAsync();
        await Expect(Banner).ToContainTextAsync("Enforced");
        await Expect(Banner).ToContainTextAsync("static");
        await Expect(Content.GetByRole(AriaRole.Navigation, new() { Name = "Access" }).GetByRole(AriaRole.Link, new() { Name = "Rules", Exact = true }))
            .ToHaveAttributeAsync("aria-current", "page");
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "New rule", Exact = true })).ToBeEnabledAsync();
        Coverage.Hit("route:/access");
        Coverage.Hit("use-case:access/area");
    }

    /// <summary>Opens the rule list from the Access area's own navigation.</summary>
    public async Task OpenAsync()
    {
        await Content.GetByRole(AriaRole.Navigation, new() { Name = "Access" }).GetByRole(AriaRole.Link, new() { Name = "Rules", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/access/rules$"));
        await Expect(Banner).ToBeVisibleAsync();
        await Expect(Content.Locator(".lt-access-count[role=status]")).ToBeVisibleAsync();
        Coverage.Hit("route:/access/rules");
        Coverage.Hit("subroute:access/rules");
    }

    /// <summary>Opens the New rule editor through the command palette's <b>Create an access rule</b>.</summary>
    public async Task OpenCreateFromPaletteAsync()
    {
        await session.Shell.RunCommandAsync("Create an access rule");
        await Expect(Page).ToHaveURLAsync(new Regex(@"/access/rules\?new=true$"));
        await Expect(NewRuleDialog).ToBeVisibleAsync();
        Coverage.Hit("command:access.create-rule");
        Coverage.Hit("query:access.new");
        Coverage.Hit("query-value:access.new=true");
    }

    /// <summary>Opens the New rule editor with the list's <b>New rule</b> button.</summary>
    public async Task OpenCreateAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "New rule", Exact = true }).ClickAsync();
        await Expect(NewRuleDialog).ToBeVisibleAsync();
    }

    /// <summary>
    /// Fills the New rule editor with one record of a pinned rule: an Allow for its group,
    /// chosen in the subject picker among the stored groups; its one tree (or the cluster-wide
    /// scope); and its operations.
    /// </summary>
    /// <param name="rule">The record.</param>
    public async Task FillAsync(PinnedRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var editor = NewRuleDialog.Locator("form.lt-access-form");
        await Expect(editor.GetByRole(AriaRole.Combobox, new() { Name = "Effect", Exact = true })).ToHaveValueAsync("allow");
        await editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("group");
        await A2Pickers.ChooseAsync(Page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }), rule.Subject.Id);
        await Expect(editor.Locator("[data-lt-provenance=stored]")).ToContainTextAsync(rule.Subject.Id);
        Coverage.Hit("use-case:access/subject-picker");

        if (rule.TreeId == LatticeScope.ClusterWideTreeId)
        {
            await editor.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true }).SelectOptionAsync("cluster");
            await Expect(editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true })).ToHaveCountAsync(0);
        }
        else
        {
            await Expect(editor.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true })).ToHaveValueAsync("tree");
            var tree = editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
            await A2Pickers.NameAsync(tree, rule.TreeId);
            await Expect(tree).Not.ToHaveAttributeAsync("aria-invalid", "true");
        }

        // The id last: the field checks it against the rules already governing the chosen tree.
        var id = editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true });
        await id.FillAsync(rule.RuleId);
        await Expect(id).ToHaveValueAsync(rule.RuleId);

        foreach (var (flag, label) in OperationLabels)
        {
            var box = editor.GetByLabel(label, new() { Exact = true });
            if (rule.Operations.HasFlag(flag))
            {
                await box.CheckAsync();
            }
            else
            {
                await Expect(box).Not.ToBeCheckedAsync();
            }
        }
    }

    /// <summary>Saves the New rule editor and waits for the rule in the list.</summary>
    /// <param name="rule">The record it holds.</param>
    public async Task SaveAsync(PinnedRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        await NewRuleDialog.GetByRole(AriaRole.Button, new() { Name = "Save rule", Exact = true }).ClickAsync();
        await Expect(NewRuleDialog).ToBeHiddenAsync();
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"Rule {rule.RuleId} saved.");
        await Expect(Table).ToContainTextAsync(rule.RuleId);
        Coverage.Hit("use-case:access/rule-create-edit-delete");
    }

    /// <summary>Narrows the list with its search to <paramref name="text"/>.</summary>
    /// <param name="text">What to search for.</param>
    /// <param name="rows">How many rule records then match.</param>
    public async Task SearchAsync(string text, int rows)
    {
        await Content.GetByRole(AriaRole.Searchbox, new() { Name = "Search rules" }).FillAsync(text);
        await Expect(Content.Locator(".lt-access-count[role=status]")).ToHaveTextAsync(new Regex($"^{rows}( of \\d+)? rules"));
    }

    /// <summary>Clears the list's search.</summary>
    public async Task ClearSearchAsync()
    {
        await Content.GetByRole(AriaRole.Searchbox, new() { Name = "Search rules" }).FillAsync(string.Empty);
        await Expect(Content.Locator(".lt-access-count[role=status]")).ToHaveTextAsync(new Regex(@"^\d+ rules"));
    }

    /// <summary>Opens a listed rule's page.</summary>
    /// <param name="ruleId">The rule id.</param>
    public async Task OpenRuleAsync(string ruleId)
    {
        await A2Pickers.FollowRowAsync(Page, Table, ruleId, await session.Shell.IsCompactAsync(), "Open rule");
        await Expect(Page).ToHaveURLAsync(new Regex($"/access/rules/{Regex.Escape(ruleId)}(\\?|$)"));
        await session.Shell.ExpectHeadingAsync(ruleId);
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Edit rule", Exact = true })).ToBeVisibleAsync();
        Coverage.Hit("route:/access/rules/{p1}");
    }

    /// <summary>Opens <b>Edit rule</b> on a rule's page and ticks one more operation.</summary>
    /// <param name="label">The operation's checklist label.</param>
    public async Task EditAddOperationAsync(string label)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Edit rule", Exact = true }).ClickAsync();
        await Expect(EditRuleDialog).ToBeVisibleAsync();
        var editor = EditRuleDialog.Locator("form.lt-access-form");
        await Expect(editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true })).Not.ToBeEditableAsync();
        await editor.GetByLabel(label, new() { Exact = true }).CheckAsync();
    }

    /// <summary>Saves the Edit rule editor; the rule's page then lists the operation.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="label">The operation added.</param>
    public async Task SaveEditAsync(string ruleId, string label)
    {
        await EditRuleDialog.GetByRole(AriaRole.Button, new() { Name = "Save rule", Exact = true }).ClickAsync();
        await Expect(EditRuleDialog).ToBeHiddenAsync();
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"Rule {ruleId} saved.");
        await Expect(Content.Locator("dl")).ToContainTextAsync(label);
    }

    /// <summary>Deletes the rule whose page is open, through its destructive confirmation.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="armed">Runs once the confirmation is armed: its screenshot.</param>
    public async Task DeleteAsync(string ruleId, Func<Task> armed)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true }).ClickAsync();
        await A2Pickers.ConfirmDestructiveAsync(Page, "Delete this rule", "Rule name", ruleId, "Delete rule", armed);
        await Expect(Page).ToHaveURLAsync(new Regex("/access/rules$"));
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"Rule {ruleId} deleted.");
        await Expect(Content.Locator(".lt-access-count[role=status]")).ToBeVisibleAsync();
        await Expect(Table).Not.ToContainTextAsync(ruleId);
        Coverage.Hit("use-case:access/rule-create-edit-delete");
        Coverage.Hit("use-case:access/pages-and-actions");
    }
}
