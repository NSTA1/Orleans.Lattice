using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// The Access area's explain page (<c>/access/explain</c>) as olivia drives it: a user, an
/// operation and a tree, and the cluster's verdict with the rules it matched. Each method
/// records its coverage hits only after its assertion holds.
/// </summary>
/// <param name="session">The session driving the page.</param>
internal sealed class ExplainPage(PersonaSession session)
{
    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    private ILocator Form => Content.Locator("form.lt-access-form");

    /// <summary>The verdict section.</summary>
    public ILocator Verdict => Content.Locator("section.lt-access-section").Filter(new() { Has = Page.Locator("#lt-access-verdict") });

    /// <summary>Opens the explain page through the command palette's <b>Explain access...</b>.</summary>
    public async Task OpenFromPaletteAsync()
    {
        await session.Shell.RunCommandAsync("Explain access...");
        await Expect(Page).ToHaveURLAsync(new Regex("/access/explain$"));
        await Expect(Form.GetByRole(AriaRole.Button, new() { Name = "Explain", Exact = true })).ToBeVisibleAsync();
        Coverage.Hit("command:access.explain");
        Coverage.Hit("route:/access/explain");
        Coverage.Hit("subroute:access/explain");
    }

    /// <summary>Asks whether a user may perform an operation on a whole tree.</summary>
    /// <param name="user">The user, chosen from the identity directory.</param>
    /// <param name="operation">The operation's label, such as <c>Write</c>.</param>
    /// <param name="tree">The tree, which need not be created yet.</param>
    /// <param name="asked">Runs once the question is filled in, before it is asked: its screenshot.</param>
    public async Task AskAsync(string user, string operation, string tree, Func<Task>? asked = null)
    {
        await Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("user");
        await A2Pickers.ChooseAsync(Page, Form.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }), user);
        await Form.GetByRole(AriaRole.Combobox, new() { Name = "Operation", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = operation });
        await Form.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true }).SelectOptionAsync("tree");
        var box = Form.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await A2Pickers.NameAsync(box, tree);
        await Expect(box).Not.ToHaveAttributeAsync("aria-invalid", "true");
        if (asked is not null)
        {
            await asked();
        }

        await Form.GetByRole(AriaRole.Button, new() { Name = "Explain", Exact = true }).ClickAsync();
        await Expect(Verdict).ToBeVisibleAsync();
        await Expect(Verdict.Locator("dl")).ToContainTextAsync(user);
        await Expect(Verdict.Locator("dl")).ToContainTextAsync(tree);
    }

    /// <summary>Asserts the verdict was <b>Allowed</b> by <paramref name="ruleId"/>.</summary>
    /// <param name="ruleId">The deciding rule.</param>
    public async Task ExpectAllowedByAsync(string ruleId)
    {
        await Expect(Verdict.Locator("#lt-access-verdict")).ToHaveTextAsync("Allowed");
        await Expect(Verdict.Locator(".lt-table-frame, .lt-table-list").First).ToContainTextAsync(ruleId);
        Coverage.Hit("use-case:access/explain");
    }

    /// <summary>Asserts the verdict was <b>Denied</b>.</summary>
    public async Task ExpectDeniedAsync()
    {
        await Expect(Verdict.Locator("#lt-access-verdict")).ToHaveTextAsync("Denied");
        Coverage.Hit("use-case:access/explain");
    }

    /// <summary>Asserts the verdict was <b>Denied</b> by the default, because no rule matched.</summary>
    public async Task ExpectNoMatchingRuleAsync()
    {
        await ExpectDeniedAsync();
        await Expect(Verdict).ToContainTextAsync("No rule matched; the default applied.");
    }
}
