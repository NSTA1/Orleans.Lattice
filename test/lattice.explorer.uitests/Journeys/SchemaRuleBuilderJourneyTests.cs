using Microsoft.Extensions.DependencyInjection;
using Microsoft.Playwright;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #3963: the schema rule builder, by the keyboard alone. Two rules are added
/// without typing a regular expression - a number range on a member picked by its
/// path, and a common format - the live check against the tree's sample shows the one
/// order that fails, and saving warns about it before the policy is set.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class SchemaRuleBuilderJourneyTests : UiTestBase
{
    [Test]
    public async Task Two_rules_are_added_by_the_keyboard_alone_a_failing_sample_is_seen_and_the_policy_saved()
    {
        var world = await UiHosts.WorldAsync();
        await ResetAsync(world);
        var page = await OpenAsync(world.Head, $"/schema/{ExplorerWorld.OrdersTree}", WorldIdentities.Admin);
        var content = Shell.Content(page);

        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Set a policy", Exact = true }), "Enter");
        await Expect(content.Locator(".lt-schema-rulebuilder__aside")).ToContainTextAsync("Checked");

        await AddRuleAsync(page, "total", arrowsToKind: 3);
        await FillAsync(page, "Smallest", "0");
        await FillAsync(page, "Largest", string.Empty);
        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Add rule", Exact = true }), "Enter");
        await Expect(RuleSentences(page)).ToHaveCountAsync(1);
        await Expect(RuleSentences(page).First).ToContainTextAsync("total must be a number of at least 0");

        await AddRuleAsync(page, "email", arrowsToKind: 5);
        await Expect(content.Locator(".lt-schema-regex__pattern")).ToHaveTextAsync("^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}$");
        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Add rule", Exact = true }), "Enter");
        await Expect(RuleSentences(page)).ToHaveCountAsync(2);
        await Expect(RuleSentences(page).Nth(1)).ToContainTextAsync("email must be an email address");

        var aside = content.Locator(".lt-schema-rulebuilder__aside");
        await Expect(aside).ToContainTextAsync("1 would fail");
        await Expect(aside.Locator(".lt-schema-failures__item code")).ToHaveTextAsync("order/0003");

        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Save policy", Exact = true }), "Enter");
        var warning = page.GetByRole(AriaRole.Alertdialog);
        await Expect(warning).ToContainTextAsync("1 of 5 sampled values fail the new rules.");
        await Expect(warning.GetByRole(AriaRole.Link, new() { Name = "Plan a remediation" })).ToBeVisibleAsync();
        await PressAsync(warning.GetByRole(AriaRole.Button, new() { Name = "Save anyway", Exact = true }), "Enter");

        await Expect(Shell.ToastMessages(page)).ToContainTextAsync($"The policy of {ExplorerWorld.OrdersTree} is saved.");
        await Expect(content.Locator("[role=tabpanel] tbody tr")).ToHaveCountAsync(2);

        var saved = await Admin(world).GetPolicyAsync(ExplorerWorld.OrdersTree);
        Assert.That(saved!.Rules.Select(rule => rule.Kind), Is.EqualTo(new[] { LatticeSchemaRuleKind.Structured, LatticeSchemaRuleKind.Regex }));
        await ResetAsync(world);
    }

    private static ILatticeSchemaAdmin Admin(ExplorerWorld world) => world.Head.Services.GetRequiredService<ILatticeSchemaAdmin>();

    private static Task ResetAsync(ExplorerWorld world) => Admin(world).ClearPolicyAsync(ExplorerWorld.OrdersTree);

    private static ILocator RuleSentences(IPage page) => Shell.Content(page).Locator(".lt-schema-ruleset__rule > .lt-schema-ruleset__sentence");

    private static async Task AddRuleAsync(IPage page, string member, int arrowsToKind)
    {
        var content = Shell.Content(page);
        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Add a rule", Exact = true }), "Enter");
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Add a rule", Exact = true })).ToBeFocusedAsync();

        var path = content.GetByRole(AriaRole.Combobox, new() { Name = "Member path", Exact = true });
        await path.FocusAsync();
        await path.FillAsync(member);
        await page.Keyboard.PressAsync("Escape");

        // The gallery is a radio group: arrow keys move the choice, as for any radio group.
        var first = content.GetByRole(AriaRole.Radio, new() { Name = "Required", Exact = false }).First;
        await first.FocusAsync();
        for (var step = 0; step < arrowsToKind; step++)
        {
            await page.Keyboard.PressAsync("ArrowDown");
        }

        await Expect(content.Locator("input[name='schema-kind-root']:checked")).ToHaveCountAsync(1);
    }

    private static async Task FillAsync(IPage page, string label, string value)
    {
        var field = Shell.Content(page).GetByLabel(label, new() { Exact = true });
        await field.FocusAsync();
        await field.FillAsync(value);
    }

    private static async Task PressAsync(ILocator control, string key)
    {
        await Expect(control).ToBeVisibleAsync();
        await control.FocusAsync();
        await control.PressAsync(key);
    }
}
