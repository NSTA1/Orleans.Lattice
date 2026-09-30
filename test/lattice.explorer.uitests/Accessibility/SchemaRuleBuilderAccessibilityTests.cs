using Microsoft.Extensions.DependencyInjection;
using Microsoft.Playwright;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Issue #3963: the schema rule builder with its composer open - shape tree,
/// constraint gallery, details and the live sample check - is held to the same bar
/// as every page: the axe sweep in all eight appearances, the chosen member still
/// marked under forced colours, and no horizontal page scroll at 360 pixels.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class SchemaRuleBuilderAccessibilityTests : UiTestBase
{
    private static readonly string EditorPage = $"/schema/{ExplorerWorld.OrdersTree}";

    [Test]
    public async Task The_open_rule_builder_has_no_serious_violations_in_any_appearance()
    {
        var world = await UiHosts.WorldAsync();
        await world.Head.Services.GetRequiredService<ILatticeSchemaAdmin>().ClearPolicyAsync(ExplorerWorld.OrdersTree);
        var page = await OpenAsync(world.Head, EditorPage, WorldIdentities.Admin);
        await OpenComposerAsync(page);

        foreach (var appearance in ShellAppearanceChoice.All)
        {
            await Shell.SetAppearanceAsync(page, appearance);
            await Shell.WaitForMotionToSettleAsync(page);
            await AxeConformance.SweepAsync(page, $"the schema rule builder on {EditorPage} in {appearance}");
        }
    }

    [Test]
    public async Task Forced_colours_keep_the_chosen_member_marked()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, EditorPage, WorldIdentities.Admin, configure: options => options.ForcedColors = ForcedColors.Active);
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(forced-colors: active)').matches"), Is.True,
            "The premise failed: the browser does not report forced colours.");
        await OpenComposerAsync(page);

        var chosen = Shell.Content(page).Locator(".lt-schema-shape__member[aria-pressed=\"true\"]");
        await Expect(chosen).ToHaveCountAsync(1);
        var node = await chosen.EvaluateAsync<string>("m => { const s = getComputedStyle(m, '::before'); return s.backgroundColor + ' ' + s.forcedColorAdjust; }");
        Assert.That(node, Does.EndWith(" none"), $"Under forced colours the chosen member's node must keep its fill ({node}).");
        await AxeConformance.SweepAsync(page, "the schema rule builder under forced colours");
    }

    [Test]
    public async Task The_open_rule_builder_reflows_on_a_phone()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, EditorPage, WorldIdentities.Admin, Shell.SmallWidth);
        await OpenComposerAsync(page);

        await Shell.AssertNoHorizontalPageScrollAsync(page, $"the schema rule builder at {Shell.SmallWidth}px");
    }

    private static async Task OpenComposerAsync(IPage page)
    {
        var content = Shell.Content(page);
        var open = content.GetByRole(AriaRole.Button, new() { NameRegex = new System.Text.RegularExpressions.Regex("^(Set a policy|Edit policy)$") });
        await Expect(open).ToBeVisibleAsync();
        await open.ClickAsync();
        await Expect(content.Locator(".lt-schema-rulebuilder__aside")).ToContainTextAsync("Checked");
        await content.GetByRole(AriaRole.Button, new() { Name = "Add a rule", Exact = true }).ClickAsync();
        await content.Locator(".lt-schema-shape__member").Filter(new() { HasText = "total" }).ClickAsync();
        await Expect(content.Locator(".lt-schema-shape__member[aria-pressed=\"true\"]")).ToHaveCountAsync(1);
        await Shell.WaitForMotionToSettleAsync(page);
    }
}
