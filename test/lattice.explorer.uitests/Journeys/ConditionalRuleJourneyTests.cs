using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Auth;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5426 (epic #5401): the rule editor's condition field, as the console shows it
/// today, captured for the pull request's before screenshots.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ConditionalRuleJourneyTests : UiTestBase
{
    private const string RuleId = "d5426-eu-visitors";
    private const string Condition = "region == \"eu\"";

    private int _shot;

    [Test]
    public async Task Capture_the_condition_field_before_the_fix()
    {
        var world = await UiHosts.WorldAsync();
        await world.PutRuleAsync(new LatticeAuthorizationRule(
            RuleId,
            LatticeSubjectSelector.Group(WorldIdentities.VisitorsGroup),
            LatticeScope.Tree(ExplorerWorld.DemoTree),
            LatticeOperation.Read,
            LatticeEffect.Allow,
            Condition));
        try
        {
            var page = await OpenAsync(world.Head, "/access/rules?new=true", WorldIdentities.Admin);
            var editor = page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" }).Locator("form.lt-access-form");
            var save = editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" });
            await Expect(save).ToBeVisibleAsync();
            await save.ScrollIntoViewIfNeededAsync();
            await ShotAsync(page, "before-new-rule-editor");

            await Shell.GotoAsync(page, world.Head, $"/access/rules/{RuleId}?tree={ExplorerWorld.DemoTree}");
            var content = Shell.Content(page);
            await Expect(page.GetByRole(AriaRole.Heading, new() { Name = RuleId, Level = 1 })).ToBeVisibleAsync();
            await Expect(content.Locator(".lt-dl")).ToContainTextAsync(Condition);
            await ShotAsync(page, "before-conditional-rule-details");

            await content.GetByRole(AriaRole.Button, new() { Name = "Edit rule" }).ClickAsync();
            var edit = page.GetByRole(AriaRole.Dialog, new() { Name = "Edit rule" }).Locator("form.lt-access-form");
            var editSave = edit.GetByRole(AriaRole.Button, new() { Name = "Save rule" });
            await Expect(editSave).ToBeVisibleAsync();
            await editSave.ScrollIntoViewIfNeededAsync();
            await ShotAsync(page, "before-conditional-rule-editor");
            await Expect(page).ToHaveURLAsync(new Regex(Regex.Escape(RuleId)));
        }
        finally
        {
            await world.RemoveRuleAsync(ExplorerWorld.DemoTree, RuleId);
        }
    }

    private async Task ShotAsync(IPage page, string name)
    {
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5426-inert-rule-condition");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
