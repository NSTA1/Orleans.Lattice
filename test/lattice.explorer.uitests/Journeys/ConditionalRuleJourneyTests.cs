using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Auth;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5426 (epic #5401): the authorization engine never evaluates a rule's
/// condition, so the Access rule editor never offers one. A rule written with a
/// condition through the API shows it read-only, warned that it is not evaluated and
/// that the rule applies unconditionally, on its details page and in its editor; and
/// saving it keeps the condition verbatim, with the confirmation repeating the warning.
/// </summary>
/// <remarks>
/// Each step keeps an ordered screenshot under <c>screenshots/journey-d5426-inert-rule-condition</c>
/// (or <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class ConditionalRuleJourneyTests : UiTestBase
{
    private const string RuleId = "d5426-eu-visitors";
    private const string Condition = "region == \"eu\"";
    private const string NotEvaluated = "Conditions are not evaluated in this version. This rule applies unconditionally, as if it had no condition.";

    private int _shot;

    [Test]
    public async Task The_editor_offers_no_condition_and_an_api_authored_one_is_shown_read_only_warned_and_kept()
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
            // A new rule: no condition is offered, so none can be implied.
            var page = await OpenAsync(world.Head, "/access/rules?new=true", WorldIdentities.Admin);
            var editor = page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" }).Locator("form.lt-access-form");
            var save = editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" });
            await Expect(save).ToBeVisibleAsync();
            await Expect(editor.Locator("label", new() { HasText = "Condition" })).ToHaveCountAsync(0);
            await Expect(editor.Locator("[data-lt-condition-warning]")).ToHaveCountAsync(0);
            await save.ScrollIntoViewIfNeededAsync();
            await ShotAsync(page, "new-rule-offers-no-condition");

            // A rule written with a condition through the API: its details warn beside it.
            await Shell.GotoAsync(page, world.Head, $"/access/rules/{RuleId}?tree={ExplorerWorld.DemoTree}");
            var content = Shell.Content(page);
            await Expect(page.GetByRole(AriaRole.Heading, new() { Name = RuleId, Level = 1 })).ToBeVisibleAsync();
            await Expect(content.Locator(".lt-dl [data-lt-condition]")).ToHaveTextAsync(Condition);
            await Expect(content.Locator(".lt-dl [data-lt-condition-warning]")).ToHaveTextAsync(NotEvaluated);
            await ShotAsync(page, "details-warn-the-condition-is-not-evaluated");

            // Its editor shows the condition read-only, with the same warning.
            await content.GetByRole(AriaRole.Button, new() { Name = "Edit rule" }).ClickAsync();
            var edit = page.GetByRole(AriaRole.Dialog, new() { Name = "Edit rule" }).Locator("form.lt-access-form");
            var condition = edit.GetByRole(AriaRole.Textbox, new() { Name = "Condition (not evaluated)", Exact = true });
            await Expect(condition).ToHaveValueAsync(Condition);
            await Expect(condition).Not.ToBeEditableAsync();
            await Expect(edit.Locator("[data-lt-condition-warning]")).ToHaveTextAsync(NotEvaluated);
            await edit.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ScrollIntoViewIfNeededAsync();
            await ShotAsync(page, "editor-shows-the-condition-read-only");

            // Saving keeps it, and the confirmation repeats the warning.
            await edit.GetByLabel("Write", new() { Exact = true }).CheckAsync();
            await edit.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
            var confirmation = Shell.ToastMessages(page).Filter(new() { HasText = $"Rule {RuleId} saved." });
            await Expect(confirmation).ToContainTextAsync(NotEvaluated);
            await Expect(Shell.Toasts(page).Filter(new() { HasText = $"Rule {RuleId} saved." })).ToHaveAttributeAsync("data-lt-tone", "warning");
            await ShotAsync(page, "saved-confirmation-repeats-the-warning");

            // Read back from the cluster: the condition is exactly as written.
            await page.ReloadAsync();
            await Expect(content.Locator(".lt-dl")).ToContainTextAsync(new Regex("Read, Write"));
            await Expect(content.Locator(".lt-dl [data-lt-condition]")).ToHaveTextAsync(Condition);
            await Expect(content.Locator(".lt-dl [data-lt-condition-warning]")).ToHaveTextAsync(NotEvaluated);
            await ShotAsync(page, "condition-kept-verbatim-after-save");
        }
        finally
        {
            await world.RemoveRuleAsync(ExplorerWorld.DemoTree, RuleId);
        }
    }

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's journey screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5426-inert-rule-condition");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
