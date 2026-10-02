using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Design;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4148: History's As-of field is a date and time picker in UTC, not free text. An
/// operator opens the picker from the keyboard, closes it with Escape, opens it again,
/// takes a quick pick, and shows the key as it stood then: the address carries the
/// instant and the view says what it is showing.
/// </summary>
/// <remarks>Every step waits on what the page shows, never on time.</remarks>
[TestFixture]
[Category("UI")]
public sealed class HistoryAsOfJourneyTests : UiTestBase
{
    [Test]
    public async Task Picking_an_as_of_time_shows_the_key_as_it_stood_then()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?tab=history&key={ControlAlignment.HistoryKey}", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var field = content.GetByRole(AriaRole.Textbox, new() { Name = "As of", Exact = true });
        var toggle = content.GetByRole(AriaRole.Button, new() { Name = "Choose As of from a calendar" });
        var picker = content.GetByRole(AriaRole.Group, new() { Name = "Choose As of", Exact = true });

        await Expect(field).ToHaveAttributeAsync("placeholder", "Latest");
        await Expect(content.Locator(".lt-datetime__zone")).ToHaveTextAsync("UTC");

        // The calendar opens on today, with focus on it; Escape closes it and returns focus.
        await toggle.FocusAsync();
        await page.Keyboard.PressAsync("Enter");
        await Expect(picker).ToBeVisibleAsync();
        await Expect(page.Locator(".lt-datetime__day:focus")).ToHaveAttributeAsync("aria-current", "date");
        await page.Keyboard.PressAsync("Escape");
        await Expect(picker).ToBeHiddenAsync();
        await Expect(toggle).ToBeFocusedAsync();

        await toggle.ClickAsync();
        await picker.GetByRole(AriaRole.Button, new() { Name = "1 hour ago", Exact = true }).ClickAsync();
        await Expect(picker).ToBeHiddenAsync();
        await Expect(field).ToHaveValueAsync(new System.Text.RegularExpressions.Regex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"));
        var picked = await field.InputValueAsync();

        await content.GetByRole(AriaRole.Button, new() { Name = "Show as of", Exact = true }).ClickAsync();

        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("[?&]at=" + System.Text.RegularExpressions.Regex.Escape(Uri.EscapeDataString(picked)) + "$"));
        var readable = picked.Replace('T', ' ').TrimEnd('Z') + " UTC";
        await Expect(content.GetByText("Showing the key as it stood at " + readable)).ToBeVisibleAsync();
        await Expect(field).ToHaveValueAsync(picked);
    }
}
