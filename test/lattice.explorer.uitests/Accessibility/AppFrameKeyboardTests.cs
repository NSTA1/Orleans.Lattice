using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Keyboard focus entering and leaving an app frame: from the host's "Leave app" control
/// Tab moves into the frame and reaches the app's first control, Shift+Tab from the
/// frame's first control comes back out, and "Leave app" is always reachable.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AppFrameKeyboardTests : UiTestBase
{
    [Test]
    public async Task Keyboard_focus_enters_and_leaves_an_app_frame()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Alice);
        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var board = AppFrames.Content(page);
        await Expect(board.Locator("#tb-add-title")).ToBeVisibleAsync();

        // In: from the Leave control before the frame, Tab lands inside the app.
        var leaveBefore = page.Locator("[data-appframe-leave=\"before\"]");
        await leaveBefore.FocusAsync();
        await page.Keyboard.PressAsync("Tab");
        await Expect(AppFrames.Element(page)).ToBeFocusedAsync();
        var frame = await AppFrames.DocumentAsync(page);
        for (var i = 0; i < 4 && await frame.EvaluateAsync<string>("() => document.activeElement?.id ?? ''") != "tb-add-title"; i++)
        {
            await page.Keyboard.PressAsync("Tab");
        }

        Assert.That(await frame.EvaluateAsync<string>("() => document.activeElement?.id ?? ''"), Is.EqualTo("tb-add-title"),
            "Tabbing into the frame never reached the app's first control.");

        // Out: Shift+Tab from the app's first control returns to the host's Leave control.
        for (var i = 0; i < 4 && !await leaveBefore.EvaluateAsync<bool>("e => e === document.activeElement"); i++)
        {
            await page.Keyboard.PressAsync("Shift+Tab");
        }

        await Expect(leaveBefore).ToBeFocusedAsync();

        // And the way out is always there: Leave app goes back to the app's overview.
        await page.Keyboard.PressAsync("Enter");
        await Expect(AppFrames.Element(page)).ToHaveCountAsync(0);
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("/apps/task-board/overview$"));
    }
}
