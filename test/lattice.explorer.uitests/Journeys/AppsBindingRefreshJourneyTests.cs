using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5517 (epic #5401): an app made available to a user while their console is
/// open - here an administrator installs the task board with its <c>viewer</c> role
/// bound to a group the user is already in - reaches that console without a reload:
/// Home's Apps line, the spine badge, "Your apps" and the <c>a/</c> completions all
/// follow once the user moves around the console.
/// </summary>
/// <remarks>
/// Each step keeps an ordered screenshot under <c>screenshots/journey-d5517-myapps-stale-binding</c>
/// (or <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AppsBindingRefreshJourneyTests : UiTestBase
{
    // How long the journey keeps moving around the console for the app to appear: well
    // past the capability refresh interval (ten seconds), and a bound, never a pause.
    private static readonly TimeSpan AppearsWithin = TimeSpan.FromSeconds(60);

    private int _shot;

    [Test]
    public async Task An_app_bound_to_the_users_group_while_the_console_is_open_appears_without_a_reload()
    {
        var world = await UiHosts.WorldAsync();

        // The world is shared: start from no install and leave none behind.
        await world.RemoveTaskBoardAsync();
        try
        {
            await BindWhileOpenAsync(world);
        }
        finally
        {
            await world.RemoveTaskBoardAsync();
        }
    }

    private async Task BindWhileOpenAsync(ExplorerWorld world)
    {
        // bob is in task-viewers, but no app is bound to it yet.
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Bob);
        var appsLine = HomeLine(page, "Apps");
        await Expect(appsLine).ToHaveTextAsync("No app is assigned to you yet");
        await Expect(Badge(page)).ToHaveCountAsync(0);
        await ShotAsync(page, "home-no-app-assigned");

        await Shell.Stop(page, "apps").ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Apps");
        await Expect(Shell.Content(page).GetByRole(AriaRole.Heading, new() { Name = "No apps yet" })).ToBeVisibleAsync();
        await ShotAsync(page, "your-apps-empty");

        // Elsewhere, an administrator installs the task board with viewer bound to task-viewers.
        // The page is never reloaded from here on.
        await world.InstallTaskBoardAsync();
        var scripted = await page.EvaluateAsync<string>("() => { window.__ltNoReload = 'kept'; return window.__ltNoReload; }");
        Assert.That(scripted, Is.EqualTo("kept"));

        // bob moves around the console, as he would: Apps, then Home, until Home lists the app.
        var clock = Stopwatch.StartNew();
        var appeared = false;
        while (!appeared && clock.Elapsed < AppearsWithin)
        {
            await Shell.Stop(page, "apps").ClickAsync();
            await Expect(Shell.Heading(page)).ToHaveTextAsync("Apps");
            await Shell.Stop(page, "home").ClickAsync();
            await Expect(Shell.Heading(page)).ToHaveTextAsync("Estate");
            try
            {
                await Expect(appsLine).ToHaveTextAsync("1 app available to you", new() { Timeout = 2_000 });
                appeared = true;
            }
            catch (PlaywrightException)
            {
                // Not yet: move around again.
            }
        }

        if (!appeared)
        {
            await ShotAsync(page, "home-still-no-app-after-a-minute");
            Assert.Fail($"Home still reads '{await appsLine.TextContentAsync()}' {AppearsWithin.TotalSeconds:0} seconds after the binding, without a reload.");
        }

        Assert.That(await page.EvaluateAsync<string>("() => window.__ltNoReload"), Is.EqualTo("kept"), "the page was reloaded");
        await Expect(Badge(page)).ToHaveTextAsync("1");
        await ShotAsync(page, "home-lists-the-app-and-the-badge");

        await Shell.Stop(page, "apps").ClickAsync();
        var row = Shell.Content(page).Locator("tbody tr").Filter(new() { HasText = "Task board" });
        await Expect(row).ToHaveCountAsync(1);
        await Expect(row).ToContainTextAsync("viewer");
        await ShotAsync(page, "your-apps-lists-the-task-board");

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("a/");
        var completion = Shell.Suggestions(page).Locator(".lt-shell-combobox__label", new() { HasTextRegex = new Regex("^a/task-board$") });
        await Expect(completion).ToHaveCountAsync(1);
        await ShotAsync(page, "address-line-completes-a-task-board");

        Assert.That(await page.EvaluateAsync<string>("() => window.__ltNoReload"), Is.EqualTo("kept"), "the page was reloaded");
    }

    private static ILocator HomeLine(IPage page, string area) =>
        Shell.Content(page)
            .Locator("li.lt-shell-estate__stop")
            .Filter(new() { Has = page.Locator(".lt-shell-estate__name", new() { HasTextRegex = new Regex($"^{area}$") }) })
            .Locator("[data-lt-area-status]");

    private static ILocator Badge(IPage page) => Shell.Stop(page, "apps").Locator(".lt-shell-directory__badge");

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's journey screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5517-myapps-stale-binding");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
