using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5420 (epic #5401): while a silo is leaving the cluster, the facades answer
/// the Explorer's capability probes with <c>Internal</c>. An administrator who reloads
/// Home then still sees every area on the spine and on Home; the ones the cluster did
/// not answer for say they could not be checked, cannot be opened, can be checked
/// again at once, and come back on their own once the cluster answers.
/// </summary>
/// <remarks>
/// The fault is injected at the world's gRPC surface (<see cref="FacadeFaults"/>), for
/// the Access, Replication status and Backups facades only. Each step keeps an ordered
/// screenshot under <c>screenshots/journey-d5420-transient-area-probe</c> (or
/// <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class TransientAreaProbeJourneyTests : UiTestBase
{
    private static readonly string[] Faulted = ["access", "replication", "backups"];

    private int _shot;

    [Test]
    public async Task An_administrator_reloading_home_during_a_silo_loss_keeps_every_area_and_gets_it_back()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        var content = Shell.Content(page);
        foreach (var area in Faulted)
        {
            await Expect(StateOf(page, area)).ToHaveAttributeAsync("data-lt-area-state", "current");
        }

        await ShotAsync(page, "home-before-the-silo-loss");

        using (world.Faults.Fail("orleans.lattice.api.auth", "orleans.lattice.api.replication.status", "orleans.lattice.api.backup"))
        {
            await page.ReloadAsync();
            await Shell.WaitForInteractiveAsync(page);

            // Every area stays on the spine and on Home, saying it could not be checked.
            foreach (var area in Faulted)
            {
                await Expect(StateOf(page, area)).ToHaveAttributeAsync("data-lt-area-state", "unchecked");
                await Expect(Shell.Stop(page, area)).ToContainTextAsync("Could not be checked");
            }

            var notice = content.Locator("[data-lt-area-recheck]");
            await Expect(notice).ToHaveAttributeAsync("role", "status");
            await Expect(notice).ToContainTextAsync("could not be checked because the cluster did not answer");
            await Expect(content.Locator("li[data-lt-area-state=unchecked]")).ToHaveCountAsync(Faulted.Length);
            await Expect(Shell.Banner(page)).ToContainTextAsync("Connected");
            await ShotAsync(page, "home-during-the-silo-loss");

            // The area's own page stays closed and says why. Check again asks at once,
            // and while the cluster still does not answer, says so again.
            await Shell.Stop(page, "access").ClickAsync();
            await Expect(Shell.Heading(page)).ToHaveTextAsync("Access");
            await Expect(content.Locator(".lt-empty h2")).ToHaveTextAsync("Could not be checked");
            await content.GetByRole(AriaRole.Button, new() { Name = "Check again" }).ClickAsync();
            await Expect(content.Locator(".lt-empty h2")).ToHaveTextAsync("Could not be checked");
            await Expect(content.Locator("[data-lt-area-recheck]")).ToHaveAttributeAsync("data-lt-area-recheck", "scheduled");
            await ShotAsync(page, "access-could-not-be-checked");
        }

        // The cluster answers again: the recheck the manual retry restarted brings the
        // area back without another click.
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Could not be checked" })).ToHaveCountAsync(0);
        await Expect(StateOf(page, "access")).ToHaveAttributeAsync("data-lt-area-state", "current");
        await Expect(Shell.Stop(page, "access")).ToHaveAttributeAsync("aria-current", "page");

        await Shell.Stop(page, "home").ClickAsync();
        foreach (var area in Faulted)
        {
            await Expect(StateOf(page, area)).ToHaveAttributeAsync("data-lt-area-state", "current");
        }

        await Expect(page.Locator("[data-lt-area-recheck]")).ToHaveCountAsync(0);
        await ShotAsync(page, "home-after-the-cluster-answers");
    }

    private static ILocator StateOf(IPage page, string area) =>
        Shell.Directory(page).Locator("li[data-lt-area-state]").Filter(new() { Has = page.Locator($"a[data-lt-command=\"go.{area}\"]") });

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's journey screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5420-transient-area-probe");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
