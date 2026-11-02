using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5478 (epic #5401): the Enrolled trees page offers <b>Enable replication for a
/// tree</b> only to a caller who holds the Replication capability. An observer with read
/// and telemetry rights sees the empty list and no control; the administrator on the
/// same cluster, with nothing enrolled yet, is offered the first enrolment.
/// </summary>
/// <remarks>
/// Each step keeps an ordered screenshot under <c>screenshots/journey-d5478-replication-enable</c>
/// (or <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class ReplicationEnrolmentJourneyTests : UiTestBase
{
    private const string EnableControl = "Enable replication for a tree";

    private int _shot;

    [Test]
    public async Task Only_a_caller_with_the_Replication_grant_is_offered_enrolment()
    {
        var world = await UiHosts.WorldAsync();

        var observer = await OpenAsync(world.Head, "/replication/trees", WorldIdentities.Erin);
        await ExpectSettledAsync(observer);
        await Expect(Shell.Content(observer).GetByText("No tree you may manage is enrolled for replication.")).ToBeVisibleAsync();
        await Expect(Shell.Content(observer).GetByRole(AriaRole.Button, new() { Name = EnableControl })).ToHaveCountAsync(0);
        await ShotAsync(observer, "observer-enrolled-trees");

        var admin = await OpenAsync(world.Head, "/replication/trees", WorldIdentities.Admin);
        await ExpectSettledAsync(admin);
        await Expect(Shell.Content(admin).GetByRole(AriaRole.Button, new() { Name = EnableControl })).ToBeVisibleAsync();
        await ShotAsync(admin, "administrator-enrolled-trees");
    }

    private static async Task ExpectSettledAsync(IPage page)
    {
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Enrolled trees");
        await Expect(page.Locator(".lt-table__empty, .lt-empty, tbody tr").First).ToBeVisibleAsync();
    }

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5478-replication-enable");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
