using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5428, in a real browser against the real state gRPC binding: a user reading a
/// tree loses their grant on it while its Keys tab is open. Re-opening the live feed is
/// refused for that tree alone (the state API answers <c>NotFound</c>, as it does for a
/// tree the caller cannot see), so the Keys tab says so and the banner's connection pill
/// stays Connected: one tree's answer is not reported as the cluster disconnecting.
/// </summary>
/// <remarks>
/// Each step saves an ordered screenshot under <c>screenshots/per-call-refusal-connection/</c>,
/// which the UI lane uploads, for the pull request's journey section. The tree and its
/// grant are the test's own, so the shared world's other trees and rules are untouched.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class PerCallRefusalConnectionJourneyTests : UiTestBase
{
    [Test]
    public async Task A_grant_revoked_mid_session_leaves_the_connection_pill_connected()
    {
        var world = await UiHosts.WorldAsync();
        var treeId = "journey-revoke-" + Guid.NewGuid().ToString("N")[..8];
        var ruleId = "carol-read-" + treeId;
        await world.SeedTreeAsync(treeId, entries: 6);
        await world.GrantReadAsync(WorldIdentities.Carol, treeId, ruleId);

        var page = await OpenAsync(world.Head, "/data/" + treeId, WorldIdentities.Carol);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(treeId);
        await Expect(Shell.Content(page).GetByRole(AriaRole.Link, new() { Name = "key-000" })).ToBeVisibleAsync();
        var pill = ConnectionPill(page);
        await Expect(pill).ToHaveTextAsync("Connected");
        await CaptureAsync(page, "01-reading-the-tree-pill-connected");

        await world.RevokeAsync(treeId, ruleId);
        await WaitUntilRevokedAsync(world, treeId);

        // Re-open the live feed: the state API now refuses it for this tree.
        var live = Shell.Content(page).GetByRole(AriaRole.Switch, new() { Name = "Live updates" });
        await live.ClickAsync();
        await Expect(live).ToHaveAttributeAsync("aria-checked", "false");
        await live.ClickAsync();

        var readings = await SamplePillAsync(page, pill, TimeSpan.FromSeconds(3));
        await Expect(Shell.Content(page).Locator(".lt-data-note")).ToContainTextAsync("no longer exists, or you cannot see it");
        await CaptureAsync(page, "02-live-feed-refused-pill-stays-connected");

        Assert.That(readings, Has.All.EqualTo("Connected"), "a refusal of one tree's feed must never move the connection pill");
    }

    private static ILocator ConnectionPill(IPage page) =>
        Shell.Banner(page).Locator("[role=status] > .lt-pill .lt-pill__text").First;

    /// <summary>
    /// Samples the pill every 100 ms. The first reading that is not Connected is
    /// captured as it shows, because the health probe restores the pill about a
    /// second later.
    /// </summary>
    private static async Task<List<string>> SamplePillAsync(IPage page, ILocator pill, TimeSpan window)
    {
        var deadline = DateTimeOffset.UtcNow + window;
        var readings = new List<string>();
        var captured = false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var reading = (await pill.InnerTextAsync()).Trim();
            readings.Add(reading);
            if (!captured && reading != "Connected")
            {
                captured = true;
                await page.ScreenshotAsync(new PageScreenshotOptions { Path = ShotPath("02-defect-pill-left-connected"), FullPage = false });
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        return readings;
    }

    // The grant is evaluated by the cluster, not the browser: wait until it answers the
    // revocation before driving the page, so the step is not a race with the policy store.
    private static async Task WaitUntilRevokedAsync(ExplorerWorld world, string treeId)
    {
        var data = world.Data(WorldIdentities.Carol);
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                if (!(await data.GetAsync(new Api.Data.Grpc.DataGetRequest { TreeId = treeId, Key = "key-000" })).Found)
                {
                    return;
                }
            }
            catch (Exception)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200));
        }

        Assert.Fail("The cluster still admitted the revoked grant after 15 seconds.");
    }

    private static string ShotPath(string step)
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "per-call-refusal-connection");
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, step + ".png");
    }

    private static async Task CaptureAsync(IPage page, string step)
    {
        var path = ShotPath(step);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = path, FullPage = false });
    }
}
