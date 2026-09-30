using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Apps;

/// <summary>
/// A regression guard for issue #4011: moving between the Apps area's pages - Your apps,
/// the catalogue, an app's review and the app's own page - as fast as the browser can ask,
/// never ends the circuit. A page left while one of its reads was still on its way used to
/// read its disposed cancellation source when the reply arrived, which threw out of a
/// lifecycle method and terminated the circuit: the console stayed on screen, inert.
/// </summary>
/// <remarks>
/// Nothing waits between navigations. The journey then proves the circuit is still live by
/// driving it, and fails on any page error, any console error and any circuit fault the web
/// head logged.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AppsNavigationStressTests : UiTestBase
{
    private const int Rounds = 50;

    private static readonly string[] Pages =
    [
        "/t/default/apps",
        "/apps/catalogue",
        $"/t/default/apps/catalogue/in-image/{TaskBoardApp.Slug}%401.0.0",
        $"/t/default/apps/{TaskBoardApp.Slug}",
    ];

    [Test]
    public async Task Alternating_the_apps_pages_within_one_circuit_never_ends_it()
    {
        var world = await PrepareAsync();
        var page = await OpenAsync(world.Head, "/t/default/apps", WorldIdentities.Admin);
        var faults = Record(page);

        for (var round = 0; round < Rounds; round++)
        {
            foreach (var path in Pages)
            {
                // The circuit's own navigation, as a link click makes it, with no wait for the page.
                await page.EvaluateAsync("href => Blazor.navigateTo(href)", world.Head.Url(path));
            }
        }

        await ExpectLiveAsync(world, page, faults);
    }

    [Test]
    public async Task Loading_the_apps_pages_one_after_another_never_ends_a_circuit()
    {
        var world = await PrepareAsync();
        var page = await OpenAsync(world.Head, "/t/default/apps", WorldIdentities.Admin);
        var faults = Record(page);

        for (var round = 0; round < Rounds; round++)
        {
            foreach (var path in Pages)
            {
                // A fresh document, and so a fresh circuit, each time: the next load starts as
                // soon as this one's circuit is live, whatever the page is still reading.
                await Shell.GotoAsync(page, world.Head, path);
            }
        }

        await ExpectLiveAsync(world, page, faults);
    }

    private static async Task<ExplorerWorld> PrepareAsync()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();

        // Only what this journey provokes is judged.
        world.Head.DescribeFaults();
        return world;
    }

    private static List<string> Record(IPage page)
    {
        var faults = new List<string>();
        page.Console += (_, message) =>
        {
            if (message.Type == "error")
            {
                faults.Add("[console.error] " + message.Text);
            }
        };
        page.PageError += (_, error) => faults.Add("[pageerror] " + error);
        return faults;
    }

    private static async Task ExpectLiveAsync(ExplorerWorld world, IPage page, List<string> faults)
    {
        // Land somewhere known, then drive the circuit: a dead circuit leaves the page on
        // screen and a link click then reloads the document into a new circuit, so the
        // catalogue must render in this same document.
        await page.EvaluateAsync("href => Blazor.navigateTo(href)", world.Head.Url("/t/default/apps"));
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/apps"));
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Apps");
        await page.EvaluateAsync("() => { window.__ltSameDocument = true; }");

        await Shell.Content(page).GetByRole(AriaRole.Link, new() { Name = "Catalogue", Exact = true }).ClickAsync();
        var entry = Shell.Content(page).GetByRole(AriaRole.Row).Filter(new() { HasText = "Task board" });
        await Expect(entry).ToBeVisibleAsync();
        var sameDocument = await page.EvaluateAsync<bool>("() => window.__ltSameDocument === true");

        var server = world.Head.DescribeFaults();
        Assert.Multiple(() =>
        {
            Assert.That(sameDocument, Is.True, "the catalogue rendered in the same circuit, not after a reload");
            Assert.That(faults, Is.Empty, "the browser reported an error");
            Assert.That(server ?? string.Empty, Does.Not.Contain("Unhandled exception"), server);
        });
    }
}
