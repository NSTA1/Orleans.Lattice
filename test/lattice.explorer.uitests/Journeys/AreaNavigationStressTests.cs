using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A regression guard for issue #4011 across the whole console: moving between every
/// area's main pages - Home, and each shown area's landing page and a deeper page - as fast
/// as the browser can ask never ends the circuit. A page left while one of its reads was
/// still on its way used to read its disposed cancellation source when the reply arrived,
/// or to declare the next page not found, and the first of those terminated the circuit.
/// </summary>
/// <remarks>
/// Nothing waits between navigations. The journey then proves the circuit is still live by
/// driving it, and fails on any page error, any console error and any circuit fault the web
/// head logged.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AreaNavigationStressTests : UiTestBase
{
    private const int CircuitRounds = 50;
    private const int DocumentRounds = 3;

    private static readonly string[] Pages =
    [
        "/t/default",
        .. ExplorerAreas.Shown.SelectMany(area => new[] { area.PrimaryPath, area.DeepPath }.Select(path => Canonical(area, path))),
    ];

    [Test]
    public async Task Alternating_every_areas_pages_within_one_circuit_never_ends_it()
    {
        var world = await PrepareAsync();
        var page = await OpenAsync(world.Head, "/t/default", WorldIdentities.Admin);
        var faults = Record(page);

        for (var round = 0; round < CircuitRounds; round++)
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
    public async Task Loading_every_areas_pages_one_after_another_never_ends_a_circuit()
    {
        var world = await PrepareAsync();
        var page = await OpenAsync(world.Head, "/t/default", WorldIdentities.Admin);
        var faults = Record(page);

        for (var round = 0; round < DocumentRounds; round++)
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

    private static string Canonical(ExplorerArea area, string path) => area.TenantScoped ? "/t/default" + path : path;

    private static async Task<ExplorerWorld> PrepareAsync()
    {
        var world = await UiHosts.WorldAsync();

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
        // next page must render in this same document.
        await page.EvaluateAsync("href => Blazor.navigateTo(href)", world.Head.Url("/t/default/data"));
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/data"));
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await page.EvaluateAsync("() => { window.__ltSameDocument = true; }");

        await Shell.Stop(page, "cluster").ClickAsync();
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/cluster"));
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        await Expect(Shell.Stop(page, "cluster")).ToHaveAttributeAsync("aria-current", "page");
        var sameDocument = await page.EvaluateAsync<bool>("() => window.__ltSameDocument === true");

        var server = world.Head.DescribeFaults();
        Assert.Multiple(() =>
        {
            Assert.That(sameDocument, Is.True, "the page rendered in the same circuit, not after a reload");
            Assert.That(faults, Is.Empty, "the browser reported an error");
            Assert.That(server ?? string.Empty, Does.Not.Contain("Unhandled exception"), server);
        });
    }
}
