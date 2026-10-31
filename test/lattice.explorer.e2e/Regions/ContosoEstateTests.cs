using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Regions;

/// <summary>
/// The Contoso estate's second region, telemetry and hosting variants (#5362): an entry written
/// in west reaches east once its tree is enrolled; Telemetry draws boards from Prometheus; a
/// session on the east console, under <c>/console</c>, survives the gateway switching it to the
/// other replica; the kiosk head opens signed in; and a region and Prometheus can be taken down
/// and brought back.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class ContosoEstateTests : EstateFixture
{
    private const string Tree = "regions-orders";

    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromMinutes(3);

    /// <inheritdoc />
    protected override JourneyEstateOptions Options => EstateProfile.Contoso;

    private LatticeDataApiGrpcClient Data(string region) =>
        LatticeDataApiGrpcClient.Create(Estate.Invoker(region: region), JourneyEstate.Serialization);

    private LatticeReplicationApiGrpcClient Replication(string region) =>
        LatticeReplicationApiGrpcClient.Create(Estate.Invoker(region: region), JourneyEstate.Serialization);

    /// <summary>
    /// West and east are peered: once a tree is enrolled in both, an entry written in west after
    /// enrolment ships to east, and one written before it arrives by east's bootstrap from west.
    /// </summary>
    [Test]
    [Order(1)]
    public async Task An_entry_written_in_west_reaches_east_once_its_tree_is_enrolled()
    {
        await Data(JourneyEstate.West).SetAsync(new DataSetRequest { TreeId = Tree, Key = "order/before", Value = [1] });

        var west = await Replication(JourneyEstate.West).EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        var east = await Replication(JourneyEstate.East).EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister, JourneyEstate.West);
        Assert.That((west.TreeId, east.TreeId), Is.EqualTo((Tree, Tree)));

        await Data(JourneyEstate.West).SetAsync(new DataSetRequest { TreeId = Tree, Key = "order/after", Value = [2] });

        foreach (var key in new[] { "order/after", "order/before" })
        {
            var arrived = await EventuallyAsync(async () =>
                (await Data(JourneyEstate.East).GetAsync(new DataGetRequest { TreeId = Tree, Key = key })).Found);
            Assert.That(arrived, Is.True, $"'{key}' written in west did not reach east within {ConvergenceTimeout.TotalMinutes:0} minutes");
        }
    }

    /// <summary>East enrols <c>factory-lines</c> through its configuration alone: a static-only enrolment.</summary>
    [Test]
    [Order(2)]
    public async Task East_enrols_factory_lines_statically()
    {
        var east = await Replication(JourneyEstate.East).GetReplicationConfigAsync();
        var lines = east.Trees.SingleOrDefault(tree => tree.TreeId == "factory-lines");

        Assert.That(lines, Is.Not.Null, "east reports factory-lines as enrolled");
        Assert.Multiple(() =>
        {
            Assert.That(lines!.Source, Is.EqualTo(ReplicationEnrollmentSource.Static));
            Assert.That(lines.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    /// <summary>
    /// Prometheus scrapes every silo and the telemetry facade queries it: once each silo is
    /// scraped, Telemetry lists its boards, and a board draws a chart with a series on it.
    /// </summary>
    [Test]
    [Order(3)]
    public async Task Telemetry_lists_boards_with_non_empty_charts_after_a_scrape()
    {
        for (var i = 0; i < 20; i++)
        {
            await Data(JourneyEstate.West).SetAsync(new DataSetRequest { TreeId = Tree, Key = $"order/telemetry-{i:00}", Value = [3] });
            await Data(JourneyEstate.West).GetAsync(new DataGetRequest { TreeId = Tree, Key = $"order/telemetry-{i:00}" });
        }

        await Estate.WaitForPrometheusScrapesAsync();

        var page = await OpenAsync(Estate.ExplorerUri);
        await JourneyBrowser.GotoAsync(page, "/");
        await JourneyBrowser.SignInAsync(page, JourneyEstateOptions.Administrator, Estate.PasswordOf(JourneyEstateOptions.Administrator));
        await JourneyBrowser.Stop(page, "telemetry").ClickAsync();
        await Expect(page).ToHaveURLAsync(new Regex("/telemetry"));
        var boards = page.Locator("main#lt-shell-content a.lt-telemetry-link");
        await Expect(boards.First).ToBeVisibleAsync();
        var hrefs = await boards.EvaluateAllAsync<string[]>("links => links.map(link => link.getAttribute('href'))");
        await ScreenshotAsync(page, "contoso-02-telemetry-boards.png");

        string? drawn = null;
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
        while (drawn is null && DateTime.UtcNow < deadline)
        {
            foreach (var href in hrefs.Distinct(StringComparer.Ordinal))
            {
                await JourneyBrowser.GotoAsync(page, href);
                await Expect(page.Locator(".lt-telemetry-chart").First).ToBeVisibleAsync();
                await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
                try
                {
                    await Expect(page.Locator("path.lt-telemetry-svg__line").First).ToBeVisibleAsync(new() { Timeout = 10_000 });
                    drawn = href;
                    break;
                }
                catch (PlaywrightException)
                {
                }
            }
        }

        Assert.That(drawn, Is.Not.Null, "no Telemetry board drew a series within two minutes of every silo being scraped");
        await ScreenshotAsync(page, "contoso-03-telemetry-board.png");
    }

    /// <summary>
    /// The east console is two replicas behind the east gateway under <c>/console</c>, sharing a
    /// key ring in Blob storage: olivia signs in, the gateway is forced off the replica that
    /// served her, and the other replica serves her signed in, under the same base path.
    /// </summary>
    [Test]
    [Order(4)]
    public async Task An_east_console_session_survives_being_served_by_the_other_replica()
    {
        var console = Estate.EastRegion.ConsoleUri;
        Assert.That(console.AbsolutePath, Is.EqualTo("/console/"), "the east console is mounted under /console");

        var page = await OpenAsync(console);
        await JourneyBrowser.GotoAsync(page, "/");
        await JourneyBrowser.SignInAsync(page, JourneyEstateOptions.Administrator, Estate.PasswordOf(JourneyEstateOptions.Administrator));
        var first = await ServingReplicaAsync(page);
        Assert.That(page.Url, Does.StartWith(console.ToString()), "the console stays under its base path after sign-in");
        await ScreenshotAsync(page, "contoso-01-east-console.png");

        try
        {
            await Estate.ForceConsoleSwitchAsync(first);
            var second = await ServingReplicaAsync(page);

            Assert.That(second, Is.Not.EqualTo(first), "the gateway switched the browser to the other replica");
            await Expect(JourneyBrowser.SessionButton(page, JourneyEstateOptions.Administrator)).ToBeVisibleAsync();

            await JourneyBrowser.Stop(page, "data").ClickAsync();
            // Data is rooted at the active tenant (/t/default/data) even with tenancy off.
            await Expect(page).ToHaveURLAsync(new Regex("^" + Regex.Escape(console.ToString()) + "(t/[^/]+/)?data"));
            await Expect(JourneyBrowser.Heading(page)).ToHaveTextAsync("Data");
            await ScreenshotAsync(page, "contoso-04-east-console-other-replica.png");
        }
        finally
        {
            await Estate.ForceConsoleSwitchAsync(from: -1);
        }
    }

    /// <summary>The kiosk head, with the environment credential seed on, opens signed in as its seeded identity.</summary>
    [Test]
    [Order(5)]
    public async Task The_kiosk_head_opens_signed_in_as_its_seeded_identity()
    {
        var page = await OpenAsync(Estate.KioskUri);
        await JourneyBrowser.GotoAsync(page, "/");

        await Expect(JourneyBrowser.SessionButton(page, JourneyEstateOptions.Administrator)).ToBeVisibleAsync();
        await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" })).ToHaveCountAsync(0);
        await ScreenshotAsync(page, "contoso-05-kiosk-signed-in.png");
    }

    /// <summary>Prometheus stops and starts again on demand, as a telemetry outage does.</summary>
    [Test]
    [Order(6)]
    public async Task Prometheus_stops_and_starts_again()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        await Estate.StopPrometheusAsync();
        Assert.That(Estate.PrometheusIsRunning, Is.False);
        Assert.ThrowsAsync(Is.InstanceOf<HttpRequestException>().Or.InstanceOf<TaskCanceledException>(),
            () => http.GetAsync(new Uri(Estate.PrometheusUri, "-/ready")));

        await Estate.StartPrometheusAsync();
        Assert.That(Estate.PrometheusIsRunning, Is.True);
        using var ready = await http.GetAsync(new Uri(Estate.PrometheusUri, "-/ready"));
        Assert.That(ready.IsSuccessStatusCode, Is.True);
    }

    /// <summary>
    /// East is isolated (its silos stop) and healed (they start again): its gateway follows,
    /// and what east held before the outage is still there after it.
    /// </summary>
    [Test]
    [Order(7)]
    public async Task East_is_isolated_and_healed()
    {
        await Estate.IsolateRegionAsync(JourneyEstate.East);
        Assert.That(Estate.EastRegion.Silos.Any(silo => silo.IsRunning), Is.False);
        await Estate.WaitForGatewayToMatchSilosAsync(JourneyEstate.East);
        Assert.That((await Estate.GatewayDestinationsAsync(JourneyEstate.East)).Any(silo => silo.Healthy), Is.False,
            "the east gateway routes to no silo while east is isolated");

        await Estate.HealRegionAsync(JourneyEstate.East);
        Assert.That(Estate.EastRegion.Silos.All(silo => silo.IsRunning), Is.True);
        var read = await Data(JourneyEstate.East).GetAsync(new DataGetRequest { TreeId = Tree, Key = "order/after" });
        Assert.That(read.Found, Is.True, "east keeps what it held before the outage");
    }

    private static async Task<int> ServingReplicaAsync(IPage page)
    {
        await page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        var response = await page.ReloadAsync() ?? throw new InvalidOperationException("The reload returned no response.");
        await JourneyBrowser.WaitForInteractiveAsync(page);
        var header = await response.HeaderValueAsync(JourneyEstate.ReplicaHeader);
        return JourneyEstate.ReplicaIndexOf(header)
            ?? throw new InvalidOperationException($"The console's response named no replica ({JourneyEstate.ReplicaHeader}: '{header}').");
    }

    private static async Task<bool> EventuallyAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + ConvergenceTimeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        return await condition();
    }
}
