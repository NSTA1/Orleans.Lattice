using System.Text;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Smoke;

/// <summary>
/// The estate harness's smoke run (#5359): boot the Contoso west region, sign in as the
/// bootstrap administrator through the Explorer's own sign-in form and see the home page;
/// then kill one west silo and keep browsing, because the Explorer holds the region's
/// gateway, not a silo.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class EstateSmokeTests
{
    private JourneyEstate _estate = null!;
    private IBrowserContext? _context;

    /// <summary>Boots the estate once for the fixture.</summary>
    [OneTimeSetUp]
    public async Task StartEstateAsync() => _estate = await JourneyEstate.StartAsync();

    /// <summary>Stops every process and removes the estate's storage.</summary>
    [OneTimeTearDown]
    public async Task StopEstateAsync()
    {
        if (_estate is not null)
        {
            await _estate.DisposeAsync();
        }
    }

    /// <summary>Keeps the trace of a failed test beside the estate's process logs.</summary>
    [TearDown]
    public async Task CloseContextAsync()
    {
        if (_context is null)
        {
            return;
        }

        var failed = TestContext.CurrentContext.Result.Outcome.Status == NUnit.Framework.Interfaces.TestStatus.Failed;
        await _context.Tracing.StopAsync(new TracingStopOptions
        {
            Path = failed ? Path.Combine(_estate.ArtifactsDirectory, TestContext.CurrentContext.Test.Name + ".trace.zip") : null,
        });
        await _context.DisposeAsync();
        _context = null;
        if (failed)
        {
            foreach (var log in Directory.EnumerateFiles(_estate.ArtifactsDirectory, "*.log"))
            {
                TestContext.AddTestAttachment(log);
            }
        }
    }

    /// <summary>
    /// The administrator signs in through the form, sees the home page; a west silo dies, the
    /// gateway takes it out of rotation within its probe interval, and the Explorer goes on
    /// serving every page through the survivor.
    /// </summary>
    [Test]
    public async Task Administrator_signs_in_and_keeps_browsing_after_a_west_silo_dies()
    {
        const string tree = "smoke-lines";
        var data = LatticeDataApiGrpcClient.Create(_estate.Invoker(), JourneyEstate.Serialization);
        for (var i = 1; i <= 3; i++)
        {
            await data.SetAsync(new DataSetRequest { TreeId = tree, Key = $"line/{i:D2}", Value = Encoding.UTF8.GetBytes($"{{\"line\":{i}}}") });
        }

        _context = await JourneyBrowser.NewContextAsync(_estate.ExplorerUri);
        var page = await _context.NewPageAsync();
        var shots = Path.Combine(_estate.ArtifactsDirectory, "screenshots");

        await JourneyBrowser.GotoAsync(page, "/");
        await JourneyBrowser.SignInAsync(page, JourneyEstateOptions.Administrator, _estate.PasswordOf(JourneyEstateOptions.Administrator));
        await ExpectServingAsync(page, "Estate");
        await JourneyBrowser.ScreenshotAsync(page, Path.Combine(shots, "01-signed-in-home.png"));

        await JourneyBrowser.Stop(page, "data").ClickAsync();
        await ExpectServingAsync(page, "Data");
        await Expect(Content(page).GetByText(tree, new() { Exact = true }).First).ToBeVisibleAsync();
        await JourneyBrowser.ScreenshotAsync(page, Path.Combine(shots, "02-data-before-silo-loss.png"));

        await _estate.KillSiloAsync(JourneyEstate.West, 0);
        var removedAfter = await _estate.WaitForGatewayToMatchSilosAsync();
        var destinations = await _estate.GatewayDestinationsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(destinations.Single(d => d.Id == "silo-0").Healthy, Is.False, "the dead silo is out of rotation");
            Assert.That(destinations.Single(d => d.Id == "silo-1").Healthy, Is.True, "the survivor serves");
            Assert.That(removedAfter, Is.LessThanOrEqualTo(_estate.Options.GatewayHealthInterval * 3),
                "the gateway's active health check removes a dead silo within its probe interval");
        });

        await JourneyBrowser.Stop(page, "access").ClickAsync();
        await ExpectServingAsync(page, "Access");

        await JourneyBrowser.Stop(page, "data").ClickAsync();
        await ExpectServingAsync(page, "Data");
        await Expect(Content(page).GetByText(tree, new() { Exact = true }).First).ToBeVisibleAsync();
        await JourneyBrowser.ScreenshotAsync(page, Path.Combine(shots, "03-data-after-silo-loss.png"));

        await JourneyBrowser.GotoAsync(page, "/");
        await ExpectServingAsync(page, "Estate");
        await Expect(JourneyBrowser.SessionButton(page, JourneyEstateOptions.Administrator)).ToBeVisibleAsync();
        await JourneyBrowser.ScreenshotAsync(page, Path.Combine(shots, "04-home-after-silo-loss.png"));
    }

    private static ILocator Content(IPage page) => page.Locator("main#lt-shell-content");

    /// <summary>
    /// The page shows <paramref name="heading"/> and the header reports the head connected:
    /// the Explorer is serving through the gateway, not merely rendering a cached shell.
    /// </summary>
    private static async Task ExpectServingAsync(IPage page, string heading)
    {
        await Expect(JourneyBrowser.Heading(page)).ToHaveTextAsync(heading, new() { Timeout = 60_000 });
        await Expect(page.GetByRole(AriaRole.Banner).GetByText("Connected", new() { Exact = true }))
            .ToBeVisibleAsync(new() { Timeout = 60_000 });
    }
}
