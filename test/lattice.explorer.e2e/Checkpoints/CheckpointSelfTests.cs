using System.Security.Cryptography;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// The checkpoint's self-test (#5360), in a real browser but with no estate: a deliberately
/// broken page fails its checkpoint on every kind of check - axe, the browser console and a
/// host fault - while its screenshot is still filed; a clean page passes, with its volatile
/// text masked; and the manifest's digests match the files beside it.
/// </summary>
[TestFixture]
[Journey(JourneyId)]
[Category("EndToEnd")]
public sealed class CheckpointSelfTests
{
    private const string JourneyId = "f2-checkpoint-self-test";

    private const string CleanPage =
        """
        <!doctype html>
        <html lang="en">
        <head><title>Checkpoint self-test: a clean page</title></head>
        <body style="font-family: sans-serif; margin: 24px">
          <header><p>Orleans.Lattice journeys</p></header>
          <main>
            <h1>A clean page</h1>
            <p>Captured at <code>2026-10-31 12:00:05 UTC</code> from <code>127.0.0.1:43211</code>, operation <code>3f2504e0-4f89-11d3-9a0c-0305e82c3301</code>.</p>
            <p>Tree <code>factory-orders</code> holds 240 orders.</p>
            <label for="search">Search</label>
            <input id="search" type="text" style="min-height: 44px">
            <button type="button" style="min-width: 44px; min-height: 44px">Go</button>
          </main>
        </body>
        </html>
        """;

    private const string BrokenPage =
        """
        <!doctype html>
        <html lang="en">
        <head><title>Checkpoint self-test: a broken page</title></head>
        <body style="font-family: sans-serif; margin: 24px">
          <main>
            <h1>A deliberately broken page</h1>
            <img src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7" width="48" height="48">
            <button type="button" style="min-width: 44px; min-height: 44px"></button>
            <script>console.error('deliberate console error');</script>
          </main>
        </body>
        </html>
        """;

    private string _logs = null!;
    private IBrowserContext _context = null!;
    private IPage _page = null!;
    private BrowserConsoleLog _console = null!;

    /// <summary>A fresh context and page at the run's viewport, recording what the page says.</summary>
    [SetUp]
    public async Task OpenPageAsync()
    {
        _logs = Path.Combine(Path.GetTempPath(), "lattice-e2e-selftest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_logs);
        var browser = await JourneyBrowser.ChromiumAsync();
        _context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = JourneyViewport.Run.Width, Height = JourneyViewport.Run.Height },
            DeviceScaleFactor = 1,
            ColorScheme = ColorScheme.Light,
            ReducedMotion = ReducedMotion.Reduce,
        });
        _page = await _context.NewPageAsync();
        _console = new BrowserConsoleLog();
        _console.Attach(_page);
    }

    /// <summary>Closes the context.</summary>
    [TearDown]
    public async Task ClosePageAsync()
    {
        await _context.DisposeAsync();
        Directory.Delete(_logs, recursive: true);
    }

    /// <summary>
    /// A page with an unlabelled image and button, a console error, and an Explorer head that
    /// logged an error fails its checkpoint naming each, and its screenshot is still filed.
    /// </summary>
    [Test]
    [Order(1)]
    [JourneyStep("broken-page", "A deliberately broken page")]
    public async Task A_deliberately_broken_page_fails_its_checkpoint_on_every_check()
    {
        var hostLog = Path.Combine(_logs, "west-explorer-0.log");
        File.WriteAllLines(hostLog, ["12:00:00.000 [out] fail: Microsoft.AspNetCore.Components.Server.Circuits.CircuitHost[111]", "12:00:00.001 [out]       deliberate host fault"]);
        var hostFaults = new HostFaultLog(() => [new HostLogSource("west-explorer-0", hostLog, FailsCheckpoint: true)]);
        await _page.SetContentAsync(BrokenPage);
        var recorder = JourneyRecorder.ForCurrentTest();

        var failure = Assert.ThrowsAsync<JourneyCheckpointException>(() =>
            JourneyCheckpoint.CaptureAsync(_page, recorder, "broken-page", "every-check-fails", "An unlabelled image and button, a console error and a host fault", persona: null, _console, hostFaults));

        var entry = recorder.Entries.Single(candidate => candidate.Step == "broken-page");
        Assert.Multiple(() =>
        {
            Assert.That(failure!.Message, Does.Contain("image-alt"));
            Assert.That(failure.Message, Does.Contain("button-name"));
            Assert.That(failure.Message, Does.Contain("deliberate console error"));
            Assert.That(failure.Message, Does.Contain("deliberate host fault"));
            Assert.That(entry.Passed, Is.False);
            Assert.That(File.Exists(Path.Combine(recorder.Directory, entry.File)), Is.True, "a failed checkpoint still files the state it failed on");
        });
    }

    /// <summary>A clean page passes every check, twice, and the manifest's digests match its files in order.</summary>
    [Test]
    [Order(2)]
    [JourneyStep("clean-page", "A clean page")]
    public async Task A_clean_page_passes_and_the_manifest_digests_match_its_files()
    {
        var recorder = JourneyRecorder.ForCurrentTest();
        await _page.SetContentAsync(CleanPage);
        var first = await JourneyCheckpoint.CaptureAsync(_page, recorder, "clean-page", "volatile-text-masked", "Times, endpoints and operation ids masked; names left", persona: null, _console, hostFaults: null);

        await _page.GetByLabel("Search").FillAsync("factory");
        var second = await JourneyCheckpoint.CaptureAsync(_page, recorder, "clean-page", "search-typed", "The search field holds a query", persona: null, _console, hostFaults: null);

        var manifest = JourneyManifest.Read(recorder.ManifestFile);
        var pngs = Directory.EnumerateFiles(recorder.Directory, "*.png").Select(Path.GetFileName).Order(StringComparer.Ordinal).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(first.Passed && second.Passed, Is.True);
            Assert.That(second.Index, Is.EqualTo(first.Index + 1));
            Assert.That(first.File, Does.Match(@"^\d{3}-clean-page-volatile-text-masked\.png$"));
            Assert.That(manifest.Journey, Is.EqualTo(JourneyId));
            Assert.That(manifest.Viewport, Is.EqualTo(JourneyViewport.Run.Name));
            Assert.That(manifest.Entries.Select(entry => entry.Index), Is.EqualTo(Enumerable.Range(1, manifest.Entries.Count)), "checkpoints are numbered in order from 1");
            Assert.That(manifest.Entries.Select(entry => entry.File), Is.EqualTo(pngs), "the manifest lists exactly the screenshots beside it");
            foreach (var entry in manifest.Entries)
            {
                var digest = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(recorder.Directory, entry.File)))).ToLowerInvariant();
                Assert.That(entry.Sha256, Is.EqualTo(digest), $"the digest of {entry.File}");
            }
        });
    }

    /// <summary>A checkpoint taken twice of the same state produces the same pixels: the screenshot is deterministic.</summary>
    [Test]
    [Order(3)]
    [JourneyStep("determinism", "The same state twice")]
    public async Task The_same_state_captured_twice_has_the_same_digest()
    {
        var recorder = JourneyRecorder.ForCurrentTest();
        await _page.SetContentAsync(CleanPage);

        var first = await JourneyCheckpoint.CaptureAsync(_page, recorder, "determinism", "first", "A clean page", persona: null, _console, hostFaults: null);
        await _page.SetContentAsync(CleanPage.Replace("12:00:05", "17:42:31", StringComparison.Ordinal).Replace("43211", "51877", StringComparison.Ordinal));
        var second = await JourneyCheckpoint.CaptureAsync(_page, recorder, "determinism", "second", "The same page in another run: other times and ports", persona: null, _console, hostFaults: null);

        Assert.That(second.Sha256, Is.EqualTo(first.Sha256), "volatile values are masked, so another run's times and ports draw the same pixels");
    }
}
