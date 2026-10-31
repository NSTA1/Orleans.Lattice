using System.Globalization;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Layout;

/// <summary>
/// The chrome is right from the very first paint at a desktop and at a phone width (#5454).
/// The browser reports the Shell's width band to the circuit only after a render, so the
/// chrome drawn before that report - the prerender, and the live circuit's renders until the
/// report lands - must already be the one the width calls for: at 1440 pixels the compact
/// <b>Directory</b> and <b>Menu</b> controls never paint, and at 390 the spine column and the
/// wide header controls never paint.
/// </summary>
/// <remarks>
/// Each first paint is captured as a journey step, so a pull request can show it.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class FirstPaintBandTests : UiTestBase
{
    private const int DesktopWidth = 1440;
    private const int DesktopHeight = 900;
    private const int PhoneWidth = 390;
    private const int PhoneHeight = 844;

    /// <summary>
    /// Samples the chrome before every frame the document renders, from its first, and counts
    /// the frames in which a compact control (<b>Directory</b> or <b>Menu</b> in the banner) or
    /// a wide one (the spine column, or the banner's appearance menu) was on screen.
    /// </summary>
    private const string PaintRecorder =
        """
        (() => {
          const record = { frames: 0, compact: 0, wide: 0, firstCompact: null, firstWide: null };
          window.__ltPaints = record;
          const shown = (element) => {
            if (!element) { return false; }
            const box = element.getBoundingClientRect();
            return box.width > 0 && box.height > 0;
          };
          const sample = () => {
            const banner = document.querySelector('header');
            if (banner) {
              record.frames += 1;
              const compact = Array.from(banner.querySelectorAll('button')).find((button) => {
                const name = button.textContent.trim();
                return (name === 'Directory' || name === 'Menu') && shown(button);
              });
              const wide = [
                document.getElementById('lt-shell-directory'),
                banner.querySelector('button[data-lt-command="appearance.menu"]'),
              ].find(shown);
              if (compact) {
                record.compact += 1;
                record.firstCompact = record.firstCompact || ('frame ' + record.frames + ': ' + compact.textContent.trim());
              }
              if (wide) {
                record.wide += 1;
                record.firstWide = record.firstWide || ('frame ' + record.frames + ': ' + (wide.id || wide.getAttribute('data-lt-command')));
              }
            }
            requestAnimationFrame(sample);
          };
          requestAnimationFrame(sample);
        })();
        """;

    /// <summary>Removes the browser's ResizeObserver, which only the chrome uses, so the Shell never learns its width band.</summary>
    private const string NoResizeObserver = "(() => { window.ResizeObserver = undefined; })();";

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_prerendered_first_paint_draws_the_chrome_for_its_width(int width, int height)
    {
        // With the Blazor script refused no circuit ever starts, so what stays on screen is
        // exactly the server's prerender: the document's first paint.
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.RouteAsync("**/_framework/blazor.web.js", route => route.AbortAsync());

        await page.GotoAsync(world.Head.Url(ExplorerAreas.Get("data").PrimaryPath), new() { WaitUntil = WaitUntilState.Load });
        await CaptureJourneyStepAsync(page, $"first-paint-{Step(width, 1)}-{width}-prerender");

        await ExpectChromeForAsync(page, width);
    }

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_live_circuit_draws_the_chrome_for_its_width_before_the_band_is_reported(int width, int height)
    {
        // The circuit is live and the page rendered, but the band never arrives: a late (or
        // lost) report must not leave the wrong chrome on screen in the meantime.
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.AddInitScriptAsync(NoResizeObserver);

        await Shell.GotoAsync(page, world.Head, ExplorerAreas.Get("data").PrimaryPath);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await CaptureJourneyStepAsync(page, $"first-paint-{Step(width, 2)}-{width}-live-unreported");

        await ExpectChromeForAsync(page, width);
    }

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_other_widths_chrome_never_paints_across_navigations(int width, int height)
    {
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.AddInitScriptAsync(PaintRecorder);

        await Shell.GotoAsync(page, world.Head, "/");
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherChromeAsync(page, width, "A cold load of Home");

        // An in-app navigation through the directory, in its sheet on a phone.
        if (width == PhoneWidth)
        {
            await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true }).ClickAsync();
        }

        await Shell.Stop(page, "data").ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await ExpectNeverPaintedOtherChromeAsync(page, width, "Home, then the Data stop,");

        await Shell.GotoAsync(page, world.Head, ExplorerAreas.Get("data").DeepPath);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherChromeAsync(page, width, "A cold load of a tree");

        await page.ReloadAsync();
        await Shell.WaitForInteractiveAsync(page);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherChromeAsync(page, width, "A reload of a tree");
    }

    private static async Task ExpectChromeForAsync(IPage page, int width)
    {
        var banner = Shell.Banner(page);
        var directoryControl = banner.GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true });
        var menuControl = banner.GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });
        var spineColumn = page.Locator("#lt-shell-directory");
        var appearanceMenu = banner.Locator("button[data-lt-command=\"appearance.menu\"]");

        if (width == DesktopWidth)
        {
            await Expect(spineColumn).ToBeVisibleAsync();
            await Expect(Shell.Directory(page)).ToBeVisibleAsync();
            await Expect(appearanceMenu).ToBeVisibleAsync();
            await Expect(directoryControl).ToBeHiddenAsync();
            await Expect(menuControl).ToBeHiddenAsync();
        }
        else
        {
            await Expect(directoryControl).ToBeVisibleAsync();
            await Expect(menuControl).ToBeVisibleAsync();
            await Expect(spineColumn).ToBeHiddenAsync();
            await Expect(appearanceMenu).ToBeHiddenAsync();
            await Shell.AssertNoHorizontalPageScrollAsync(page, $"The first paint at {width}px");
        }
    }

    private static async Task ExpectNeverPaintedOtherChromeAsync(IPage page, int width, string where)
    {
        var record = await page.EvaluateAsync<PaintRecord>("() => window.__ltPaints");
        Assert.That(record.Frames, Is.GreaterThan(0), $"{where}: the recorder sampled no frame");

        if (width == DesktopWidth)
        {
            Assert.Multiple(() =>
            {
                Assert.That(record.Compact, Is.Zero,
                    $"{where} at {width}px painted the compact chrome in {record.Compact} of {record.Frames} frames, first at {record.FirstCompact}");
                Assert.That(record.Wide, Is.GreaterThan(0), $"{where}: the recorder never saw the wide chrome, so its silence proves nothing");
            });
        }
        else
        {
            Assert.Multiple(() =>
            {
                Assert.That(record.Wide, Is.Zero,
                    $"{where} at {width}px painted the wide chrome in {record.Wide} of {record.Frames} frames, first at {record.FirstWide}");
                Assert.That(record.Compact, Is.GreaterThan(0), $"{where}: the recorder never saw the compact chrome, so its silence proves nothing");
            });
        }
    }

    private static void Viewport(BrowserNewContextOptions options, int width, int height) =>
        options.ViewportSize = new ViewportSize { Width = width, Height = height };

    // The captures sort desktop first, then phone; the prerender before the live circuit.
    private static string Step(int width, int stage) =>
        ((width == DesktopWidth ? 0 : 2) + stage).ToString(CultureInfo.InvariantCulture);

    private sealed class PaintRecord
    {
        public int Frames { get; set; }

        public int Compact { get; set; }

        public int Wide { get; set; }

        public string? FirstCompact { get; set; }

        public string? FirstWide { get; set; }
    }
}
