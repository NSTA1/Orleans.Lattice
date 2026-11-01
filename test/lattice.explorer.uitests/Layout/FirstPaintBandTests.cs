using System.Globalization;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Layout;

/// <summary>
/// The chrome is right from the very first paint at a desktop, a medium and a phone width.
/// The browser reports the Shell's width band to the circuit only after a render, so what is
/// drawn before that report - the prerender, and the live circuit's renders until the report
/// lands - must already be what the width calls for: the right chrome (#5454), and the band's
/// styling (#5465). At 1440 pixels the compact <b>Directory</b> and <b>Menu</b> controls never
/// paint and the spine is its full column; at a medium width the spine is already the narrow
/// rail; at 390 the spine column and the wide header controls never paint, the brand fits
/// whole, and the toolbars, the header and the content take the compact band's layout.
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
    private const int MediumWidth = 1000;
    private const int MediumHeight = 800;
    private const int PhoneWidth = 390;
    private const int PhoneHeight = 844;

    /// <summary>The spine's full column and its medium-band rail, in rem (lattice-chrome.css).</summary>
    private const double ColumnRem = 15;
    private const double RailRem = 11.5;

    /// <summary>
    /// Samples the chrome before every frame the document renders, from its first, and counts
    /// the frames in which a compact control (<b>Directory</b> or <b>Menu</b> in the banner) or
    /// a wide one (the spine column, or the banner's appearance menu) was on screen, the
    /// narrowest and widest the spine column was, and the frames in which the brand was cut off.
    /// </summary>
    private const string PaintRecorder =
        """
        (() => {
          const record = { frames: 0, compact: 0, wide: 0, firstCompact: null, firstWide: null, spineMin: null, spineMax: null, brandClipped: 0 };
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
              const spine = document.getElementById('lt-shell-directory');
              const wide = [spine, banner.querySelector('button[data-lt-command="appearance.menu"]')].find(shown);
              if (compact) {
                record.compact += 1;
                record.firstCompact = record.firstCompact || ('frame ' + record.frames + ': ' + compact.textContent.trim());
              }
              if (wide) {
                record.wide += 1;
                record.firstWide = record.firstWide || ('frame ' + record.frames + ': ' + (wide.id || wide.getAttribute('data-lt-command')));
              }
              if (shown(spine)) {
                const width = spine.getBoundingClientRect().width;
                record.spineMin = record.spineMin === null ? width : Math.min(record.spineMin, width);
                record.spineMax = record.spineMax === null ? width : Math.max(record.spineMax, width);
              }
              const brand = banner.querySelector('.lt-shell-brand__name');
              if (brand && brand.scrollWidth > brand.clientWidth + 1) { record.brandClipped += 1; }
            }
            requestAnimationFrame(sample);
          };
          requestAnimationFrame(sample);
        })();
        """;

    /// <summary>
    /// The band styling on screen now: the header's and the main landmark's inline padding, the
    /// spacing tokens they are drawn from, the brand, the first toolbar's direction, the spine
    /// column's width and whether its reasons and badges are drawn, and the root font size.
    /// </summary>
    private const string StyleProbe =
        """
        () => {
          const length = (css) => {
            const probe = document.createElement('div');
            probe.style.cssText = 'position:absolute;visibility:hidden;height:1px;width:' + css;
            document.body.appendChild(probe);
            const size = probe.getBoundingClientRect().width;
            probe.remove();
            return size;
          };
          const shown = (element) => {
            const box = element.getBoundingClientRect();
            return box.width > 1 && box.height > 1;
          };
          const header = document.querySelector('header.lt-shell-header');
          const brand = header.querySelector('.lt-shell-brand__name');
          const content = document.querySelector('main#lt-shell-content');
          const toolbar = content.querySelector('.lt-toolbar');
          const spine = document.getElementById('lt-shell-directory');
          return {
            rem: parseFloat(getComputedStyle(document.documentElement).fontSize),
            space3: length('var(--lt-space-3)'),
            space5: length('var(--lt-space-5)'),
            space6: length('var(--lt-space-6)'),
            headerPadding: parseFloat(getComputedStyle(header).paddingInlineStart),
            contentPadding: parseFloat(getComputedStyle(content).paddingInlineStart),
            brandText: brand.innerText.trim(),
            brandClipped: brand.scrollWidth > brand.clientWidth + 1,
            toolbarDirection: toolbar ? getComputedStyle(toolbar).flexDirection : '',
            spineWidth: spine && shown(spine) ? spine.getBoundingClientRect().width : 0,
            spineNotesShown: spine ? [...spine.querySelectorAll('.lt-shell-directory__reason, .lt-shell-directory__badge')].filter(shown).length : 0,
          };
        }
        """;

    /// <summary>Removes the browser's ResizeObserver, which only the chrome uses, so the Shell never learns its width band.</summary>
    private const string NoResizeObserver = "(() => { window.ResizeObserver = undefined; })();";

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(MediumWidth, MediumHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_prerendered_first_paint_draws_and_styles_the_chrome_for_its_width(int width, int height)
    {
        // With the Blazor script refused no circuit ever starts, so what stays on screen is
        // exactly the server's prerender: the document's first paint.
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.RouteAsync("**/_framework/blazor.web.js", route => route.AbortAsync());

        await page.GotoAsync(world.Head.Url(ExplorerAreas.Get("data").PrimaryPath), new() { WaitUntil = WaitUntilState.Load });
        await CaptureJourneyStepAsync(page, $"first-paint-{Step(width, 1)}-{width}-prerender");

        await ExpectChromeForAsync(page, width);
        await ExpectStylingForAsync(page, width, "The prerendered first paint");
    }

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(MediumWidth, MediumHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_live_circuit_draws_and_styles_the_chrome_for_its_width_before_the_band_is_reported(int width, int height)
    {
        // The circuit is live and the page rendered, but the band never arrives: a late (or
        // lost) report must not leave the wrong chrome, or the wrong band's styling, on screen.
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.AddInitScriptAsync(NoResizeObserver);

        await Shell.GotoAsync(page, world.Head, ExplorerAreas.Get("data").PrimaryPath);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await CaptureJourneyStepAsync(page, $"first-paint-{Step(width, 2)}-{width}-live-unreported");

        await ExpectChromeForAsync(page, width);
        await ExpectStylingForAsync(page, width, "The live circuit before the band report");
    }

    [TestCase(DesktopWidth, DesktopHeight)]
    [TestCase(MediumWidth, MediumHeight)]
    [TestCase(PhoneWidth, PhoneHeight)]
    public async Task The_other_widths_chrome_and_styling_never_paint_across_navigations(int width, int height)
    {
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin, width, options => Viewport(options, width, height));
        await page.AddInitScriptAsync(PaintRecorder);

        await Shell.GotoAsync(page, world.Head, "/");
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherBandAsync(page, width, "A cold load of Home");

        // An in-app navigation through the directory, in its sheet on a phone.
        if (width == PhoneWidth)
        {
            await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true }).ClickAsync();
        }

        await Shell.Stop(page, "data").ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await ExpectNeverPaintedOtherBandAsync(page, width, "Home, then the Data stop,");

        await Shell.GotoAsync(page, world.Head, ExplorerAreas.Get("data").DeepPath);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherBandAsync(page, width, "A cold load of a tree");

        await page.ReloadAsync();
        await Shell.WaitForInteractiveAsync(page);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await ExpectNeverPaintedOtherBandAsync(page, width, "A reload of a tree");
    }

    private static async Task ExpectChromeForAsync(IPage page, int width)
    {
        var banner = Shell.Banner(page);
        var directoryControl = banner.GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true });
        var menuControl = banner.GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });
        var spineColumn = page.Locator("#lt-shell-directory");
        var appearanceMenu = banner.Locator("button[data-lt-command=\"appearance.menu\"]");

        if (width != PhoneWidth)
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
        }

        await Shell.AssertNoHorizontalPageScrollAsync(page, $"The first paint at {width}px");
    }

    private static async Task ExpectStylingForAsync(IPage page, int width, string where)
    {
        await Expect(page.Locator("main#lt-shell-content .lt-toolbar").First).ToBeVisibleAsync();
        var style = await page.EvaluateAsync<StyleReport>(StyleProbe);
        var at = $"{where} at {width}px";

        Assert.Multiple(() =>
        {
            Assert.That(style.BrandClipped, Is.False, $"{at}: the brand is cut off ('{style.BrandText}')");
            if (width == PhoneWidth)
            {
                Assert.That(style.BrandText, Is.EqualTo("Lattice Explorer"), $"{at}: the compact brand drops its namespace");
                Assert.That(style.HeaderPadding, Is.EqualTo(style.Space3).Within(0.5), $"{at}: the header keeps the compact band's padding");
                Assert.That(style.ContentPadding, Is.EqualTo(style.Space3).Within(0.5), $"{at}: the main landmark keeps the compact band's padding");
                Assert.That(style.ToolbarDirection, Is.EqualTo("column"), $"{at}: a toolbar stacks in the compact band");
            }
            else
            {
                Assert.That(style.BrandText, Is.EqualTo("Orleans.Lattice Explorer"), $"{at}: the brand keeps its namespace");
                Assert.That(style.HeaderPadding, Is.EqualTo(style.Space5).Within(0.5), $"{at}: the header keeps the wide padding");
                Assert.That(style.ContentPadding, Is.EqualTo(style.Space6).Within(0.5), $"{at}: the main landmark keeps the wide padding");
                Assert.That(style.ToolbarDirection, Is.EqualTo("row"), $"{at}: a toolbar is one row from the medium band up");

                var expected = (width == MediumWidth ? RailRem : ColumnRem) * style.Rem;
                Assert.That(style.SpineWidth, Is.EqualTo(expected).Within(1),
                    $"{at}: the spine is {(width == MediumWidth ? "the medium band's rail" : "its full column")}");
                if (width == MediumWidth)
                {
                    Assert.That(style.SpineNotesShown, Is.Zero, $"{at}: the rail draws no reasons or badges");
                }
            }
        });
    }

    private static async Task ExpectNeverPaintedOtherBandAsync(IPage page, int width, string where)
    {
        var record = await page.EvaluateAsync<PaintRecord>("() => window.__ltPaints");
        var rem = await page.EvaluateAsync<double>("() => parseFloat(getComputedStyle(document.documentElement).fontSize)");
        Assert.That(record.Frames, Is.GreaterThan(0), $"{where}: the recorder sampled no frame");

        Assert.Multiple(() =>
        {
            Assert.That(record.BrandClipped, Is.Zero, $"{where} at {width}px cut the brand off in {record.BrandClipped} of {record.Frames} frames");
            if (width == PhoneWidth)
            {
                Assert.That(record.Wide, Is.Zero,
                    $"{where} at {width}px painted the wide chrome in {record.Wide} of {record.Frames} frames, first at {record.FirstWide}");
                Assert.That(record.Compact, Is.GreaterThan(0), $"{where}: the recorder never saw the compact chrome, so its silence proves nothing");
            }
            else
            {
                Assert.That(record.Compact, Is.Zero,
                    $"{where} at {width}px painted the compact chrome in {record.Compact} of {record.Frames} frames, first at {record.FirstCompact}");
                Assert.That(record.Wide, Is.GreaterThan(0), $"{where}: the recorder never saw the wide chrome, so its silence proves nothing");

                var expected = (width == MediumWidth ? RailRem : ColumnRem) * rem;
                Assert.That(record.SpineMin, Is.EqualTo(expected).Within(1), $"{where} at {width}px: the narrowest spine painted");
                Assert.That(record.SpineMax, Is.EqualTo(expected).Within(1), $"{where} at {width}px: the widest spine painted");
            }
        });
    }

    private static void Viewport(BrowserNewContextOptions options, int width, int height) =>
        options.ViewportSize = new ViewportSize { Width = width, Height = height };

    // The captures sort desktop, then medium, then phone; the prerender before the live circuit.
    private static string Step(int width, int stage) =>
        ((width switch { DesktopWidth => 0, MediumWidth => 2, _ => 4 }) + stage).ToString(CultureInfo.InvariantCulture);

    private sealed class PaintRecord
    {
        public int Frames { get; set; }

        public int Compact { get; set; }

        public int Wide { get; set; }

        public string? FirstCompact { get; set; }

        public string? FirstWide { get; set; }

        public double? SpineMin { get; set; }

        public double? SpineMax { get; set; }

        public int BrandClipped { get; set; }
    }

    private sealed class StyleReport
    {
        public double Rem { get; set; }

        public double Space3 { get; set; }

        public double Space5 { get; set; }

        public double Space6 { get; set; }

        public double HeaderPadding { get; set; }

        public double ContentPadding { get; set; }

        public string BrandText { get; set; } = string.Empty;

        public bool BrandClipped { get; set; }

        public string ToolbarDirection { get; set; } = string.Empty;

        public double SpineWidth { get; set; }

        public int SpineNotesShown { get; set; }
    }
}
