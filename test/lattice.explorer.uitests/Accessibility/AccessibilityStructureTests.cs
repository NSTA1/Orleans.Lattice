using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// The accessibility criteria an axe sweep cannot see, asserted by name: landmarks, the
/// heading outline, enumerated ARIA state values, the live region, reduced motion and
/// forced colours.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AccessibilityStructureTests : UiTestBase
{
    private static readonly int[] Widths = [Shell.SmallWidth, Shell.MediumWidth, Shell.LargeWidth];

    [TestCaseSource(nameof(Widths))]
    public async Task The_shell_exposes_a_main_a_navigation_and_a_banner_landmark(int width)
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin, width);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        await Expect(page.GetByRole(AriaRole.Main)).ToHaveCountAsync(1);
        await Expect(page.GetByRole(AriaRole.Banner)).ToHaveCountAsync(1);
        Assert.That(await page.GetByRole(AriaRole.Navigation).CountAsync(), Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Each_area_page_has_one_h1_and_no_skipped_heading_levels()
    {
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin);
        foreach (var area in ExplorerAreas.All)
        {
            foreach (var path in new[] { area.PrimaryPath, area.DeepPath })
            {
                await Shell.GotoAsync(page, world.Head, path);
                await Expect(Shell.Heading(page)).ToBeVisibleAsync();
                await Shell.WaitForMotionToSettleAsync(page);

                var levels = await Shell.Content(page).EvaluateAsync<int[]>(
                    "main => [...main.querySelectorAll('h1,h2,h3,h4,h5,h6')].filter(h => h.getClientRects().length > 0).map(h => Number(h.tagName[1]))");
                Assert.That(levels.Count(level => level == 1), Is.EqualTo(1), $"{path} must have exactly one visible h1; it has [{string.Join(", ", levels)}].");
                for (var i = 1; i < levels.Length; i++)
                {
                    Assert.That(levels[i], Is.LessThanOrEqualTo(levels[i - 1] + 1),
                        $"{path} skips a heading level: [{string.Join(", ", levels)}].");
                }
            }
        }
    }

    /// <summary>
    /// Every toggle, tab and disclosure reports an ARIA state token the specification
    /// allows. A state bound to a C# <c>bool</c> renders a valueless attribute, which no
    /// control may report and which axe tolerates (see <c>AxeMutationProof.md</c>).
    /// </summary>
    [Test]
    public async Task Every_control_reports_a_valid_enumerated_aria_state()
    {
        var world = await UiHosts.WorldAsync();
        var page = await NewPageAsync(world.Head, WorldIdentities.Admin);
        var checkedControls = 0;
        foreach (var area in ExplorerAreas.Shown)
        {
            await Shell.GotoAsync(page, world.Head, area.PrimaryPath);
            await Expect(Shell.Heading(page)).ToHaveTextAsync(area.DisplayName);
            await page.Locator("button[data-lt-command=\"appearance.menu\"]").ClickAsync();
            await Expect(page.GetByRole(AriaRole.Group, new() { Name = "Appearance" })).ToBeVisibleAsync();

            var invalid = await page.EvaluateAsync<string[]>(
                """
                () => {
                  const allowed = {
                    'aria-pressed': ['true', 'false', 'mixed'],
                    'aria-selected': ['true', 'false'],
                    'aria-expanded': ['true', 'false'],
                    'aria-checked': ['true', 'false', 'mixed'],
                  };
                  const bad = [];
                  for (const [name, tokens] of Object.entries(allowed)) {
                    for (const element of document.querySelectorAll('[' + name + ']')) {
                      const value = element.getAttribute(name);
                      if (!tokens.includes(value)) {
                        bad.push(name + '="' + value + '" on ' + element.outerHTML.slice(0, 120));
                      }
                    }
                  }
                  return bad;
                }
                """);
            checkedControls += await page.Locator("[aria-pressed],[aria-selected],[aria-expanded],[aria-checked]").CountAsync();
            Assert.That(invalid, Is.Empty, $"The {area.DisplayName} area reports ARIA states outside their enumerated tokens.");
        }

        // The appearance menu alone holds eight toggles; a scan that saw nothing proved nothing.
        Assert.That(checkedControls, Is.GreaterThan(8 * ExplorerAreas.Shown.Count()));
    }

    [Test]
    public async Task The_notification_region_is_a_polite_live_region_before_anything_is_announced()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        // A live region rendered at the same moment as its message is silent, so the
        // region must already be in the tree, empty, before anything is announced.
        var region = page.GetByRole(AriaRole.Status, new() { Name = "Notifications" });
        await Expect(region).ToHaveCountAsync(1);
        await Expect(region).ToHaveAttributeAsync("aria-live", "polite");
        await Expect(Shell.Toasts(page)).ToHaveCountAsync(0);
    }

    [Test]
    public async Task A_reduced_motion_preference_neutralises_shell_motion()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin, configure: options => options.ReducedMotion = ReducedMotion.Reduce);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(prefers-reduced-motion: reduce)').matches"), Is.True,
            "The premise failed: the browser does not report a reduced-motion preference.");

        var longest = await page.EvaluateAsync<double>(
            """
            () => {
              const seconds = value => value.split(',').map(v => v.trim().endsWith('ms') ? parseFloat(v) / 1000 : parseFloat(v)).reduce((a, b) => Math.max(a, b), 0);
              let longest = 0;
              for (const element of document.querySelectorAll('*')) {
                const style = getComputedStyle(element);
                longest = Math.max(longest, seconds(style.transitionDuration), seconds(style.animationDuration));
              }
              return longest;
            }
            """);
        Assert.That(longest, Is.LessThanOrEqualTo(0.001), "Something still moves for longer than a moment under a reduced-motion preference.");
    }

    [Test]
    public async Task Forced_colours_keep_the_current_stop_and_the_focus_ring_visible()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin, configure: options => options.ForcedColors = ForcedColors.Active);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(forced-colors: active)').matches"), Is.True,
            "The premise failed: the browser does not report forced colours.");

        // The current stop's node is drawn differently from every other stop's, in the
        // platform's own colours, so "you are here" survives the palette being replaced.
        var current = await Shell.Stop(page, "data").Locator(".lt-node").EvaluateAsync<string>("n => getComputedStyle(n).backgroundColor");
        var other = await Shell.Stop(page, "cluster").Locator(".lt-node").EvaluateAsync<string>("n => getComputedStyle(n).backgroundColor");
        Assert.That(current, Is.Not.EqualTo(other), "Under forced colours the current directory stop looks like every other stop.");

        // A keyboard focus ring is an outline, which forced colours keep (a box shadow they drop).
        await page.Keyboard.PressAsync("Tab");
        var ring = await page.EvaluateAsync<string>("() => { const s = getComputedStyle(document.activeElement); return s.outlineStyle + ' ' + s.outlineWidth; }");
        Assert.That(ring, Does.Not.StartWith("none").And.Not.EndWith(" 0px"), $"The focused control paints no outline under forced colours ({ring}).");
    }
}
