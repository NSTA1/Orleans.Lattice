using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Reflow;

/// <summary>
/// The responsive contract, measured in a real browser: every area at 360, 768 and 1280
/// pixels wide reflows with no horizontal page scroll (WCAG 2.2 SC 1.4.10) - a table wider
/// than the viewport scrolls inside its own frame - and every control is a touch target of
/// at least 44 pixels in comfortable density, and never below 24 in compact density.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ReflowTests : UiTestBase
{
    private static readonly int[] Widths = [Shell.SmallWidth, Shell.MediumWidth, Shell.LargeWidth];

    /// <summary>Every area at every width.</summary>
    public static IEnumerable<TestCaseData> AreasAtEveryWidth() =>
        from key in ExplorerAreas.Keys()
        from width in Widths
        select new TestCaseData(key, width).SetArgDisplayNames(key, width.ToString(System.Globalization.CultureInfo.InvariantCulture));

    [TestCaseSource(nameof(AreasAtEveryWidth))]
    public async Task Every_area_reflows_without_horizontal_page_scroll(string areaKey, int width)
    {
        var area = ExplorerAreas.Get(areaKey);
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, area.PrimaryPath, WorldIdentities.Admin, width);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(area.ShownToAdmin ? area.DisplayName : ExplorerAreas.NotFoundHeading);
        await Shell.WaitForMotionToSettleAsync(page);

        await Shell.AssertNoHorizontalPageScrollAsync(page, $"The {area.DisplayName} area at {width}px");
        await Shell.GotoAsync(page, world.Head, area.DeepPath);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await Shell.WaitForMotionToSettleAsync(page);
        await Shell.AssertNoHorizontalPageScrollAsync(page, $"{area.DeepPath} at {width}px");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Every_control_on_a_phone_is_a_touch_target_for_its_density(bool compact)
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin, Shell.SmallWidth);
        await Shell.SetAppearanceAsync(page, ShellAppearanceChoice.Default with { Compact = compact });
        var minimum = compact ? 24 : 44;

        var measured = 0;
        foreach (var area in ExplorerAreas.Shown)
        {
            await Shell.GotoAsync(page, world.Head, area.PrimaryPath);
            await Expect(Shell.Heading(page)).ToHaveTextAsync(area.DisplayName);
            await Shell.WaitForMotionToSettleAsync(page);

            var small = await page.EvaluateAsync<string[]>(
                """
                (minimum) => {
                  const controls = document.querySelectorAll(
                    'button, select, [role="tab"], a.lt-btn, input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"])');
                  const small = [];
                  for (const control of controls) {
                    const box = control.getBoundingClientRect();
                    if (box.width === 0 || box.height === 0 || control.closest('[hidden]')) { continue; }
                    if (box.height < minimum - 0.5 || box.width < minimum - 0.5) {
                      small.push(control.tagName.toLowerCase() + ' "' + (control.innerText || control.getAttribute('aria-label') || control.id || '').trim().slice(0, 40)
                        + '" ' + Math.round(box.width) + 'x' + Math.round(box.height));
                    }
                  }
                  return small;
                }
                """,
                minimum);
            measured += await page.Locator("button, select, [role=\"tab\"], a.lt-btn").CountAsync();

            Assert.That(small, Is.Empty,
                $"On a phone in {(compact ? "compact" : "comfortable")} density, these {area.DisplayName} controls are smaller than {minimum}px: "
                + string.Join("; ", small));
        }

        Assert.That(measured, Is.GreaterThan(ExplorerAreas.Shown.Count()), "The touch-target scan measured almost nothing.");
    }
}
