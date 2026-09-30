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

    [Test]
    public async Task On_a_phone_a_tab_row_stays_on_one_line_scrolls_in_its_own_frame_and_keeps_the_active_tab_in_view()
    {
        // Issue #3986: the Data tree's six tabs wrapped onto two lines at phone width.
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?tab=views", WorldIdentities.Admin, Shell.SmallWidth);
        var list = page.GetByRole(AriaRole.Tablist).First;
        await Expect(list.GetByRole(AriaRole.Tab, new() { Name = "Views", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        await Shell.WaitForMotionToSettleAsync(page);

        var row = await RowAsync(list);
        Assert.Multiple(() =>
        {
            Assert.That(row.Lines, Is.EqualTo(1), "The tab row wraps onto more than one line.");
            Assert.That(row.Scrolls, Is.True, "The premise failed: six tabs fit a phone's width, so nothing here scrolls.");
            Assert.That(row.ActiveInView, Is.True, "The active tab is outside the row's visible frame.");
        });
        await Shell.AssertNoHorizontalPageScrollAsync(page, "A tab row at phone width");

        foreach (var path in new[] { $"/cluster/trees/{ExplorerWorld.DemoTree}", $"/schema/{ExplorerWorld.DemoTree}" })
        {
            await Shell.GotoAsync(page, world.Head, path);
            var other = page.GetByRole(AriaRole.Tablist).First;
            await Expect(other).ToBeVisibleAsync();
            await Shell.WaitForMotionToSettleAsync(page);
            Assert.That((await RowAsync(other)).Lines, Is.EqualTo(1), $"The tab row on {path} wraps at phone width.");
            await Shell.AssertNoHorizontalPageScrollAsync(page, $"{path} at phone width");
        }
    }

    private static async Task<(int Lines, bool Scrolls, bool ActiveInView)> RowAsync(ILocator list)
    {
        var measured = await list.EvaluateAsync<double[]>(
            """
            l => {
              const tabs = [...l.querySelectorAll('[role="tab"]')];
              const tops = new Set(tabs.map(t => Math.round(t.getBoundingClientRect().top)));
              const frame = l.getBoundingClientRect();
              const active = l.querySelector('[aria-selected="true"]').getBoundingClientRect();
              const inView = active.left >= frame.left - 0.5 && active.right <= frame.right + 0.5;
              return [tops.size, l.scrollWidth > l.clientWidth + 1 ? 1 : 0, inView ? 1 : 0];
            }
            """);
        return ((int)measured[0], measured[1] > 0, measured[2] > 0);
    }

    [Test]
    public async Task An_open_picker_reflows_on_a_phone_and_its_suggestions_are_touch_targets()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/cluster/orphans", WorldIdentities.Admin, Shell.SmallWidth);
        var tree = Accessibility.ComboBoxAccessibilityTests.Tree(page);
        await Expect(tree).ToBeVisibleAsync();

        await Accessibility.ComboBoxAccessibilityTests.OpenListAsync(page, tree);

        await Shell.AssertNoHorizontalPageScrollAsync(page, $"An open tree picker at {Shell.SmallWidth}px");
        var heights = await page.Locator(".lt-combobox__option").EvaluateAllAsync<double[]>("options => options.map(o => o.getBoundingClientRect().height)");
        Assert.That(heights, Is.Not.Empty.And.All.GreaterThanOrEqualTo(43.5), "Every suggestion on a phone is a 44px touch target.");
        var list = await page.Locator(".lt-combobox__list").BoundingBoxAsync();
        Assert.That(list!.X + list.Width, Is.LessThanOrEqualTo(Shell.SmallWidth + 0.5), "The listbox stays inside the viewport.");
    }
}