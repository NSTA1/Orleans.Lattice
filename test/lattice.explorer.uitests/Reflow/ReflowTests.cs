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

    /// <summary>Every page the test world shows, at its primary and its deeper address.</summary>
    public static IEnumerable<TestCaseData> ShownPages() =>
        from area in ExplorerAreas.Shown
        from path in new[] { area.PrimaryPath, area.DeepPath }
        select new TestCaseData(path).SetArgDisplayNames(path);

    [TestCaseSource(nameof(ShownPages))]
    public async Task On_a_phone_the_chrome_keeps_one_line_and_a_stacked_toolbar_is_as_tall_as_its_controls(string path)
    {
        // #3987: at 390px the Data toolbar held a 250px blank under its filter (a
        // flex basis read as a height once the row stacked), the brand was cut to
        // "Orleans.Lattice Ex...", and the address chain wrapped with its prompt cut off.
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, path, WorldIdentities.Admin, Shell.SmallWidth);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await Shell.WaitForMotionToSettleAsync(page);

        var faults = await page.EvaluateAsync<string[]>(
            """
            () => {
              const faults = [];
              const clipped = (element) => element && element.scrollWidth > element.clientWidth + 1;
              if (clipped(document.querySelector('.lt-shell-brand__name'))) {
                faults.push('the brand is cut off: ' + document.querySelector('.lt-shell-brand__name').innerText);
              }
              const line = document.querySelector('.lt-shell-address-line');
              const edit = document.querySelector('.lt-shell-address-line__edit');
              if (line && edit) {
                const lineBox = line.getBoundingClientRect();
                const editBox = edit.getBoundingClientRect();
                if (lineBox.height > editBox.height + 4) { faults.push('the address line wraps: ' + Math.round(lineBox.height) + 'px tall'); }
                if (clipped(document.querySelector('.lt-shell-address-line__hint'))) { faults.push('the address prompt is cut off'); }
              }
              for (const toolbar of document.querySelectorAll('main .lt-toolbar')) {
                const items = [...toolbar.children].filter(item => {
                  const box = item.getBoundingClientRect();
                  return box.width > 0 && box.height > 0;
                });
                const label = (item) => (item.innerText || item.className).trim().slice(0, 30);
                for (let i = 0; i < items.length; i++) {
                  // An item taller than what it holds is blank space: the stacked
                  // row read a width basis as a height.
                  const box = items[i].getBoundingClientRect();
                  const parts = [...items[i].querySelectorAll('*')].map(part => part.getBoundingClientRect()).filter(part => part.height > 0);
                  if (parts.length > 0) {
                    const held = Math.max(...parts.map(part => part.bottom)) - Math.min(...parts.map(part => part.top));
                    if (box.height - held > 24) {
                      faults.push('"' + label(items[i]) + '" in a toolbar is ' + Math.round(box.height - held) + 'px taller than what it holds');
                    }
                  }
                  if (i > 0) {
                    const gap = box.top - items[i - 1].getBoundingClientRect().bottom;
                    if (gap > 24) {
                      faults.push('a ' + Math.round(gap) + 'px gap in a toolbar after "' + label(items[i - 1]) + '"');
                    }
                  }
                }
              }
              return faults;
            }
            """);

        Assert.That(faults, Is.Empty, $"{path} at {Shell.SmallWidth}px: " + string.Join("; ", faults));
    }

    [Test]
    public async Task On_a_phone_the_apps_catalogue_note_stands_clear_of_the_listing_below_it()
    {
        // #3987: "No configured source supports search." touched the "Sort by" label.
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/apps/catalogue", WorldIdentities.Admin, Shell.SmallWidth);
        var note = page.Locator(".lt-apps-search-note");
        await Expect(note).ToBeVisibleAsync();
        await Shell.WaitForMotionToSettleAsync(page);

        var gap = await note.EvaluateAsync<double>(
            """
            note => {
              let next = note.nextElementSibling;
              while (next && next.getBoundingClientRect().height === 0) { next = next.nextElementSibling; }
              return next ? next.getBoundingClientRect().top - note.getBoundingClientRect().bottom : 999;
            }
            """);

        Assert.That(gap, Is.GreaterThanOrEqualTo(12), "the note keeps a clear space above what follows it");
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