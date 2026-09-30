using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Journeys;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Design;

/// <summary>
/// Issue #3986: the design system draws each kind of control one way wherever it is used,
/// measured in a real browser across every area: anything drawn as a button - an anchor
/// or a native button - carries no link underline and is the same control height; an id
/// in a table is never broken mid-token, and one cut short keeps its full text; and the
/// header's panels close each other.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class DesignConsistencyTests : UiTestBase
{
    [Test]
    public async Task Every_anchor_drawn_as_a_button_is_drawn_as_a_native_button_is()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        var anchors = 0;
        var mismatches = new List<string>();

        foreach (var path in AreaPages())
        {
            await VisitAsync(page, world, path);
            var measured = await page.EvaluateAsync<string[]>(
                """
                () => {
                  const height = b => b.getBoundingClientRect().height;
                  const buttons = [...document.querySelectorAll('main button.lt-btn')].filter(b => b.offsetParent);
                  const reference = buttons.length ? height(buttons[0]) : '';
                  return [...document.querySelectorAll('main a.lt-btn')].filter(a => a.offsetParent).map(a =>
                    (a.innerText || '').trim().slice(0, 30) + '|' + getComputedStyle(a).textDecorationLine + '|' + height(a) + '|' + reference);
                }
                """);
            foreach (var entry in measured)
            {
                anchors++;
                var parts = entry.Split('|');
                var tall = Number(parts[2]);
                var reference = parts[3].Length == 0 ? tall : Number(parts[3]);
                if (parts[1] != "none" || Math.Abs(tall - reference) > 0.5)
                {
                    mismatches.Add($"{path}: \"{parts[0]}\" underline={parts[1]} height={tall} (a native button is {reference})");
                }
            }
        }

        Assert.That(anchors, Is.GreaterThan(0), "The scan measured no anchor drawn as a button.");
        Assert.That(mismatches, Is.Empty,
            "These anchors are not drawn as the design system's one button:" + Environment.NewLine + string.Join(Environment.NewLine, mismatches));
    }

    [Test]
    public async Task No_id_in_a_table_is_broken_mid_token_and_one_cut_short_keeps_its_full_text()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        var cells = 0;
        var faults = new List<string>();

        foreach (var path in AreaPages())
        {
            await VisitAsync(page, world, path);
            var measured = await page.EvaluateAsync<string[]>(
                """
                () => [...document.querySelectorAll('.lt-table__cell--mono')].filter(c => c.offsetParent).map(c =>
                  (c.innerText || '').trim().slice(0, 40) + '|' + getComputedStyle(c).whiteSpace + '|'
                    + (c.scrollWidth > c.clientWidth + 1) + '|' + c.hasAttribute('title'))
                """);
            foreach (var entry in measured)
            {
                cells++;
                var parts = entry.Split('|');
                if (parts[1] != "nowrap")
                {
                    faults.Add($"{path}: \"{parts[0]}\" can wrap (white-space {parts[1]})");
                }
                else if (parts[2] == "true" && parts[3] != "true")
                {
                    faults.Add($"{path}: \"{parts[0]}\" is cut short with no tooltip holding its full text");
                }
            }
        }

        Assert.That(cells, Is.GreaterThan(0), "The scan measured no mono table cell.");
        Assert.That(faults, Is.Empty, string.Join(Environment.NewLine, faults));
    }

    [Test]
    public async Task Opening_a_header_panel_closes_the_one_already_open()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        var tenant = TenantSwitcherJourneyTests.Toggle(page);
        var appearance = Shell.Banner(page).Locator("button[data-lt-command=\"appearance.menu\"]");
        await Expect(tenant).ToBeVisibleAsync();

        await appearance.ClickAsync();
        await Expect(appearance).ToHaveAttributeAsync("aria-expanded", "true");
        await tenant.ClickAsync();

        await Expect(tenant).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(appearance).ToHaveAttributeAsync("aria-expanded", "false");
        await Expect(page.Locator(".lt-shell-menu[aria-label=\"Appearance\"]")).ToHaveCountAsync(0);

        await appearance.ClickAsync();

        await Expect(appearance).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(tenant).ToHaveAttributeAsync("aria-expanded", "false");
        await Expect(page.Locator(".lt-shell-tenant__panel")).ToHaveCountAsync(0);
    }

    private static IEnumerable<string> AreaPages() =>
        ExplorerAreas.Shown.SelectMany(area => new[] { area.PrimaryPath, area.DeepPath });

    private static async Task VisitAsync(IPage page, ExplorerWorld world, string path)
    {
        await Shell.GotoAsync(page, world.Head, path);
        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await Shell.WaitForMotionToSettleAsync(page);
    }

    private static double Number(string text) => double.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
}
