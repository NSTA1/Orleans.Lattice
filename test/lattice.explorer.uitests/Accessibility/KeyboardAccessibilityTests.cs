using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Keyboard-only journeys through the Explorer's chrome: the skip links, the directory,
/// the address line and the command palette - every stop reached, operated and left
/// with the keyboard alone, and focus always somewhere a sighted keyboard user can see.
/// </summary>
/// <remarks>
/// Focus entering and leaving an app frame is asserted with the pilot app, in
/// <c>AppFrameKeyboardTests</c>, because it needs an installed app.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class KeyboardAccessibilityTests : UiTestBase
{
    [Test]
    public async Task A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        await page.Keyboard.PressAsync("Tab");
        var first = await FocusedAsync(page);
        Assert.That(first, Is.EqualTo("a|Skip to directory"), "The first tab stop must be the first skip link.");

        await page.Keyboard.PressAsync("Tab");
        await page.Keyboard.PressAsync("Tab");
        Assert.That(await FocusedAsync(page), Is.EqualTo("a|Skip to content"));
        await Expect(page.GetByRole(AriaRole.Link, new() { Name = "Skip to content" })).ToBeVisibleAsync();

        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Content(page)).ToBeFocusedAsync();
    }

    [Test]
    public async Task The_directory_is_reached_walked_and_followed_with_the_keyboard_alone()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "access")).ToBeVisibleAsync();

        await page.Keyboard.PressAsync("Tab");
        await page.Keyboard.PressAsync("Enter");
        await Expect(page.Locator("#lt-shell-directory")).ToBeFocusedAsync();

        // Walk the spine in directory order until the Access stop has focus.
        var walked = new List<string>();
        for (var i = 0; i < 12; i++)
        {
            await page.Keyboard.PressAsync("Tab");
            var command = await page.EvaluateAsync<string>("() => document.activeElement?.getAttribute('data-lt-command') ?? ''");
            walked.Add(command);
            if (command == "go.access")
            {
                break;
            }
        }

        Assert.That(walked, Is.EqualTo(new[] { "go.home", "go.data", "go.apps", "go.access" }),
            "Tabbing from the directory must visit its stops in directory order.");

        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Access");
        await Expect(page).ToHaveURLAsync(world.Head.Url("/access"));
        await Expect(Shell.Stop(page, "access")).ToHaveAttributeAsync("aria-current", "page");
    }

    [Test]
    public async Task The_address_line_opens_goes_and_restores_with_the_keyboard_alone()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        // "/" opens the line; Escape restores it and gives focus back.
        await page.Locator("body").FocusAsync();
        await page.Keyboard.PressAsync("/");
        await Expect(Shell.AddressInput(page)).ToBeFocusedAsync();
        await page.Keyboard.TypeAsync("/clus");
        await page.Keyboard.PressAsync("Escape");
        await Expect(Shell.AddressInput(page)).ToHaveCountAsync(0);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        // Ctrl+K opens it again; an address and Enter go there.
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("/cluster");
        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        await Expect(page).ToHaveURLAsync(world.Head.Url("/cluster"));
    }

    [Test]
    public async Task The_command_palette_is_driven_with_the_keyboard_alone()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "schema")).ToBeVisibleAsync();

        await Shell.OpenAddressLineAsync(page);
        await page.Keyboard.TypeAsync(">Go to Schema");
        var option = Shell.Suggestions(page).Filter(new() { HasText = "Go to Schema" });
        await Expect(option).ToHaveCountAsync(1);

        // The highlighted suggestion is exposed through aria-activedescendant, not by
        // moving focus out of the input.
        await page.Keyboard.PressAsync("ArrowDown");
        var active = await Shell.AddressInput(page).GetAttributeAsync("aria-activedescendant");
        Assert.That(active, Is.Not.Null.And.Not.Empty, "Arrowing through suggestions must set aria-activedescendant.");
        await Expect(page.Locator($"[id=\"{active}\"]")).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Shell.AddressInput(page)).ToBeFocusedAsync();

        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Schema");
        await Expect(Shell.Stop(page, "schema")).ToHaveAttributeAsync("aria-current", "page");
    }

    [Test]
    public async Task The_directory_sheet_traps_focus_and_returns_it_when_it_closes()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin, width: Shell.SmallWidth);
        var toggle = Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true });
        await Expect(toggle).ToBeVisibleAsync();

        await toggle.FocusAsync();
        await page.Keyboard.PressAsync("Enter");
        var sheet = page.GetByRole(AriaRole.Dialog, new() { Name = "Directory" });
        await Expect(sheet).ToBeVisibleAsync();

        // Tabbing through more stops than the sheet holds never leaves it.
        for (var i = 0; i < 15; i++)
        {
            await page.Keyboard.PressAsync("Tab");
            Assert.That(await sheet.EvaluateAsync<bool>("sheet => sheet.contains(document.activeElement)"), Is.True,
                "Focus left the open directory sheet.");
        }

        await page.Keyboard.PressAsync("Escape");
        await Expect(sheet).ToBeHiddenAsync();
        await Expect(toggle).ToBeFocusedAsync();
    }

    [Test]
    public async Task Every_keyboard_focus_stop_paints_a_visible_focus_indicator()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        var unmarked = new List<string>();
        var visited = 0;
        for (var i = 0; i < 30; i++)
        {
            await page.Keyboard.PressAsync("Tab");
            var ring = await page.EvaluateAsync<string>(
                """
                () => {
                  const e = document.activeElement;
                  if (!e || e === document.body) { return 'body'; }
                  const s = getComputedStyle(e);
                  const outline = s.outlineStyle !== 'none' && parseFloat(s.outlineWidth) > 0;
                  const shadow = s.boxShadow && s.boxShadow !== 'none';
                  return outline || shadow ? 'ok' : e.outerHTML.slice(0, 100);
                }
                """);
            if (ring == "body")
            {
                continue;
            }

            visited++;
            if (ring != "ok")
            {
                unmarked.Add(ring);
            }
        }

        Assert.That(visited, Is.GreaterThan(10), "The keyboard walk reached too few controls to prove anything.");
        Assert.That(unmarked, Is.Empty, "These focus stops paint no focus indicator: " + string.Join("; ", unmarked));
    }

    /// <summary>The focused element as <c>tag|accessible text</c>.</summary>
    internal static Task<string> FocusedAsync(IPage page) =>
        page.EvaluateAsync<string>("() => { const e = document.activeElement; return e ? e.tagName.toLowerCase() + '|' + (e.innerText || e.getAttribute('aria-label') || '').trim() : ''; }");
}
