using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>The tenant switcher, the appearance controls, and the denied and hidden states.</summary>
public sealed partial class ShellPage
{
    /// <summary>The document root, which carries the appearance (<c>data-bs-theme</c>, <c>data-lt-contrast</c>, <c>data-lt-density</c>).</summary>
    public ILocator Root => Page.Locator("html");

    // ----- The tenant switcher -----

    /// <summary>The expanded header's tenant switcher: drawn only for a caller who may switch between two or more tenants.</summary>
    public ILocator TenantSwitcher => Banner.Locator("button[data-lt-command=\"tenant.switch\"]");

    /// <summary>
    /// Switches the active tenant to <paramref name="tenant"/> through the switcher's type-ahead
    /// field: from the header when expanded, from the directory sheet when compact.
    /// </summary>
    /// <param name="tenant">The tenant id.</param>
    public async Task SwitchTenantAsync(string tenant)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        var compact = await IsCompactAsync();
        ILocator field;
        if (compact)
        {
            await OpenDirectoryAsync();
            field = DirectorySheet.GetByRole(AriaRole.Combobox, new() { Name = "Switch tenant" });
        }
        else
        {
            await TenantSwitcher.ClickAsync();
            field = Banner.GetByRole(AriaRole.Group, new() { Name = "Switch tenant" }).GetByRole(AriaRole.Combobox, new() { Name = "Switch to tenant" });
        }

        await field.FillAsync(tenant);
        var option = Page.GetByRole(AriaRole.Option, new() { Name = tenant, Exact = true });
        await Expect(option).ToBeVisibleAsync();
        await option.ClickAsync();

        if (compact)
        {
            await CloseDirectoryAsync();
        }
        else
        {
            await Expect(TenantSwitcher).ToContainTextAsync(tenant);
        }

        await Expect(Announcement).ToHaveTextAsync($"Scoped to tenant {tenant}.");
    }

    // ----- Appearance -----

    /// <summary>
    /// Makes each appearance choice in <paramref name="commands"/> (see <see cref="AppearanceCommands"/>)
    /// through the appearance controls: the header's appearance menu, or the <b>Menu</b> sheet when compact.
    /// </summary>
    /// <param name="commands">The choices, such as <see cref="AppearanceCommands.Board"/>.</param>
    public async Task ChooseAppearanceAsync(params string[] commands)
    {
        ArgumentNullException.ThrowIfNull(commands);
        foreach (var command in commands)
        {
            var control = Page.Locator($"button[data-lt-command=\"{command}\"]");
            if (!await control.IsVisibleAsync())
            {
                await OpenAppearanceControlsAsync();
            }

            await control.ClickAsync();
            if (await control.IsVisibleAsync())
            {
                // Compact, the Menu sheet closes as soon as anything in it is activated.
                await Expect(control).ToHaveAttributeAsync("aria-pressed", "true");
            }

            await CloseAppearanceControlsAsync();
        }
    }

    /// <summary>Asserts the document is drawn in the Board (<see langword="true"/>) or Paper material.</summary>
    /// <param name="board">Whether Board is expected.</param>
    public Task ExpectMaterialAsync(bool board) => Expect(Root).ToHaveAttributeAsync("data-bs-theme", board ? "dark" : "light");

    private async Task OpenAppearanceControlsAsync()
    {
        if (await IsCompactAsync())
        {
            await OpenMenuAsync();
            return;
        }

        await Page.Locator("button[data-lt-command=\"appearance.menu\"]").ClickAsync();
        await Expect(Page.GetByRole(AriaRole.Group, new() { Name = "Appearance" })).ToBeVisibleAsync();
    }

    private async Task CloseAppearanceControlsAsync()
    {
        var toggle = Page.Locator("button[data-lt-command=\"appearance.menu\"]");
        if (await toggle.IsVisibleAsync() && await toggle.GetAttributeAsync("aria-expanded") == "true")
        {
            await toggle.ClickAsync();
            await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        }

        await CloseMenuAsync();
    }

    // ----- Denied and hidden states -----

    /// <summary>
    /// Asserts the page refuses an action the caller may not take, in the Explorer's words:
    /// "You may not <paramref name="what"/>" (for example <c>restore this backup's tree</c>).
    /// </summary>
    /// <param name="what">The rest of the sentence after "You may not ".</param>
    public Task ExpectDeniedAsync(string what)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(what);
        return Expect(Content).ToContainTextAsync("You may not " + what);
    }

    /// <summary>
    /// Asserts the page shows the caller nothing, in the Explorer's words: "Nothing you can
    /// <paramref name="what"/>" (for example <c>administer</c>).
    /// </summary>
    /// <param name="what">The rest of the sentence after "Nothing you can ".</param>
    public Task ExpectHiddenAsync(string what)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(what);
        return Expect(Content).ToContainTextAsync("Nothing you can " + what);
    }

    /// <summary>
    /// Asserts <paramref name="area"/> is withheld from the caller: its spine stop is not open,
    /// and its address shows the not-found page or why the area is not available.
    /// </summary>
    /// <param name="area">The area key.</param>
    public async Task ExpectAreaWithheldAsync(string area)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(area);
        var spine = await ReadSpineAsync();
        Assert.That(spine.CanOpen(area), Is.False, $"the spine offers '{area}' to this caller: {spine}");

        await GotoAsync("/" + area);
        await Expect(Heading).ToBeVisibleAsync();
        await Expect(Content).ToContainTextAsync(new Regex($"{Regex.Escape(NotFoundHeading)}|Not available"));
    }
}
