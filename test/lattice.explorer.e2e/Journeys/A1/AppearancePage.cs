using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>
/// Reads the appearance a persona has chosen from the appearance controls themselves (the
/// header's appearance menu, or the <b>Menu</b> sheet when compact): the pressed choice of each
/// group, as the ledger's <c>theme/contrast/density</c> form (#5364).
/// </summary>
/// <param name="shell">The shell whose appearance is read.</param>
public sealed class AppearancePage(ShellPage shell)
{
    private const string MenuToggle = "button[data-lt-command=\"appearance.menu\"]";

    /// <summary>The shell.</summary>
    public ShellPage Shell { get; } = shell ?? throw new ArgumentNullException(nameof(shell));

    /// <summary>
    /// Opens the appearance controls, reads the pressed theme, contrast and density, closes them
    /// again, and returns them as <c>theme/contrast/density</c> in lower case (for example
    /// <c>board/system/compact</c>).
    /// </summary>
    public async Task<string> ReadAsync()
    {
        var compact = await Shell.IsCompactAsync();
        ILocator scope;
        if (compact)
        {
            await Shell.OpenMenuAsync();
            scope = Shell.MenuSheet;
        }
        else
        {
            var toggle = Shell.Page.Locator(MenuToggle);
            await toggle.ClickAsync();
            scope = Shell.Page.GetByRole(AriaRole.Group, new() { Name = "Appearance" });
            await Expect(scope).ToBeVisibleAsync();
        }

        var theme = await PressedAsync(scope, "appearance.theme.");
        var contrast = await PressedAsync(scope, "appearance.contrast.");
        var density = await PressedAsync(scope, "appearance.density.");

        if (compact)
        {
            await Shell.CloseMenuAsync();
        }
        else
        {
            var toggle = Shell.Page.Locator(MenuToggle);
            await toggle.ClickAsync();
            await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        }

        return $"{theme}/{contrast}/{density}";
    }

    /// <summary>Asserts the document is drawn compact (<see langword="true"/>) or comfortable.</summary>
    /// <param name="compact">Whether compact density is expected.</param>
    public Task ExpectCompactAsync(bool compact) => compact
        ? Expect(Shell.Root).ToHaveAttributeAsync("data-lt-density", "compact")
        : Expect(Shell.Root).Not.ToHaveAttributeAsync("data-lt-density", "compact");

    private static async Task<string> PressedAsync(ILocator scope, string prefix)
    {
        var pressed = scope.Locator($"button[data-lt-command^=\"{prefix}\"][aria-pressed=\"true\"]");
        await Expect(pressed).ToHaveCountAsync(1);
        var command = await pressed.GetAttributeAsync("data-lt-command") ?? string.Empty;
        return command[prefix.Length..];
    }
}
