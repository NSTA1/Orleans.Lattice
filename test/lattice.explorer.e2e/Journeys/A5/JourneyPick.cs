using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>Chooses a value from one of the Explorer's type-ahead pickers.</summary>
internal static class JourneyPick
{
    /// <summary>Types <paramref name="value"/> into <paramref name="box"/> and chooses the suggestion that names it exactly.</summary>
    /// <param name="page">The page the picker's suggestion list is drawn on.</param>
    /// <param name="box">The picker's combobox.</param>
    /// <param name="value">The value to choose.</param>
    public static async Task PickAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        var option = page.Locator(".lt-combobox__list [role=option]").Filter(new()
        {
            Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = new Regex($"^\\s*{Regex.Escape(value)}\\s*$") }),
        });
        await option.First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }
}
