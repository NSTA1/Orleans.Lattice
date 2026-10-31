using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>Chooses a value from one of the Explorer's type-ahead pickers.</summary>
internal static class JourneyPick
{
    /// <summary>Types <paramref name="value"/> into <paramref name="box"/> and chooses the suggestion that names it exactly.</summary>
    /// <remarks>
    /// A picker that suggests from a cached list may not suggest a value created moments ago;
    /// it still accepts any typed id, so the value is then committed by leaving the field.
    /// </remarks>
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
        try
        {
            await option.First.WaitForAsync(new() { Timeout = 15_000 });
            await option.First.ClickAsync();
        }
        catch (System.TimeoutException)
        {
            await box.PressAsync("Tab");
        }

        await Expect(box).ToHaveValueAsync(value);
    }
}
