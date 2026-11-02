using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>The Explorer's type-ahead pickers (<c>LtComboBox</c>), as Access draws them.</summary>
internal static class Pickers
{
    /// <summary>The options the open list of <paramref name="page"/> offers.</summary>
    /// <param name="page">The page.</param>
    public static ILocator Options(IPage page) => page.Locator(".lt-combobox__list [role=option]");

    /// <summary>The option whose value is exactly <paramref name="value"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="value">The value.</param>
    public static ILocator Option(IPage page, string value) => Options(page).Filter(new()
    {
        Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = Exactly(value) }),
    });

    /// <summary>Types <paramref name="value"/> into <paramref name="box"/> and chooses the suggestion that names it exactly.</summary>
    /// <param name="page">The page.</param>
    /// <param name="box">The picker's input.</param>
    /// <param name="value">The value to choose.</param>
    public static async Task PickAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        await Option(page, value).First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }

    /// <summary>The flag shown under the field that holds <paramref name="box"/>.</summary>
    /// <param name="box">The picker's input.</param>
    public static ILocator FlagOf(ILocator box) => FieldOf(box).Locator(".lt-combobox__flag");

    /// <summary>The error shown under the field that holds <paramref name="box"/>.</summary>
    /// <param name="box">The picker's input.</param>
    public static ILocator ErrorOf(ILocator box) => FieldOf(box).Locator(".lt-field__error");

    /// <summary>A pattern matching exactly <paramref name="text"/>, give or take surrounding white space.</summary>
    /// <param name="text">The text.</param>
    public static Regex Exactly(string text) => new($"^\\s*{Regex.Escape(text)}\\s*$");

    private static ILocator FieldOf(ILocator box) => box.Locator("xpath=ancestor::div[contains(concat(' ', normalize-space(@class), ' '), ' lt-field ')][1]");
}
