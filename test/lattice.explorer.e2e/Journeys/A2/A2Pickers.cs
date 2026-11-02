using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// How the A2 page objects drive the Explorer's type-ahead pickers and responsive tables:
/// choosing a listed suggestion, naming a value the picker need not list, and opening a
/// table row at either width.
/// </summary>
internal static class A2Pickers
{
    /// <summary>The open suggestion list of whichever picker has focus.</summary>
    /// <param name="page">The page.</param>
    public static ILocator Options(IPage page) => page.Locator(".lt-combobox__list [role=option]");

    /// <summary>Types <paramref name="value"/> into a picker and chooses the suggestion that names it exactly.</summary>
    /// <param name="page">The page.</param>
    /// <param name="box">The picker's combobox.</param>
    /// <param name="value">The exact value to choose.</param>
    public static async Task ChooseAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        var option = Options(page).Filter(new()
        {
            Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = new Regex($"^\\s*{Regex.Escape(value)}\\s*$") }),
        });
        await option.First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }

    /// <summary>
    /// Types <paramref name="value"/> into a picker and leaves it, so the field judges the
    /// typed value: a tree the cluster lists, or one that is not created yet.
    /// </summary>
    /// <param name="box">The picker's combobox.</param>
    /// <param name="value">The value.</param>
    public static async Task NameAsync(ILocator box, string value)
    {
        await box.FillAsync(value);
        await Expect(box).ToHaveValueAsync(value);
        await box.PressAsync("Tab");
    }

    /// <summary>
    /// Opens the row of <paramref name="table"/> that names <paramref name="text"/>: on a wide
    /// page it clicks the row's link, compact it opens the row's detail sheet and follows its
    /// action.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="table">The region holding the table.</param>
    /// <param name="text">The row's primary text, which its link also names.</param>
    /// <param name="compact">Whether the page is compact.</param>
    /// <param name="detailAction">The detail sheet's link that opens the row's page.</param>
    public static async Task FollowRowAsync(IPage page, ILocator table, string text, bool compact, string detailAction)
    {
        if (!compact)
        {
            await table.GetByRole(AriaRole.Link, new() { Name = text, Exact = true }).First.ClickAsync();
            return;
        }

        await table.GetByRole(AriaRole.Button).Filter(new() { HasText = text }).First.ClickAsync();
        var sheet = page.GetByRole(AriaRole.Dialog, new() { Name = text, Exact = true });
        await Expect(sheet).ToBeVisibleAsync();
        await sheet.GetByRole(AriaRole.Link, new() { Name = detailAction, Exact = true }).ClickAsync();
    }

    /// <summary>Types the object's name into a destructive confirmation, captures it armed, and confirms it.</summary>
    /// <param name="page">The page.</param>
    /// <param name="title">The confirmation's title, such as <c>Delete this rule</c>.</param>
    /// <param name="field">The name field's label, such as <c>Rule name</c>.</param>
    /// <param name="name">The object's name.</param>
    /// <param name="confirm">The confirming button's text.</param>
    /// <param name="armed">Runs once the name is typed and the confirmation is armed: its screenshot.</param>
    public static async Task ConfirmDestructiveAsync(IPage page, string title, string field, string name, string confirm, Func<Task> armed)
    {
        var dialog = page.GetByRole(AriaRole.Alertdialog, new() { Name = title });
        await Expect(dialog).ToBeVisibleAsync();
        var button = dialog.GetByRole(AriaRole.Button, new() { Name = confirm, Exact = true });
        await Expect(button).ToBeDisabledAsync();
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = field, Exact = true }).FillAsync(name);
        await Expect(button).ToBeEnabledAsync();
        await armed();
        await button.ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
    }

    /// <summary>
    /// Waits for a toast that says <paramref name="text"/>. The newest match is awaited,
    /// because a rule recorded on several trees raises the same toast once per tree.
    /// </summary>
    /// <param name="toasts">The shell's toast messages.</param>
    /// <param name="text">What the toast says.</param>
    public static Task ExpectToastAsync(ILocator toasts, string text) =>
        Expect(toasts.Filter(new() { HasText = text }).Last).ToBeVisibleAsync();
}
