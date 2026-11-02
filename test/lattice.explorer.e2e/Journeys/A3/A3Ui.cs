using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3;

/// <summary>The pieces of the Explorer's design system A3's page objects share.</summary>
internal static class A3Ui
{
    /// <summary>The value of the definition-list row whose term is exactly <paramref name="term"/>, within <paramref name="scope"/>.</summary>
    /// <param name="scope">Where to look.</param>
    /// <param name="term">The term.</param>
    public static ILocator Term(ILocator scope, string term) =>
        scope.Locator(".lt-dl__row")
            .Filter(new() { Has = scope.Page.Locator("dt", new() { HasTextRegex = new Regex("^\\s*" + Regex.Escape(term) + "\\s*$") }) })
            .Locator("dd");

    /// <summary>The destructive-action confirmation titled <paramref name="title"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="title">The dialog's title.</param>
    public static ILocator Confirmation(IPage page, string title) =>
        page.GetByRole(AriaRole.Alertdialog, new() { Name = title });

    /// <summary>Types the object's name into an open confirmation, leaving it ready to confirm.</summary>
    /// <param name="page">The page.</param>
    /// <param name="title">The dialog's title.</param>
    /// <param name="name">The name to type.</param>
    /// <returns>The dialog.</returns>
    public static async Task<ILocator> TypeConfirmationAsync(IPage page, string title, string name)
    {
        var dialog = Confirmation(page, title);
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.Locator(".lt-confirm input").FillAsync(name);
        return dialog;
    }

    /// <summary>Confirms an open confirmation with its button, and waits for it to close.</summary>
    /// <param name="dialog">The dialog.</param>
    /// <param name="confirm">The confirm button's text.</param>
    public static async Task ConfirmAsync(ILocator dialog, string confirm)
    {
        var button = dialog.GetByRole(AriaRole.Button, new() { Name = confirm, Exact = true });
        await Expect(button).ToBeEnabledAsync();
        await button.ClickAsync();
        await Expect(dialog).ToBeHiddenAsync(new() { Timeout = 30_000 });
    }

    /// <summary>Waits for a toast that says <paramref name="message"/>.</summary>
    /// <param name="shell">The chrome.</param>
    /// <param name="message">The toast's text, or part of it.</param>
    public static Task ExpectToastAsync(ShellPage shell, string message) =>
        Expect(shell.Toasts.Filter(new() { HasText = message }).First).ToBeVisibleAsync(new() { Timeout = 30_000 });

    /// <summary>Waits until the address carries <paramref name="query"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="query">A query fragment, such as <c>tab=views</c>.</param>
    public static Task ExpectAddressAsync(IPage page, string query) =>
        Expect(page).ToHaveURLAsync(new Regex(Regex.Escape(query)));
}
