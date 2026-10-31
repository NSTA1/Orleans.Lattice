using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// The Explorer's type-the-name confirmation (<c>LtConfirmDestructive</c>): an alert dialog
/// titled for the action, a single name field, and a destructive submit that stays disabled
/// until the typed name matches.
/// </summary>
/// <param name="page">The page showing the dialog.</param>
/// <param name="title">The dialog's title, such as <c>Delete this group</c>.</param>
internal sealed class ConfirmDialog(IPage page, string title)
{
    /// <summary>The dialog.</summary>
    public ILocator Dialog { get; } = page.GetByRole(AriaRole.Alertdialog, new() { Name = title });

    /// <summary>Waits for the dialog and types <paramref name="name"/> into its name field.</summary>
    /// <param name="name">The object's name, exactly as the dialog asks for it.</param>
    public async Task TypeAsync(string name)
    {
        await Expect(Dialog).ToBeVisibleAsync();
        await Expect(Dialog).ToContainTextAsync(name);
        await Dialog.GetByRole(AriaRole.Textbox).FillAsync(name);
    }

    /// <summary>Presses the dialog's confirming button and waits for the dialog to close.</summary>
    /// <param name="confirmText">The button's text, such as <c>Delete group</c>.</param>
    public async Task ConfirmAsync(string confirmText)
    {
        var confirm = Dialog.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true });
        await Expect(confirm).ToBeEnabledAsync();
        await confirm.ClickAsync();
        await Expect(Dialog).ToBeHiddenAsync();
    }
}
