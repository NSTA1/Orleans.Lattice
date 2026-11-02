using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// The Explorer's typed confirmation (<c>LtConfirmDestructive</c>): an alert dialog that
/// names what it changes and enables its confirm button only once the object's name is typed.
/// </summary>
/// <param name="page">The page the dialog opens on.</param>
/// <param name="title">The dialog's title, such as <c>Resize tree</c>.</param>
internal sealed class ConfirmDialog(IPage page, string title)
{
    /// <summary>The dialog.</summary>
    public ILocator Root => page.GetByRole(AriaRole.Alertdialog, new() { Name = title, Exact = true });

    /// <summary>Waits for the dialog, then types <paramref name="objectName"/> and sees the confirm button enable.</summary>
    /// <param name="objectName">The name the dialog asks for.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    public async Task TypeAsync(string objectName, string confirmText)
    {
        await Expect(Root).ToBeVisibleAsync();
        await Expect(Root.Locator(".lt-confirm__name")).ToHaveTextAsync(objectName);
        await Expect(Confirm(confirmText)).ToBeDisabledAsync();
        await Root.GetByRole(AriaRole.Textbox).FillAsync(objectName);
        await Expect(Confirm(confirmText)).ToBeEnabledAsync();
    }

    /// <summary>Presses the confirm button and waits for the dialog to close.</summary>
    /// <param name="confirmText">The confirm button's text.</param>
    public async Task ConfirmAsync(string confirmText)
    {
        await Confirm(confirmText).ClickAsync();
        await Expect(Root).ToBeHiddenAsync();
    }

    private ILocator Confirm(string confirmText) =>
        Root.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true });
}
