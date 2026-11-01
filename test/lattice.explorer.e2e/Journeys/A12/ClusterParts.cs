using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// The parts every Cluster page shares, as A12 drives them: a definition list's value by its
/// term, the rows of a table (or of its compact list), the typed confirmation every
/// destructive Cluster operation asks for, and the patience a structural operation needs.
/// </summary>
internal static class ClusterParts
{
    /// <summary>How long a structural operation (a reshard, a snapshot, an orphan pass) may take on the estate: coordinators step once per tick.</summary>
    public static readonly TimeSpan Structural = TimeSpan.FromMinutes(10);

    /// <summary>The value of the definition whose term reads <paramref name="term"/> exactly.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="term">The term.</param>
    public static ILocator Term(ShellPage shell, string term) =>
        shell.Content.Locator("dl div").Filter(new() { Has = shell.Page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }) }).Locator("dd");

    /// <summary>The rows of the page's table, or of its compact list at a narrow width.</summary>
    /// <param name="shell">The shell.</param>
    public static ILocator Rows(ShellPage shell) => shell.Content.Locator("tbody tr, li.lt-table-list__row");

    /// <summary>The row that names <paramref name="name"/> exactly.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="name">The name, such as a tree id.</param>
    public static ILocator Row(ShellPage shell, string name) =>
        Rows(shell).Filter(new() { Has = shell.Page.GetByText(name, new() { Exact = true }) });

    /// <summary>The typed confirmation titled <paramref name="title"/>.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="title">The dialog's title.</param>
    public static ILocator Confirmation(ShellPage shell, string title) =>
        shell.Page.GetByRole(AriaRole.Alertdialog, new() { Name = title });

    /// <summary>Types <paramref name="objectName"/> into the confirmation, which enables its confirm button.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="title">The dialog's title.</param>
    /// <param name="objectName">The name the dialog asks for.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    public static async Task TypeConfirmationAsync(ShellPage shell, string title, string objectName, string confirmText)
    {
        var dialog = Confirmation(shell, title);
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.GetByRole(AriaRole.Textbox).FillAsync(objectName);
        await Expect(dialog.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true })).ToBeEnabledAsync();
    }

    /// <summary>Confirms the dialog <see cref="TypeConfirmationAsync"/> filled in.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="title">The dialog's title.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    public static async Task ConfirmAsync(ShellPage shell, string title, string confirmText)
    {
        var dialog = Confirmation(shell, title);
        await dialog.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
    }

    /// <summary>A toast reading <paramref name="message"/>.</summary>
    /// <param name="shell">The shell.</param>
    /// <param name="message">The message.</param>
    public static ILocator Toast(ShellPage shell, string message) => shell.Toasts.Filter(new() { HasText = message });

    /// <summary>Waits as long as a structural operation may take.</summary>
    public static LocatorAssertionsToHaveTextOptions Patiently => new() { Timeout = (float)Structural.TotalMilliseconds };

    /// <summary>Waits as long as a structural operation may take.</summary>
    public static LocatorAssertionsToContainTextOptions PatientlyContaining => new() { Timeout = (float)Structural.TotalMilliseconds };

    /// <summary>Waits as long as a structural operation may take.</summary>
    public static LocatorAssertionsToBeVisibleOptions PatientlyVisible => new() { Timeout = (float)Structural.TotalMilliseconds };
}
