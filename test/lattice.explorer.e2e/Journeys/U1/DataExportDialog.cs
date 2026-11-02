using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>The Export dialog (#5366): the scope and format, the prepared download, and the trailer's verdict.</summary>
/// <param name="page">The page.</param>
/// <param name="tree">The tree the dialog exports.</param>
internal sealed class DataExportDialog(IPage page, string tree)
{
    /// <summary>The dialog.</summary>
    public ILocator Dialog => page.GetByRole(AriaRole.Dialog, new() { Name = "Export " + tree });

    /// <summary>The What to export select.</summary>
    public ILocator Scope => Dialog.GetByLabel("What to export", new() { Exact = true });

    /// <summary>The Prefix field.</summary>
    public ILocator Prefix => Dialog.GetByLabel("Prefix", new() { Exact = true });

    /// <summary>The Format select.</summary>
    public ILocator Format => Dialog.GetByLabel("Format", new() { Exact = true });

    /// <summary>The Prepare download control.</summary>
    public ILocator Prepare => Dialog.GetByRole(AriaRole.Button, new() { Name = "Prepare download", Exact = true });

    /// <summary>The Download control of a prepared download.</summary>
    public ILocator Download => Dialog.GetByRole(AriaRole.Button, new() { Name = "Download", Exact = true });

    /// <summary>The trailer's verdict.</summary>
    public ILocator Result => Dialog.Locator(".lt-data-export__result");

    /// <summary>The element whose <c>data-lt-export-state</c> names the download's state.</summary>
    public ILocator State => Dialog.Locator(".lt-data-export");

    /// <summary>The Done control.</summary>
    public ILocator Done => Dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true });

    /// <summary>Waits for the dialog's form.</summary>
    public Task WaitOpenAsync() => Expect(Dialog).ToBeVisibleAsync(new() { Timeout = 30_000 });
}
