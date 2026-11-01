using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U4;

/// <summary>
/// The tree page's Export dialog (G2, #5366) as U4 drives it: choose the scope and the
/// format, prepare the single-use download, take it, and read the trailer's verdict.
/// </summary>
/// <param name="page">The session's page.</param>
/// <param name="tree">The tree being exported.</param>
public sealed class ExportDialog(IPage page, string tree)
{
    /// <summary>The dialog.</summary>
    public ILocator Dialog { get; } = page.GetByRole(AriaRole.Dialog, new() { Name = "Export " + tree, Exact = true });

    /// <summary>The verdict once the download has finished.</summary>
    public ILocator Result => Dialog.Locator(".lt-data-export__result");

    /// <summary>Asserts the dialog is open on its first page.</summary>
    public async Task ExpectChoosingAsync()
    {
        await Expect(Dialog).ToBeVisibleAsync();
        await Expect(Dialog.Locator("[data-lt-export-state=choose]")).ToBeVisibleAsync();
    }

    /// <summary>Chooses the whole tree.</summary>
    public Task ChooseWholeTreeAsync() => Dialog.GetByLabel("What to export").SelectOptionAsync("Tree");

    /// <summary>Chooses the key range from <paramref name="from"/> (included) up to <paramref name="upTo"/> (excluded).</summary>
    /// <param name="from">The first key.</param>
    /// <param name="upTo">The key the range stops before.</param>
    public async Task ChooseRangeAsync(string from, string upTo)
    {
        await Dialog.GetByLabel("What to export").SelectOptionAsync("Range");
        await Dialog.GetByLabel("From key").FillAsync(from);
        await Dialog.GetByLabel("Up to key").FillAsync(upTo);
    }

    /// <summary>Chooses the file format: <c>Ndjson</c> or <c>Csv</c>.</summary>
    /// <param name="format">The format's option value.</param>
    public Task ChooseFormatAsync(string format) => Dialog.GetByLabel("Format").SelectOptionAsync(format);

    /// <summary>Prepares the download and waits for the single-use <b>Download</b> button.</summary>
    public async Task PrepareAsync()
    {
        await Dialog.GetByRole(AriaRole.Button, new() { Name = "Prepare download", Exact = true }).ClickAsync();
        await Expect(Dialog.Locator("[data-lt-export-state=ready]")).ToBeVisibleAsync();
        await Expect(DownloadButton).ToBeVisibleAsync();
    }

    private ILocator DownloadButton => Dialog.GetByRole(AriaRole.Button, new() { Name = "Download", Exact = true });

    /// <summary>Takes the download and returns its suggested name and text, once the dialog says the file is complete.</summary>
    public async Task<(string FileName, string Text)> DownloadAsync()
    {
        var file = await page.RunAndWaitForDownloadAsync(() => DownloadButton.ClickAsync(), new() { Timeout = 120_000 });
        var path = await file.PathAsync();
        var bytes = await File.ReadAllBytesAsync(path);
        Assert.That(bytes.AsSpan().StartsWith((ReadOnlySpan<byte>)[0xEF, 0xBB, 0xBF]), Is.False, "the export has no byte-order mark");
        await Expect(Dialog.Locator("[data-lt-export-state=finished]")).ToBeVisibleAsync(new() { Timeout = 120_000 });
        await Expect(Result).ToContainTextAsync(new Regex("Every key in the scope is in the file\\.$"));
        return (file.SuggestedFilename, Encoding.UTF8.GetString(bytes));
    }

    /// <summary>Closes the finished dialog with <b>Done</b>.</summary>
    public async Task DoneAsync()
    {
        await Dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
        await Expect(Dialog).ToBeHiddenAsync();
    }
}
