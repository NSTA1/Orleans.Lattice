using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>The Import dialog (#5365): choosing a file and its settings, the run's progress, and its result.</summary>
/// <param name="page">The page.</param>
/// <param name="tree">The tree the dialog imports into.</param>
internal sealed class DataImportDialog(IPage page, string tree)
{
    /// <summary>The dialog.</summary>
    public ILocator Dialog => page.GetByRole(AriaRole.Dialog, new() { Name = "Import into " + tree });

    /// <summary>The element whose <c>data-lt-import-state</c> names the run's phase.</summary>
    public ILocator State => Dialog.Locator(".lt-data-import");

    /// <summary>The file input.</summary>
    public ILocator File => Dialog.Locator("#data-import-file");

    /// <summary>The Format select.</summary>
    public ILocator Format => Dialog.GetByLabel("Format", new() { Exact = true });

    /// <summary>The Mode select.</summary>
    public ILocator Mode => Dialog.GetByLabel("Mode", new() { Exact = true });

    /// <summary>The Key field.</summary>
    public ILocator Key => Dialog.GetByLabel("Key", new() { Exact = true });

    /// <summary>The Value select.</summary>
    public ILocator ValueSource => Dialog.GetByLabel("Value", new() { Exact = true });

    /// <summary>The committed count, as an attribute.</summary>
    public ILocator Committed => Dialog.Locator("[data-lt-import-committed]");

    /// <summary>The refused count, as an attribute.</summary>
    public ILocator Refused => Dialog.Locator("[data-lt-import-refused]");

    /// <summary>The rows of the refused-lines table.</summary>
    public ILocator RefusedRows => Dialog.Locator("[data-lt-import-refusals] tbody tr");

    /// <summary>The result's sentence.</summary>
    public ILocator Result => Dialog.Locator(".lt-data-import__result");

    /// <summary>The Stop import control.</summary>
    public ILocator Stop => Dialog.Locator("[data-lt-import-stop]");

    /// <summary>The Download refused lines control.</summary>
    public ILocator DownloadRefused => Dialog.Locator("[data-lt-import-download]");

    /// <summary>The Prepare the list of refused lines control.</summary>
    public ILocator PrepareRefused => Dialog.GetByRole(AriaRole.Button, new() { Name = "Prepare the list of refused lines" });

    /// <summary>The Done control of a finished run.</summary>
    public ILocator Done => Dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true });

    /// <summary>The submit control: Import for an upsert, Seed tree for a seed.</summary>
    /// <param name="seed">Whether the mode is a seed.</param>
    public ILocator Start(bool seed) => Dialog.GetByRole(AriaRole.Button, new() { Name = seed ? "Seed tree" : "Import", Exact = true });

    /// <summary>Waits for the dialog's form.</summary>
    public async Task WaitOpenAsync()
    {
        await Expect(Dialog).ToBeVisibleAsync(new() { Timeout = 30_000 });
        await Expect(State).ToHaveAttributeAsync("data-lt-import-state", "choose");
    }

    /// <summary>Chooses the file at <paramref name="path"/>.</summary>
    /// <param name="path">The file's path.</param>
    public async Task ChooseFileAsync(string path)
    {
        await File.SetInputFilesAsync(path);
        await Expect(Dialog).ToContainTextAsync(Path.GetFileName(path));
    }

    /// <summary>Waits until the run reaches <paramref name="state"/>.</summary>
    /// <param name="state">The phase, lower-case: <c>succeeded</c>, <c>stopped</c>, ...</param>
    /// <param name="timeout">How long to wait, in milliseconds.</param>
    public Task WaitForStateAsync(string state, float timeout = 120_000) =>
        Expect(State).ToHaveAttributeAsync("data-lt-import-state", state, new() { Timeout = timeout });

    /// <summary>The committed count the dialog reports.</summary>
    public async Task<long> CommittedAsync() =>
        long.Parse((await Committed.GetAttributeAsync("data-lt-import-committed"))!, System.Globalization.CultureInfo.InvariantCulture);
}
