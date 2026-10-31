using System.Text;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Streaming export of a tree (issue #5366), in a real browser over the real download
/// endpoint: an operator exports a prefix as NDJSON and the whole tree, a user whose grant
/// covers only part of the tree downloads exactly that part with a trailer that says keys
/// were withheld, and a user the Data API does not admit to the tree is offered no export at all.
/// </summary>
/// <remarks>
/// Each step saves an ordered screenshot under <c>screenshots/data-export-journey/</c>,
/// which the UI lane uploads, for the pull request's journey section.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class DataExportJourneyTests : UiTestBase
{
    private static readonly string Tree = ExplorerWorld.ExportTree;

    [Test]
    public async Task An_operator_exports_a_prefix_as_ndjson_and_the_file_ends_with_a_complete_trailer()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data/" + Tree, WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(Tree);

        var export = Shell.Content(page).Locator("[data-lt-command=\"data.export\"]");
        await Expect(export).ToBeVisibleAsync();
        await CaptureAsync(page, "01-export-action-on-the-tree-page");

        await export.ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Export " + Tree });
        await Expect(dialog).ToBeVisibleAsync();
        await CaptureAsync(page, "02-choose-scope-and-format");

        await dialog.GetByLabel("What to export").SelectOptionAsync("Prefix");
        await dialog.GetByLabel("Prefix").FillAsync("public/");
        await CaptureAsync(page, "03-a-prefix-scope");

        await dialog.GetByRole(AriaRole.Button, new() { Name = "Prepare download" }).ClickAsync();
        var download = dialog.GetByRole(AriaRole.Button, new() { Name = "Download", Exact = true });
        await Expect(download).ToBeVisibleAsync();
        await CaptureAsync(page, "04-download-ready");

        var file = await page.RunAndWaitForDownloadAsync(() => download.ClickAsync());
        var lines = await ReadLinesAsync(file);
        await Expect(dialog.Locator("[data-lt-export-state=finished]")).ToBeVisibleAsync();
        await Expect(dialog.Locator(".lt-data-export__result")).ToContainTextAsync("Every key in the scope is in the file.");
        await CaptureAsync(page, "05-export-complete");

        Assert.Multiple(() =>
        {
            Assert.That(file.SuggestedFilename, Is.EqualTo(Tree + ".ndjson"));
            Assert.That(lines, Has.Length.EqualTo(ExplorerWorld.ExportPublicEntries + 2), "every public entry, the binary one, and the trailer");
            Assert.That(lines[0], Is.EqualTo("{\"key\":\"public/0001\",\"value\":{\"line\":1,\"status\":\"idle\"}}"));
            Assert.That(lines.Single(line => line.Contains("public/blob", StringComparison.Ordinal)), Is.EqualTo("{\"key\":\"public/blob\",\"value\":\"AP8QIA==\",\"encoding\":\"base64\"}"));
            Assert.That(lines, Has.None.Contains("private/"));
            Assert.That(lines[^1], Is.EqualTo($"{{\"trailer\":{{\"format\":\"lattice-export/1\",\"items\":{ExplorerWorld.ExportPublicEntries + 1},\"complete\":true,\"refused\":\"none\",\"reason\":null}}}}"));
        });

        // The ticket was single-use: the console is still here and nothing navigated away.
        await Expect(Shell.Heading(page)).ToHaveTextAsync(Tree);
    }

    [Test]
    public async Task A_user_whose_grant_covers_part_of_the_tree_gets_that_part_and_a_trailer_that_says_so()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data/" + Tree, WorldIdentities.Dave);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(Tree);

        await Shell.Content(page).Locator("[data-lt-command=\"data.export\"]").ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Export " + Tree });
        await dialog.GetByLabel("Format").SelectOptionAsync("Csv");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Prepare download" }).ClickAsync();

        var file = await page.RunAndWaitForDownloadAsync(() => dialog.GetByRole(AriaRole.Button, new() { Name = "Download", Exact = true }).ClickAsync());
        var text = await ReadTextAsync(file);
        await Expect(dialog.Locator(".lt-data-export__result")).ToContainTextAsync("the file is not complete");
        await CaptureAsync(page, "06-partial-access-says-keys-were-withheld");

        var rows = text.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        Assert.Multiple(() =>
        {
            Assert.That(file.SuggestedFilename, Is.EqualTo(Tree + ".csv"));
            Assert.That(rows[0], Is.EqualTo("key,value,encoding"));
            Assert.That(rows[1..^1], Has.All.StartsWith("\"public/"), "only the keys the grant admits");
            Assert.That(rows[1..^1], Has.Length.EqualTo(ExplorerWorld.ExportPublicEntries + 1));
            Assert.That(rows[^1], Does.StartWith(",\"{").And.EndWith(",trailer"));
            Assert.That(rows[^1], Does.Contain("\"\"complete\"\":false").And.Contain("\"\"refused\"\":\"\"some\"\""));
        });
    }

    [Test]
    public async Task A_user_the_data_api_does_not_admit_to_the_tree_is_offered_no_export()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data/" + Tree, WorldIdentities.Carol);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(Tree);

        // The keys panel has finished asking, so the action has had its chance to appear.
        await Expect(Shell.Content(page).Locator(".lt-skeleton")).ToHaveCountAsync(0);
        await Expect(Shell.Content(page).Locator("[data-lt-command=\"data.export\"]")).ToHaveCountAsync(0);
        await CaptureAsync(page, "07-no-export-without-read-access");
    }

    private static async Task<string[]> ReadLinesAsync(IDownload file) =>
        (await ReadTextAsync(file)).Split('\n', StringSplitOptions.RemoveEmptyEntries);

    private static async Task<string> ReadTextAsync(IDownload file)
    {
        var path = await file.PathAsync();
        var text = await File.ReadAllTextAsync(path, Encoding.UTF8);
        Assert.That(text, Does.Not.StartWith("\uFEFF"), "the file has no byte-order mark");
        return text;
    }

    private static async Task CaptureAsync(IPage page, string step)
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "data-export-journey");
        Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, step + ".png"), FullPage = false });
    }
}
