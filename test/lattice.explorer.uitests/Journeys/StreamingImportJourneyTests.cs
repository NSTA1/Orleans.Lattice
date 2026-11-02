using System.Text;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Accessibility;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5365: a data steward loads files into trees. From the Data directory, New tree
/// from file... creates a tree and opens its Import dialog on a seed; the NDJSON file is
/// checked, then streamed in, and the tree holds it. On a tree that already holds keys, a
/// CSV upsert writes every readable row and lists each refused line, downloadable in full.
/// A file out of key order is refused by the seed's pre-flight pass with its line and
/// nothing is written. The Cluster tools' bulk load offers the same pipeline for a file,
/// and a caller who may only read is shown no Import control.
/// </summary>
/// <remarks>
/// Every step writes a screenshot to <c>screenshots/journey-g1-streaming-import/</c> under
/// the test output, which the UI lane uploads, so the pull request walks the flow from the
/// head commit's own run.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class StreamingImportJourneyTests : UiTestBase
{
    private const string ScreenshotFolder = "journey-g1-streaming-import";

    [Test]
    public async Task A_steward_seeds_a_new_tree_from_a_file_then_upserts_a_csv_into_it()
    {
        var world = await UiHosts.WorldAsync();
        var tree = "g1-orders-" + Guid.NewGuid().ToString("N")[..8];

        // The affordance: New tree from file... on the Data directory.
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var fromFile = content.GetByRole(AriaRole.Button, new() { Name = "New tree from file...", Exact = true });
        await Expect(fromFile).ToBeVisibleAsync();
        await StepAsync(page, 1, "directory-new-tree-from-file");

        // Create the tree through the Cluster area's creation seam.
        await fromFile.ClickAsync();
        var create = page.GetByRole(AriaRole.Dialog, new() { Name = "New tree from file" });
        await create.GetByLabel("Tree name").FillAsync(tree);
        await create.GetByLabel("Physical shards").FillAsync("4");
        await create.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await StepAsync(page, 2, "create-tree-review");
        await create.GetByRole(AriaRole.Button, new() { Name = "Create tree" }).ClickAsync();

        // The new tree's page opens with the Import dialog on a seed.
        var import = page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}" });
        await Expect(import).ToBeVisibleAsync();
        await Expect(import.Locator(".lt-data-import")).ToHaveAttributeAsync("data-lt-import-state", "choose");
        await Expect(import.GetByLabel("Mode")).ToHaveValueAsync("Seed");
        await import.Locator("#data-import-file").SetInputFilesAsync(new FilePayload
        {
            Name = "orders.ndjson",
            MimeType = "application/x-ndjson",
            Buffer = Encoding.UTF8.GetBytes(Orders(20_000)),
        });
        await import.GetByLabel("Key").FillAsync("order/{/id}");
        await StepAsync(page, 3, "seed-dialog-file-chosen");
        await AxeConformance.SweepAsync(page, "the Import dialog");

        await import.GetByRole(AriaRole.Button, new() { Name = "Seed tree" }).ClickAsync();
        var state = import.Locator(".lt-data-import");
        await Expect(import.GetByRole(AriaRole.Progressbar)).ToBeVisibleAsync();
        await StepAsync(page, 4, "seed-progress");
        await Expect(state).ToHaveAttributeAsync("data-lt-import-state", "succeeded", new() { Timeout = 60_000 });
        await Expect(import.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync("data-lt-import-committed", "20000");
        await Expect(import.Locator(".lt-data-import__result")).ToContainTextAsync("Seeded 20,000 entries. The tree holds 20,000 live keys.");
        await StepAsync(page, 5, "seed-succeeded");
        await import.GetByRole(AriaRole.Button, new() { Name = "Done" }).ClickAsync();
        await Expect(content.Locator(".lt-table__row").First).ToContainTextAsync("order/00000");
        await StepAsync(page, 6, "seeded-tree-keys");

        // An upsert of a CSV into the tree that now holds keys: every readable row lands,
        // and each refused line is listed with its line number.
        var importButton = content.GetByRole(AriaRole.Button, new() { Name = "Import...", Exact = true });
        await Expect(importButton).ToHaveAttributeAsync("data-lt-command", "data.import");
        await importButton.ClickAsync();
        await Expect(import.GetByLabel("Mode")).ToHaveValueAsync("Upsert");
        await import.Locator("#data-import-file").SetInputFilesAsync(new FilePayload
        {
            Name = "corrections.csv",
            MimeType = "text/csv",
            Buffer = Encoding.UTF8.GetBytes("id,total,status\r\n000017,12.5,shipped\r\n000101,\"7\",held\r\n000233\r\nzzz,1,new\r\n"),
        });
        await Expect(import.GetByLabel("Format")).ToHaveValueAsync("Csv");
        await import.GetByLabel("Key").FillAsync("order/{id}");
        await import.GetByLabel("Infer numbers and booleans").CheckAsync();
        await StepAsync(page, 7, "upsert-csv-dialog");
        await import.GetByRole(AriaRole.Button, new() { Name = "Import", Exact = true }).ClickAsync();

        await Expect(state).ToHaveAttributeAsync("data-lt-import-state", "succeeded", new() { Timeout = 30_000 });
        await Expect(import.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync("data-lt-import-committed", "3");
        await Expect(import.Locator("[data-lt-import-refused]")).ToHaveAttributeAsync("data-lt-import-refused", "1");
        var refusedRow = import.Locator("[data-lt-import-refusals] tbody tr");
        await Expect(refusedRow).ToHaveCountAsync(1);
        await Expect(refusedRow.Locator("td").First).ToHaveTextAsync("4");
        await Expect(refusedRow).ToContainTextAsync("The row has 1 field; the header has 3.");
        await StepAsync(page, 8, "upsert-result-with-refused-line");

        await import.GetByRole(AriaRole.Button, new() { Name = "Prepare the list of refused lines" }).ClickAsync();
        var download = import.Locator("[data-lt-import-download]");
        await Expect(download).ToBeVisibleAsync();
        var file = await page.RunAndWaitForDownloadAsync(() => download.ClickAsync());
        var text = await File.ReadAllTextAsync((await file.PathAsync())!);
        Assert.That(text, Does.Contain("\"line\":4").And.Contain("\"format\":\"lattice-import-rejections/1\""));
        await StepAsync(page, 9, "refused-lines-downloaded");
    }

    [Test]
    public async Task A_seed_out_of_key_order_is_refused_with_its_line_and_writes_nothing()
    {
        var world = await UiHosts.WorldAsync();
        var tree = "g1-unsorted-" + Guid.NewGuid().ToString("N")[..8];
        await world.TreeAdmin().CreateTreeAsync(tree, shardCount: 2);

        var page = await OpenAsync(world.Head, $"/data/{tree}?import=seed", WorldIdentities.Admin);
        var import = page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}" });
        await Expect(import).ToBeVisibleAsync();
        await import.Locator("#data-import-file").SetInputFilesAsync(new FilePayload
        {
            Name = "unsorted.txt",
            MimeType = "text/plain",
            Buffer = Encoding.UTF8.GetBytes("a=1\nc=3\nb=2\n"),
        });
        await Expect(import.GetByLabel("Format")).ToHaveValueAsync("KeyValue");
        await import.GetByRole(AriaRole.Button, new() { Name = "Seed tree" }).ClickAsync();

        await Expect(import.Locator(".lt-data-import")).ToHaveAttributeAsync("data-lt-import-state", "failed", new() { Timeout = 30_000 });
        await Expect(import.Locator(".lt-data-import__result")).ToContainTextAsync("Line 3: the key \"b\" does not sort after \"c\" on line 2.");
        await StepAsync(page, 10, "seed-refused-out-of-order");

        var count = await world.TreeAdmin().GetTreeStatsAsync(tree);
        Assert.That(count.TotalLiveKeys, Is.Zero, "a refused seed writes nothing");
    }

    [Test]
    public async Task The_cluster_tools_bulk_load_offers_a_file_and_a_reader_is_shown_no_import()
    {
        var world = await UiHosts.WorldAsync();

        var tools = await OpenAsync(world.Head, $"/cluster/trees/{ExplorerWorld.DemoTree}/tools", WorldIdentities.Admin);
        var link = Shell.Content(tools).Locator("[data-lt-bulk-from-file]");
        await Expect(link).ToHaveTextAsync("Seed it from an NDJSON, CSV or exported file");
        await StepAsync(tools, 11, "cluster-tools-bulk-load-from-file");

        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}", WorldIdentities.Alice);
        var content = Shell.Content(page);
        await Expect(content.Locator(".lt-table__row").First).ToBeVisibleAsync();
        await Expect(content.Locator("[data-lt-command=\"data.import\"]")).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Import...", Exact = true })).ToHaveCountAsync(0);
        await StepAsync(page, 12, "reader-sees-no-import");
    }

    private static string Orders(int count)
    {
        var text = new StringBuilder(count * 80);
        for (var i = 0; i < count; i++)
        {
            text.Append("{\"id\":\"").Append(i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture))
                .Append("\",\"total\":").Append(i % 97).Append(",\"status\":\"open\"}\n");
        }

        return text.ToString();
    }

    private static async Task StepAsync(IPage page, int step, string name)
    {
        var directory = Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", ScreenshotFolder);
        System.IO.Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{step:D2}-{name}.png") });
    }
}
