using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U4;

/// <summary>Steps 1 to 5: dana's console, and the Data area read end to end.</summary>
public sealed partial class AuditorsReviewChapter
{
    /// <summary>The first order of the export range, included.</summary>
    private const string RangeFrom = "order/000100";

    /// <summary>The key the export range stops before, so the range ends at <c>order/000199</c>.</summary>
    private const string RangeUpTo = "order/000200";

    /// <summary>How many orders the range holds.</summary>
    private const int RangeCount = 100;

    /// <summary>An order dana opens by browsing the Keys tab.</summary>
    private const string Order1 = "order/000001";

    // U4.1: the spine shows the read areas her rules grant, and nothing else she could open.
    private static async Task DanaSignsInAsync(PersonaSession dana)
    {
        await dana.Shell.ExpectSignedInAsync(dana.Persona.Id);
        var spine = await dana.Shell.ReadSpineAsync();
        Assert.Multiple(() =>
        {
            foreach (var area in new[] { "data", "apps", "schema", "replication", "telemetry" })
            {
                Assert.That(spine.CanOpen(area), Is.True, $"dana can open {area}: {spine}");
            }

            Assert.That(spine.Unavailable, Does.Contain("backups"), $"Backups is drawn, but not open, for a reader without a backup grant: {spine}");
            Assert.That(spine.Stops, Has.None.EqualTo("access"), $"Access is hidden from dana: {spine}");
            Assert.That(spine.CanOpen("cluster"), Is.True, $"Cluster is hers through the cluster-wide Telemetry grant: {spine}");
        });
        await SnapAsync(dana, "1", "signed-in", "dana's console: the areas her grants open, Backups drawn but not open, no Access");
    }

    // U4.2: History of two orders, read as far as the write-ahead log's window keeps them.
    private static async Task HistoryOfTheOrdersAsync(PersonaSession dana)
    {
        var tree = new TreeWorkspacePage(dana.Shell);
        await tree.OpenFromDirectoryAsync(Names.Trees.FactoryOrders);
        await tree.ChooseTabAsync("History", "history");
        await SnapAsync(dana, "2", "choose-a-key", "History of factory-orders asks for a key");

        await tree.ChooseKeyAsync(Names.Orders.Order17, requireRevisions: false);
        await SnapAsync(dana, "2", "order-17", "order/000017's history: what is still in the write-ahead log's window");
        Coverage.Hit("tab:data/history");

        await tree.ChooseTabAsync("Keys", "keys");
        await tree.OpenEntryAsync(Order1, Order1);
        await Expect(tree.EntryValue).ToContainTextAsync("\"total\"");
        await SnapAsync(dana, "2", "order-1-entry", "order/000001 today");

        // factory-orders has no change-history view: the library ships no provider for one
        // (#5611), and one built by the host stalls on this busy tree (#5612). So History reads
        // only the write-ahead log's window, the seed may have left it, and the timeline cannot
        // show the key as it stood before U2. The note says why.
        await tree.OpenHistoryOfThisKeyAsync(Order1, requireRevisions: false);
        await HistoryWithoutAViewKeepsOnlyTheWal.ReproducesAsync(async () =>
            await Expect(dana.Shell.Content.GetByText(new Regex("^(This tree has no change-history view|Older revisions were trimmed)"))).ToBeVisibleAsync());
        await SnapAsync(dana, "2", "order-1-history", "order/000001's history: only the write-ahead log's window, and a note saying why (#5611, #5612)");
    }

    // U4.3: the telemetry, which dana reads only through the nested factory-staff group, and
    // the line feed's latest reading, whose History pages.
    private static async Task TelemetryThroughTheNestedGroupAsync(PersonaSession dana)
    {
        var tree = new TreeWorkspacePage(dana.Shell);
        await tree.OpenFromDirectoryAsync(Names.Trees.FactoryTelemetry);
        await Expect(dana.Shell.Content.Locator("a.lt-data-key").First).ToBeVisibleAsync();
        await SnapAsync(dana, "3", "telemetry-keys", "factory-telemetry, readable to dana only through factory-staff, which nests auditors");
        Coverage.Hit("use-case:access/through-nested-group");

        await tree.OpenEntryAsync(LineFeedControl.LatestReadingKey, LineFeedControl.LatestReadingKey);
        await tree.OpenHistoryOfThisKeyAsync(LineFeedControl.LatestReadingKey);
        await tree.StopFollowingAsync();
        await Expect(tree.RevisionCount).ToHaveTextAsync(new Regex("revisions so far$"));
        await Expect(tree.LoadOlder).ToBeVisibleAsync();
        var first = await tree.RevisionCountAsync();
        await SnapAsync(dana, "3", "latest-reading", "The line feed's latest reading: the newest page of its revisions, with older ones to load");

        await tree.LoadOlder.ClickAsync();
        await Expect(tree.RevisionCount).Not.ToHaveTextAsync(new Regex("^" + first.ToString(System.Globalization.CultureInfo.InvariantCulture) + " "), new() { Timeout = 60_000 });
        Assert.That(await tree.RevisionCountAsync(), Is.GreaterThan(first), "Load older revisions adds the next page below the first");
        await SnapAsync(dana, "3", "older-revisions", "Load older revisions: the next page joins the timeline");
        Coverage.Hit("use-case:data/history");

        // Show as of: the reading as it stood a moment ago - one revision is marked in effect.
        var times = await tree.RevisionTimesAsync();
        Assert.That(times, Has.Count.GreaterThan(3), "the latest reading has a page of revisions");
        await tree.ShowAsOfAsync(times[2].AddSeconds(1));
        await SnapAsync(dana, "3", "as-of", "Show as of: the reading as it stood a moment ago, its revision then in effect marked");
        Coverage.Hit("query:shell.at");

        await tree.ShowLatestAsync();
        await SnapAsync(dana, "3", "latest", "Latest: the whole timeline again");
    }

    // U4.4: two streamed exports - the archive as NDJSON, and a key range of the orders as CSV.
    private static async Task ExportsAsync(PersonaSession dana)
    {
        var tree = new TreeWorkspacePage(dana.Shell);
        var archive = Names.Trees.FactoryOrdersArchive;
        await tree.OpenFromDirectoryAsync(archive);
        await tree.Export.ClickAsync();
        var dialog = new ExportDialog(dana.Page, archive);
        await dialog.ExpectChoosingAsync();
        await dialog.ChooseWholeTreeAsync();
        await dialog.ChooseFormatAsync("Ndjson");
        await SnapAsync(dana, "4", "archive-choose", "Export the whole archive as NDJSON");
        await dialog.PrepareAsync();
        await SnapAsync(dana, "4", "archive-ready", "The single-use download is ready");

        var (archiveName, archiveText) = await dialog.DownloadAsync();
        var lines = archiveText.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Multiple(() =>
        {
            Assert.That(archiveName, Is.EqualTo(archive + ".ndjson"));
            Assert.That(lines, Has.Length.EqualTo(Names.Files.ArchiveRows + 1), "every archived order, then the trailer");
            Assert.That(lines[0], Does.StartWith("{\"key\":\"order/"));
            Assert.That(lines[^1], Does.StartWith("{\"trailer\":").And.Contain("\"complete\":true").And.Contain($"\"items\":{Names.Files.ArchiveRows}"));
        });
        await SnapAsync(dana, "4", "archive-done", "The archive's 5,000 entries are in the file, and its trailer says it is complete");
        await dialog.DoneAsync();

        var orders = Names.Trees.FactoryOrders;
        await tree.OpenFromDirectoryAsync(orders);
        await tree.Export.ClickAsync();
        dialog = new ExportDialog(dana.Page, orders);
        await dialog.ExpectChoosingAsync();
        await dialog.ChooseRangeAsync(RangeFrom, RangeUpTo);
        await dialog.ChooseFormatAsync("Csv");
        await SnapAsync(dana, "4", "range-choose", "Export order/000100 up to order/000200 (so through order/000199) as CSV");
        await dialog.PrepareAsync();

        var (rangeName, rangeText) = await dialog.DownloadAsync();
        var rows = rangeText.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        Assert.Multiple(() =>
        {
            Assert.That(rangeName, Is.EqualTo(orders + ".csv"));
            Assert.That(rows[0], Is.EqualTo("key,value,encoding"));
            Assert.That(rows, Has.Length.EqualTo(RangeCount + 2), "the header, the hundred orders of the range, and the trailer");
            Assert.That(rows[1], Does.Contain(RangeFrom));
            Assert.That(rows[^2], Does.Contain("order/000199"));
            Assert.That(rows[^1], Does.EndWith(",trailer").And.Contain("\"\"complete\"\":true"));
        });
        await SnapAsync(dana, "4", "range-done", "The range's hundred orders are in the CSV, and its trailer says it is complete");
        await dialog.DoneAsync();
        Coverage.Hit("command:data.export");
    }

    // U4.5: no New entry, Edit, Delete, Import or view and index administration anywhere in Data.
    private static async Task NoWriteAffordanceInDataAsync(PersonaSession dana)
    {
        var tree = new TreeWorkspacePage(dana.Shell);
        var content = dana.Shell.Content;
        await tree.OpenFromDirectoryAsync(Names.Trees.FactoryOrders);
        await tree.OpenEntryAsync(Names.Orders.Order17, Names.Orders.Order17);
        await Expect(tree.Export).ToBeVisibleAsync();
        await Expect(tree.Import).ToHaveCountAsync(0);
        await Expect(tree.NewEntry).ToHaveCountAsync(0);
        await Expect(tree.EditEntry).ToHaveCountAsync(0);
        await Expect(tree.DeleteEntry).ToHaveCountAsync(0);
        await SnapAsync(dana, "5", "keys-read-only", "Keys and an entry: Export only - no Import, New entry, Edit or Delete");

        await tree.ChooseTabAsync("Views", "views");
        await Expect(content.Locator("[data-lt-command=\"data.create-view\"]")).ToHaveCountAsync(0);
        await Expect(content.Locator("[data-lt-view-drop]")).ToHaveCountAsync(0);
        await Expect(content.GetByRole(Microsoft.Playwright.AriaRole.Button, new() { NameRegex = new Regex("^(Rebuild|Reconcile)") })).ToHaveCountAsync(0);
        await SnapAsync(dana, "5", "views-read-only", "Views: no New view, Rebuild, Reconcile or Drop view");

        await tree.ChooseTabAsync("Tag indexes", "tag-indexes");
        await Expect(content.GetByRole(Microsoft.Playwright.AriaRole.Button, new() { NameRegex = new Regex("^Reconcile") })).ToHaveCountAsync(0);
        await SnapAsync(dana, "5", "tag-indexes-read-only", "Tag indexes: no Reconcile");

        await dana.Shell.GoToAreaAsync("data");
        await Expect(dana.Shell.Content.GetByRole(Microsoft.Playwright.AriaRole.Link, new() { Name = Names.Trees.FactoryOrders, Exact = true })).ToBeVisibleAsync();
        await Expect(content.Locator("[data-lt-data-new-from-file]")).ToHaveCountAsync(0);
        await SnapAsync(dana, "5", "directory-read-only", "The Data directory: the trees she reads, and no New tree from file");
    }
}
