using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>Steps 8 to 11: a tree seeded from a CSV, an import cancelled and its tree deleted, the corrections, and an export.</summary>
public sealed partial class DataStewardJourney
{
    private static readonly KnownDefect StarvedSeed = new KnownDefect("#5569", "under replay-permit starvation the seed's bulk appends to the new archive tree's shards time out at 30 s, so the Import dialog says it could not seed the tree");

    private async Task SeedTheArchiveFromACsvAsync()
    {
        var archive = Names.Trees.FactoryOrdersArchive;
        var file = StoryData.WriteFile(_files, Names.Files.ArchiveCsv);
        await CreateTreeFromFileAsync(archive, "8");

        var import = new DataImportDialog(_priya.Page, archive);
        await import.WaitOpenAsync();
        await Expect(import.Mode).ToHaveValueAsync("Seed");
        await import.ChooseFileAsync(file);
        await Expect(import.Format).ToHaveValueAsync("Csv");
        await import.Key.FillAsync("order/{id}");
        await Expect(import.ValueSource).ToHaveValueAsync("Record");
        await Expect(import.Dialog.GetByLabel("Infer numbers and booleans")).Not.ToBeCheckedAsync();
        await CheckpointAsync("8", "seed-dialog", "archive-2026-10.csv: seed mode, key order/{id}, each row stored as a JSON object of its columns");

        await import.Start(seed: true).ClickAsync();
        await Expect(import.State).ToHaveAttributeAsync("data-lt-import-state", new Regex("^(succeeded|failed)$"), new() { Timeout = 300_000 });
        if (await import.State.GetAttributeAsync("data-lt-import-state") == "failed")
        {
            await StandInForAStarvedSeedAsync(import, archive);
            return;
        }

        Assert.That(await import.CommittedAsync(), Is.EqualTo(Names.Files.ArchiveRows), "every row of the archive was written");
        await Expect(import.Result).ToContainTextAsync($"Seeded {Names.Files.ArchiveRows.ToString("N0", CultureInfo.InvariantCulture)} entries.");
        await CheckpointAsync("8", "seeded", "The archive is seeded: 5,000 entries");

        await import.Done.ClickAsync();
        await Expect(import.Dialog).ToHaveCountAsync(0);
        await Expect(_tree.KeyLinks.First).ToHaveTextAsync(StoryData.Archive[0].Key, new() { Timeout = PanelSettled.BoundMs });
        Coverage.Hit("use-case:data/import-csv-new-tree");
        await CheckpointAsync("8", "archive-keys", "The new tree holds the October orders");
    }

    /// <summary>
    /// The seed failed. Under replay-permit starvation (#5569) the seed's bulk appends to the new
    /// tree's shards time out at 30 s, and the dialog says it could not seed the tree; that is
    /// recorded as a known defect, and the archive later chapters read is written as priya through
    /// the Data API instead, as U1's contract does. Any other failure fails the step.
    /// </summary>
    private async Task StandInForAStarvedSeedAsync(DataImportDialog import, string archive)
    {
        var failure = (await import.Dialog.InnerTextAsync()).ReplaceLineEndings(" ");
        if (!failure.Contains("could not seed this tree", StringComparison.Ordinal))
        {
            Assert.Fail($"The archive's seed failed: {failure}");
        }

        await StarvedSeed.ReproducesAsync(async () =>
            await Expect(import.Dialog).ToContainTextAsync("could not seed this tree"));
        await CheckpointAsync("8", "seed-failed", "Under replay-permit starvation the seed's writes time out, and the dialog says the tree could not be seeded (#5569)");

        await import.Done.ClickAsync();
        await Expect(import.Dialog).ToHaveCountAsync(0);
        var data = ContractFacades.Of(_context, Personas.Priya.Id).Data;
        await WriteArchiveWhileAdvancingAsync(data, archive);
        Assert.That(await ContractCalls.CountPrefixAsync(data, archive, "order/"), Is.EqualTo(Names.Files.ArchiveRows), "the stand-in wrote every row of the archive");

        await _priya.Page.ReloadAsync();
        await Expect(_tree.KeyLinks.First).ToHaveTextAsync(StoryData.Archive[0].Key, new() { Timeout = 120_000 });
        await CheckpointAsync("8", "archive-keys", "The new tree holds the October orders, written through the Data API in place of the starved seed");
    }

    /// <summary>
    /// Writes the archive through the Data API in batches, judged by whether the writing advances,
    /// not by one fixed window: a transient failure (#5743 maps a grain timeout to DeadlineExceeded
    /// or Unavailable) retries the batch, and the wait fails only when no batch lands for three
    /// minutes, or the whole write runs past fifteen.
    /// </summary>
    private static async Task WriteArchiveWhileAdvancingAsync(Api.Data.Grpc.LatticeDataApiGrpcClient data, string archive)
    {
        var batches = StoryData.Archive.Chunk(100).ToArray();
        var written = 0;
        await ContractWait.UntilAdvancingAsync(
            $"writing the archive to {archive}",
            async () =>
            {
                var batch = batches[written];
                await data.SetManyAsync(new Api.Data.Grpc.DataSetManyRequest
                {
                    TreeId = archive,
                    Upserts = [.. batch.Select(entry => new Api.Data.DataEntry { Key = entry.Key, Value = entry.Value })],
                });
                written++;
                return written == batches.Length;
            },
            () => written,
            stall: TimeSpan.FromMinutes(3),
            ceiling: TimeSpan.FromMinutes(15),
            () => $"{written} of {batches.Length} batches of 100 written");
    }

    private async Task CancelAnOversizedImportAndDeleteItsTreeAsync()
    {
        var history = Names.Trees.FactoryHistory;
        var file = StoryData.WriteHistoryFile(_files);
        await CreateTreeFromFileAsync(history, "9");

        var import = new DataImportDialog(_priya.Page, history);
        await import.WaitOpenAsync();
        await import.ChooseFileAsync(file);
        await Expect(import.Format).ToHaveValueAsync("Ndjson");
        await import.Key.FillAsync("order/{/id}");
        await import.Start(seed: true).ClickAsync();
        await Expect(import.Stop).ToBeVisibleAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));
        await CheckpointAsync("9", "importing", "history-2025.ndjson, 500,000 lines, streams in");

        await import.Stop.ClickAsync();
        await import.ExpectEndAsync("stopped", PanelSettled.BoundMs);
        var committed = await import.CommittedAsync();
        var written = committed.ToString("N0", CultureInfo.InvariantCulture) + (committed == 1 ? " entry" : " entries");
        await Expect(import.Result).ToContainTextAsync($"Stopped. {written} were written before the stop, and nothing after it.");
        Assert.That(committed, Is.LessThan(Names.Files.History2025Lines), "stopped within seconds, the import did not finish");
        await CheckpointAsync("9", "stopped", $"Cancelled within seconds: the import reports exactly what it committed ({written})");

        await import.Done.ClickAsync();
        await Expect(import.Dialog).ToHaveCountAsync(0);
        if (committed == 0)
        {
            await Expect(_tree.EmptyKeys).ToContainTextAsync("This tree holds no keys yet.", new() { Timeout = PanelSettled.BoundMs });
        }
        else
        {
            // What was committed is the file's head.
            await Expect(_tree.KeyLinks.First).ToHaveTextAsync(Names.Orders.Key(1), new() { Timeout = PanelSettled.BoundMs });
        }

        Coverage.Hit("use-case:data/import-cancel");

        await _tree.DeleteTree.ClickAsync();
        var confirm = await _tree.TypeConfirmationAsync("Delete this tree?", history);
        await CheckpointAsync("9", "delete-confirmation", "Delete tree... asks her to type the tree's name");
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Delete tree", Exact = true }).ClickAsync();
        await ExpectToastAsync($"Tree {history} deleted. It can be recovered until its window closes.");
        await Expect(_tree.Deleted).ToHaveAttributeAsync("data-lt-tree-deleted", "recoverable", new() { Timeout = PanelSettled.BoundMs });
        Coverage.Hit("use-case:data/tree-delete-recover");
        await CheckpointAsync("9", "deleted", "factory-history is deleted, and recoverable until its window closes");
    }

    private async Task ApplyTheCorrectionsAsync()
    {
        var orders = Names.Trees.FactoryOrders;
        var file = StoryData.WriteFile(_files, Names.Files.CorrectionsNdjson);
        await _priya.Shell.GotoAsync("/data/" + orders);
        await Expect(_tree.Heading).ToHaveTextAsync(orders);
        await _tree.WaitForKeysAsync();

        await _tree.Import.ClickAsync();
        var import = new DataImportDialog(_priya.Page, orders);
        await import.WaitOpenAsync();
        await Expect(import.Mode).ToHaveValueAsync("Upsert");
        await import.ChooseFileAsync(file);
        await Expect(import.Format).ToHaveValueAsync("Ndjson");
        await import.Key.FillAsync("order/{/id}");
        await Expect(import.ValueSource).ToHaveValueAsync("Record");
        Coverage.Hit("command:data.import");
        await CheckpointAsync("10", "upsert-dialog", "corrections.ndjson: 50 lines, upserted into factory-orders");

        await import.Start(seed: false).ClickAsync();
        await import.ExpectEndAsync("succeeded", 300_000);
        Assert.That(await import.CommittedAsync(), Is.EqualTo(Names.Files.CorrectionsApplied));
        await Expect(import.Refused).ToHaveAttributeAsync("data-lt-import-refused", Names.Files.CorrectionsRefused.ToString(CultureInfo.InvariantCulture));
        await Expect(import.RefusedRows).ToHaveCountAsync(Names.Files.CorrectionsRefused);
        await Expect(import.RefusedRows.Filter(new() { HasText = Names.Orders.Key(StoryData.RefusedNegativeOrder) })).ToHaveCountAsync(1);
        await Expect(import.RefusedRows.Filter(new() { HasText = Names.Orders.Key(StoryData.RefusedStatusOrder) })).ToHaveCountAsync(1);
        await CheckpointAsync("10", "upsert-result", "48 corrections apply; the schema refuses 2, each listed with its line");

        await import.PrepareRefused.ClickAsync();
        await Expect(import.DownloadRefused).ToBeVisibleAsync();
        var download = await _priya.Page.RunAndWaitForDownloadAsync(() => import.DownloadRefused.ClickAsync());
        var saved = Path.Combine(_files, "corrections-refused-" + download.SuggestedFilename);
        await download.SaveAsAsync(saved);
        var text = await File.ReadAllTextAsync(saved);
        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("lattice-import-rejections/1"));
            Assert.That(text, Does.Contain(Names.Orders.Key(StoryData.RefusedNegativeOrder)));
            Assert.That(text, Does.Contain(Names.Orders.Key(StoryData.RefusedStatusOrder)));
        });
        Coverage.Hit("use-case:data/import-upsert-rejections");
        await CheckpointAsync("10", "rejections-downloaded", "The full list of refused lines downloads");

        // C1 replays the same file.
        _context.Record(StoryKeys.CorrectionsFile, file);
        await import.Done.ClickAsync();
        await Expect(import.Dialog).ToHaveCountAsync(0);
    }

    private async Task ExportEvidenceAsync()
    {
        var orders = Names.Trees.FactoryOrders;
        var prefix = Names.Orders.ExportPrefix;
        await _tree.Export.ClickAsync();
        var export = new DataExportDialog(_priya.Page, orders);
        await export.WaitOpenAsync();
        await export.Scope.SelectOptionAsync("Prefix");
        await export.Prefix.FillAsync(prefix);
        await export.Format.SelectOptionAsync("Csv");
        await CheckpointAsync("11", "export-dialog", $"Export...: prefix {prefix}, as CSV");

        await export.Prepare.ClickAsync();
        await Expect(export.Download).ToBeVisibleAsync(new() { Timeout = PanelSettled.BoundMs });
        await CheckpointAsync("11", "download-ready", "The download is ready");

        var download = await _priya.Page.RunAndWaitForDownloadAsync(() => export.Download.ClickAsync());
        var saved = Path.Combine(_files, download.SuggestedFilename);
        await download.SaveAsAsync(saved);
        await Expect(export.Result).ToContainTextAsync("Every key in the scope is in the file.", new() { Timeout = PanelSettled.BoundMs });
        await CheckpointAsync("11", "export-complete", "The export streams to the end: every key in the scope is in the file");

        // She opens the file: a header, the hundred orders under the prefix, and the trailer.
        var rows = (await File.ReadAllTextAsync(saved)).Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        var corrected = Names.Orders.Key(StoryData.FirstCorrectedOrder);
        Assert.Multiple(() =>
        {
            Assert.That(download.SuggestedFilename, Does.EndWith(".csv"));
            Assert.That(rows[0], Is.EqualTo("key,value,encoding"));
            Assert.That(rows[1..^1], Has.Length.EqualTo(100), "order/000100 to order/000199");
            Assert.That(rows[1..^1], Has.All.Matches<string>(row => Regex.IsMatch(row, "^\"?" + Regex.Escape(prefix))));
            Assert.That(rows.Single(row => row.Contains(corrected, StringComparison.Ordinal)),
                Does.Contain(StoryData.CorrectedTotal(StoryData.FirstCorrectedOrder).ToString(CultureInfo.InvariantCulture)), "the export carries her corrections");
            Assert.That(rows[^1], Does.EndWith(",trailer"));
            Assert.That(rows[^1], Does.Contain("\"\"complete\"\":true"));
        });
        Coverage.Hit("command:data.export");
        Coverage.Hit("use-case:data/export-streamed");

        await export.Done.ClickAsync();
        await Expect(export.Dialog).ToHaveCountAsync(0);
    }

    /// <summary>Creates <paramref name="tree"/> with the Data directory's New tree from file..., which opens its Import dialog on a seed.</summary>
    private async Task CreateTreeFromFileAsync(string tree, string step)
    {
        await _directory.OpenAsync();
        await Expect(_directory.NewTreeFromFile).ToHaveTextAsync("New tree from file...");
        await _directory.NewTreeFromFile.ClickAsync();
        var create = _priya.Page.GetByRole(AriaRole.Dialog, new() { Name = "New tree from file" });
        await Expect(create).ToBeVisibleAsync();
        await create.GetByLabel("Tree name", new() { Exact = true }).FillAsync(tree);
        await create.GetByRole(AriaRole.Button, new() { Name = "Review", Exact = true }).ClickAsync();
        await Expect(create.GetByRole(AriaRole.Button, new() { Name = "Create tree", Exact = true })).ToBeVisibleAsync(new() { Timeout = PanelSettled.BoundMs });
        await CheckpointAsync(step, "create-review", $"New tree from file...: {tree}, reviewed before it is created");

        await create.GetByRole(AriaRole.Button, new() { Name = "Create tree", Exact = true }).ClickAsync();
        await Expect(_tree.Heading).ToHaveTextAsync(tree, new() { Timeout = PanelSettled.BoundMs });
        await Expect(_priya.Page).ToHaveURLAsync(new Regex("[?&]import=seed(&|$)"));
    }
}
