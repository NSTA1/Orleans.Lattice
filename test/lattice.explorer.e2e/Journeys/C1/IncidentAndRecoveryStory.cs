using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>The nine steps of C1 (#5389), in order, on one run's estate.</summary>
/// <param name="context">The chapter's context.</param>
internal sealed class IncidentAndRecoveryStory(ChapterContext context)
{
    /// <summary>How many wrong prices the faulty deploy writes.</summary>
    public const int Burst = 500;

    private const string Orders = Names.Trees.FactoryOrders;
    private const string Archive = Names.Trees.FactoryOrdersArchive;
    private const uint TargetVersion = 2;

    private readonly C1Steps _steps = new();
    private PersonaSession? _olivia;
    private WrongPrice? _affected;
    private string? _restorePath;

    private JourneyEstate Estate => context.Estate;

    private PersonaSession Olivia => _olivia ?? throw new InvalidOperationException("olivia has not signed in yet.");

    private WrongPrice Affected => _affected ?? throw new InvalidOperationException("No wrong price has been written yet.");

    /// <summary>Runs the journey.</summary>
    public async Task RunAsync()
    {
        try
        {
            await _steps.StepAsync(1, "priya deletes the archive tree", DeleteArchiveAsync);
            _olivia = await Estate.SignInAsAsync(Personas.Olivia);
            await _steps.StepAsync(2, "olivia recovers the archive", RecoverArchiveAsync);
            await _steps.StepAsync(3, "a faulty upstream deploy writes 500 wrong prices", InjectWrongPricesAsync);
            await _steps.StepAsync(4, "olivia sees the incident", SeeIncidentAsync);
            await _steps.StepAsync(5, "olivia restores orders-baseline over factory-orders", RestoreBaselineAsync);
            await _steps.StepAsync(6, "priya misses her corrections; olivia reverts the restore", RevertAsync);
            await _steps.StepAsync(7, "olivia restores again; priya re-imports her corrections", RestoreAgainAndReimportAsync);
            await _steps.StepAsync(8, "sam re-runs the migration to version 2", MigrateAsync);
            await _steps.StepAsync(9, "the east ledger replays three captured orders into the dead letters", DeadLettersAsync);
        }
        finally
        {
            if (_olivia is not null)
            {
                await _olivia.DisposeAsync();
            }
        }
    }

    private async Task DeleteArchiveAsync()
    {
        // priya holds tree lifecycle on the archive but no cluster Telemetry: the Cluster
        // area admits her on that grant (#5581).
        await using var priya = await Estate.SignInAsAsync(Personas.Priya);
        var cluster = new ClusterLifecyclePage(priya);
        await cluster.OpenLifecycleAsync(Archive);
        await Expect(cluster.State).ToHaveTextAsync("Live");
        await _steps.CaptureAsync(priya, "lifecycle", "priya opens the archive's Lifecycle tab, taking it for a scratch copy: it is live");

        var confirmation = await cluster.RequestDeleteAsync();
        await ClusterLifecyclePage.TypeNameAsync(confirmation, Archive, "Delete");
        await _steps.CaptureAsync(priya, "confirm", "Delete tree, confirmed by typing the archive's name");

        await cluster.ConfirmAsync(confirmation, "Delete", ClusterLifecyclePage.DeletedToast, "Deleted, recoverable");
        await _steps.CaptureAsync(priya, "deleted", "The archive is soft-deleted: every read and write on it fails, and it is recoverable");
    }

    private async Task RecoverArchiveAsync()
    {
        var cluster = new ClusterLifecyclePage(Olivia);
        var row = await cluster.OpenTreeListAsync(Archive);
        await Expect(row).ToContainTextAsync("Deleted");
        await _steps.CaptureAsync(Olivia, "tree-list", "olivia's tree list shows the archive deleted");

        await cluster.OpenLifecycleFromRowAsync(row, Archive);
        await Expect(cluster.State).ToHaveTextAsync("Deleted, recoverable");
        await _steps.CaptureAsync(Olivia, "lifecycle", "Its Lifecycle tab offers Recover tree until the soft-delete window closes");

        var confirmation = await cluster.RequestRecoverAsync();
        await ClusterLifecyclePage.TypeNameAsync(confirmation, Archive, "Recover");
        await _steps.CaptureAsync(Olivia, "confirm", "Recover tree, confirmed by typing the archive's name");

        await cluster.ConfirmAsync(confirmation, "Recover", ClusterLifecyclePage.RecoveredToast, "Live");
        await _steps.CaptureAsync(Olivia, "recovered", "The archive is live again");

        var data = new DataTreePage(Olivia);
        await data.ExpectLiveKeysAsync(Archive, Names.Files.ArchiveRows);
        await Expect(data.KeyRow(Names.Orders.Key(StoryData.ArchiveFirstOrder))).ToBeVisibleAsync();
        await _steps.CaptureAsync(Olivia, "data", "Data shows the archive's 5,000 entries back");
        Coverage.Hit("use-case:cluster/delete-and-recover");
    }

    private async Task InjectWrongPricesAsync()
    {
        // Off-UI, the external actor: the line feed's faulty deploy (#5389 step 3).
        var feed = Estate.LineFeed;
        var before = (await feed.StatusAsync()).WrongPricesWritten;
        await feed.InjectWrongPriceBurstAsync(Burst);
        await feed.WaitUntilAsync(status => status.WrongPricesWritten >= before + Burst, TimeSpan.FromMinutes(5));

        var wrong = await WrongPricesAsync(ContractFacades.Of(context).Data);
        Assert.That(wrong, Has.Count.GreaterThanOrEqualTo(Burst), "the burst's wrong prices are stored in factory-orders");
        _affected = wrong[0];

        // The wrong prices pass the policy, so nothing is dead-lettered: that is the incident.
        var data = new DataTreePage(Olivia);
        await data.OpenKeysAsync(Orders, Affected.Key);
        await data.ExpectKeyWithTotalAsync(Affected.Key, Affected.Total);
        await _steps.CaptureAsync(Olivia, "stored", $"The burst is stored: {Affected.Key} carries a total a hundred times too large");
        await data.ExpectNoDeadLettersAsync(Orders);
        await _steps.CaptureAsync(Olivia, "no-dead-letters", "The wrong prices pass the strict policy, so nothing was diverted to the dead letters");
    }

    private async Task SeeIncidentAsync()
    {
        var board = new TelemetryBoardPage(Olivia);
        var (median, peak) = await board.ExpectSpikeAsync(Orders, TimeSpan.FromMinutes(4));
        TestContext.Progress.WriteLine($"C1.4: factory-orders write operations, median {median}, peak {peak}.");
        await _steps.CaptureAsync(Olivia, "writes-board", "The Throughput board's write operations for factory-orders spike");

        var revisions = await new DataTreePage(Olivia).OpenHistoryAsync(Orders, Affected.Key);
        await new KnownDefect("#5583", "Data History renders a schema-versioned revision as a hex dump of its envelope").ReproducesAsync(async () =>
        {
            await Expect(revisions).ToContainTextAsync(new Regex(@"00000000\s+fe 01 "));
            await _steps.CaptureAsync(Olivia, "history", $"History of {Affected.Key} shows the wrong price's revision, as a hex dump of its version envelope (#5583)");
        });
    }

    private async Task RestoreBaselineAsync()
    {
        await RestoreBaselineFromUiAsync("first");

        var west = new DataTreePage(Olivia);
        await west.ExpectKeyGoneAsync(Orders, Affected.Key);
        await _steps.CaptureAsync(Olivia, "west", $"In west, {Affected.Key} - written after the baseline - is gone");

        await using var east = await Estate.SignInAsAsync(Personas.Olivia, region: JourneyEstate.East);
        await new DataTreePage(east).ExpectKeyGoneAsync(Orders, Affected.Key, TimeSpan.FromMinutes(5));
        await _steps.CaptureAsync(east, "east", $"Once replication converges, {Affected.Key} is gone in east too");
        Coverage.Hit("use-case:backups/restore-replicates");
    }

    private async Task RevertAsync()
    {
        var number = StoryData.FirstCorrectedOrder;
        var key = Names.Orders.Key(number);
        await using (var priya = await Estate.SignInAsAsync(Personas.Priya))
        {
            var data = new DataTreePage(priya);
            await data.OpenKeysAsync(Orders, key);
            await data.ExpectKeyWithTotalAsync(key, Json(StoryData.SeededTotal(number)));
            await _steps.CaptureAsync(priya, "corrections-missing", $"priya finds {key} back at its total from before her correction");
        }

        var restore = new BackupsRestorePage(Olivia);
        await Olivia.Shell.GotoAsync(_restorePath ?? throw new InvalidOperationException("The restore's status page is not known."));
        await restore.ExpectSucceededAsync();
        await _steps.CaptureAsync(Olivia, "status-page", "olivia returns to the restore's status page, which offers its revert");

        var confirmation = await restore.RequestRevertAsync();
        await BackupsRestorePage.TypeTreeAsync(confirmation, Orders, "Revert");
        await _steps.CaptureAsync(Olivia, "confirm", "Revert this restore?, confirmed by typing the tree's name");

        await restore.ConfirmRevertAsync(confirmation, Orders);
        await _steps.CaptureAsync(Olivia, "reverted", "The revert swaps factory-orders back to the copy it held before the restore");

        var west = new DataTreePage(Olivia);
        await west.OpenKeysAsync(Orders, Affected.Key);
        await west.ExpectKeyWithTotalAsync(Affected.Key, Affected.Total);
        await _steps.CaptureAsync(Olivia, "pre-restore", $"Data shows the pre-restore state: {Affected.Key} and its wrong total are back");
        Coverage.Hit("use-case:backups/restore-destructive-and-revert");
    }

    private async Task RestoreAgainAndReimportAsync()
    {
        await RestoreBaselineFromUiAsync("again");
        await new DataTreePage(Olivia).ExpectKeyGoneAsync(Orders, Affected.Key);
        await _steps.CaptureAsync(Olivia, "restored", $"factory-orders is the baseline again: {Affected.Key} is gone");

        await using var priya = await Estate.SignInAsAsync(Personas.Priya);
        var data = new DataTreePage(priya);
        await data.OpenKeysAsync(Orders);
        var dialog = await data.OpenImportAsync(Orders);
        await DataTreePage.ChooseNdjsonUpsertAsync(dialog, context.Recall(StoryKeys.CorrectionsFile), "order/{/id}");
        await _steps.CaptureAsync(priya, "import-form", "priya re-imports corrections.ndjson, adding or replacing entries keyed order/{/id}");

        var (written, refused) = await DataTreePage.ImportAsync(dialog);
        Assert.That(written, Is.EqualTo(Names.Files.CorrectionsApplied), "the corrections that apply are written");
        Assert.That(refused, Is.EqualTo(Names.Files.CorrectionsRefused), "the corrections the schema refuses are refused");
        await _steps.CaptureAsync(priya, "import-result", "48 corrections written, 2 refused by the schema, as on her first day");
        await DataTreePage.DoneAsync(dialog);

        var number = StoryData.FirstCorrectedOrder;
        var key = Names.Orders.Key(number);
        await data.OpenKeysAsync(Orders, key);
        await data.ExpectKeyWithTotalAsync(key, Json(StoryData.CorrectedTotal(number)));
        await _steps.CaptureAsync(priya, "corrected", $"{key} carries its correction again");
    }

    private async Task MigrateAsync()
    {
        await using var sam = await Estate.SignInAsAsync(Personas.Sam);
        var schema = new SchemaTreePage(sam);
        await schema.OpenAsync(Orders, "versions", "Versions");
        await _steps.CaptureAsync(sam, "versions", "sam opens factory-orders' Versions: the target is still version 2");

        var dialog = await schema.RequestMigrationAsync(TargetVersion);
        await _steps.CaptureAsync(sam, "confirm", "Migrate stored values rewrites every restored value at version 2");

        await schema.StartMigrationAsync(dialog, TargetVersion);
        await schema.ExpectMigrationFinishedAsync();
        await _steps.CaptureAsync(sam, "migrated", "The migration finished, and factory-orders serves its values at version 2");
    }

    private async Task DeadLettersAsync()
    {
        // Off-UI, the external actor: the east ledger replays three orders as the baseline captured them.
        await C1Contract.ReplayCapturedOrdersFromEastAsync(Estate);

        await using (var sam = await Estate.SignInAsAsync(Personas.Sam))
        {
            var schema = new SchemaTreePage(sam);
            await schema.OpenAsync(Orders, "dead-letters", "Dead letters");
            await _steps.CaptureAsync(sam, "count", "sam's Schema Dead letters counts the diverted writes");

            var table = await schema.LoadDeadLettersAsync(Orders);
            foreach (var key in Names.Orders.NegativeTotals)
            {
                await Expect(table).ToContainTextAsync(key);
            }

            // Load more dead letters is drawn only while more than a page (100) remain.
            await Expect(schema.LoadMore).ToHaveCountAsync(0);
            await _steps.CaptureAsync(sam, "loaded", "Load dead letters lists the three replayed orders, diverted by strict ingest");
        }

        await using var priya = await Estate.SignInAsAsync(Personas.Priya);
        var letters = await new DataTreePage(priya).OpenDeadLettersAsync(Orders);
        foreach (var key in Names.Orders.NegativeTotals)
        {
            await Expect(letters).ToContainTextAsync(key);
        }

        await _steps.CaptureAsync(priya, "data", "priya sees the same three entries in Data Dead letters");
    }

    private async Task RestoreBaselineFromUiAsync(string pass)
    {
        var restore = new BackupsRestorePage(Olivia);
        await restore.OpenBackupAsync(Names.Backups.OrdersBaseline);
        await _steps.CaptureAsync(Olivia, "backup", "olivia opens orders-baseline in Backups");

        await restore.ChoosePointInTimeAsync(Orders);
        await _steps.CaptureAsync(Olivia, "restore-form", "Restore into factory-orders with Point-in-time replace (destructive)");

        var confirmation = await restore.RequestRestoreAsync();
        await BackupsRestorePage.TypeTreeAsync(confirmation, Orders, "Restore");
        await _steps.CaptureAsync(Olivia, "confirm", "Restore this backup?, confirmed by typing the tree's name");

        await restore.ConfirmRestoreAsync(confirmation);
        await restore.ExpectSucceededAsync();
        TestContext.Progress.WriteLine($"C1: the {pass} orders-baseline restore succeeded at {Olivia.Page.Url}.");
        _restorePath ??= new Uri(Olivia.Page.Url).PathAndQuery.TrimStart('/');
        await _steps.CaptureAsync(Olivia, "succeeded", "The restore's status page: the coordinated restore succeeded");
    }

    private static async Task<IReadOnlyList<WrongPrice>> WrongPricesAsync(Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcClient data)
    {
        var wrong = new List<WrongPrice>();
        foreach (var entry in await ContractCalls.RangeAsync(data, Orders, "order/", ContractCalls.PrefixEnd("order/")))
        {
            try
            {
                using var document = JsonDocument.Parse(entry.Value);
                if (document.RootElement.TryGetProperty("total", out var total)
                    && total.TryGetDecimal(out var parsed)
                    && parsed >= C1Contract.WrongPriceFloor)
                {
                    wrong.Add(new WrongPrice(entry.Key, total.GetRawText()));
                }
            }
            catch (JsonException)
            {
                // Not an order the line feed or the seeders wrote.
            }
        }

        return wrong;
    }

    private static string Json(decimal value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>An order the faulty deploy wrote, and its total as JSON stores it.</summary>
    /// <param name="Key">The order's key.</param>
    /// <param name="Total">The stored total.</param>
    private sealed record WrongPrice(string Key, string Total);
}
