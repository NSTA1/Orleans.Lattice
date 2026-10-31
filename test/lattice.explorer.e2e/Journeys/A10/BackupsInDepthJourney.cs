using System.Globalization;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A10;

/// <summary>
/// A10, backups in depth (#5378). Before the auditors ask, olivia proves every recovery path
/// through the Backups area: she pages the catalogue's history, captures a prefix and a single
/// key, an incremental chain and a cross-tree set at one fence, and restores in every mode -
/// repair in place, point-in-time replace into a new tree at the chain's restore point, and a
/// cold restore from the backup store alone - then reopens a finished operation by its
/// address and adds an incremental schedule. Its F6 contract is
/// <see cref="Contracts.Contoso.A10Contract"/>.
/// </summary>
/// <remarks>
/// The one off-UI action is the consume the issue names: the 60 historical telemetry backups
/// (<see cref="ContosoHarness.SeedHistoricalBackupsAsync"/>), which an hourly schedule would
/// have left over the previous days.
/// </remarks>
[Journey(JourneyId)]
[JourneyChapter("A10", After = new[] { "A6" })]
public sealed class BackupsInDepthJourney : JourneyChapter
{
    /// <summary>The journey id its screenshots are filed under.</summary>
    public const string JourneyId = "a10";

    private const string RepairMode = "Repair missing items (non-destructive)";
    private const string PointInTimeMode = "Point-in-time replace (destructive)";

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        await ContosoHarness.SeedHistoricalBackupsAsync(context.Estate);

        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        var run = new Run(context, olivia);
        await run.CatalogueAsync();
        await run.ScopedCapturesAsync();
        await run.IncrementalChainAsync();
        await run.SetAsync();
        await run.RepairAsync();
        await run.PointInTimeAsync();
        await run.ColdRestoreAsync();
        await run.ReopenOperationAsync();
        await run.IncrementalScheduleAsync();
    }

    /// <summary>One run of the journey: olivia's session and what each step hands the next.</summary>
    private sealed class Run(ChapterContext context, PersonaSession olivia)
    {
        private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
        private readonly BackupsArea _backups = new(olivia.Shell);
        private readonly DataEntry _entry = new(olivia.Shell);
        private string _prefixOperation = string.Empty;
        private string _prefixBackup = string.Empty;
        private string _linesFull = string.Empty;

        private static string FactoryOrders => Names.Trees.FactoryOrders;

        private static string FactoryLines => Names.Trees.FactoryLines;

        /// <summary>A10.1: the catalogue's 60 historical backups, filtered by tree and kind, paged forward and back.</summary>
        public Task CatalogueAsync() => StepAsync(1, async () =>
        {
            await _backups.OpenCatalogueAsync();
            await ShotAsync(1, "catalogue", "The catalogue lists every backup olivia may read, newest first");

            await _backups.FilterByTreeAsync(Names.Trees.FactoryTelemetry);
            await _backups.FilterByKindAsync("Full", "full");
            await Expect(_backups.CatalogueRows).ToHaveCountAsync(BackupsArea.CataloguePageSize);
            await Expect(_backups.CatalogueRows.Filter(new() { HasNotText = Names.Trees.FactoryTelemetry })).ToHaveCountAsync(0);
            await Expect(_backups.CatalogueRows.Filter(new() { HasText = "Incremental" })).ToHaveCountAsync(0);
            await Expect(_backups.PagerPosition).ToHaveTextAsync("Page 1");
            await Expect(_backups.PreviousPage).ToBeDisabledAsync();
            await Expect(_backups.NextPage).ToBeEnabledAsync();
            Coverage.Hit("query:backups.tree");
            Coverage.Hit("query:backups.kind");
            await ShotAsync(1, "filtered", "Filtered to factory-telemetry's full backups: the first 25 of its history");

            await _backups.NextPageAsync(2);
            await Expect(_backups.CatalogueRows).ToHaveCountAsync(BackupsArea.CataloguePageSize);
            await ShotAsync(1, "page-2", "Next page: the second 25");

            // 60 historical backups and A6's scheduled captures: a third page, part full.
            await _backups.NextPageAsync(3);
            var lastPage = await _backups.CatalogueRows.CountAsync();
            Assert.That(lastPage, Is.InRange(Names.Backups.HistoricalCount - (2 * BackupsArea.CataloguePageSize), BackupsArea.CataloguePageSize), "the third page holds the rest of the history");
            await Expect(_backups.CatalogueRows.Filter(new() { HasNotText = Names.Trees.FactoryTelemetry })).ToHaveCountAsync(0);
            await ShotAsync(1, "page-3", "Next page again: the oldest of the history");

            await _backups.PreviousPageAsync(2);
            await Expect(_backups.CatalogueRows).ToHaveCountAsync(BackupsArea.CataloguePageSize);
            await Expect(_backups.PreviousPage).ToBeEnabledAsync();
            Coverage.Hit("use-case:backups/catalogue-filters-and-paging");
            Coverage.Hit("use-case:backups/limits");
            await ShotAsync(1, "previous-page", "Previous page returns to page 2, the filters kept");
        });

        /// <summary>A10.2: a full capture of the orders under a prefix, and of one key.</summary>
        public Task ScopedCapturesAsync() => StepAsync(2, async () =>
        {
            await _backups.OpenCaptureFormAsync();
            await _backups.NameCaptureAsync(Names.Backups.OrdersPrefix0001);
            await _backups.ChooseCaptureTreeAsync(FactoryOrders);
            await _backups.ScopeToPrefixAsync(Names.Orders.BackupPrefix);
            await ShotAsync(2, "prefix-form", "A full capture of factory-orders, narrowed to the keys under order/00001");

            _prefixOperation = await _backups.SubmitCaptureAsync();
            _prefixBackup = await CapturedAsync(2, "prefix", "Capture a full backup of " + FactoryOrders, Names.Backups.OrdersPrefix0001);
            await Expect(_backups.Fact("Scope")).ToHaveTextAsync("Keys under prefix " + Names.Orders.BackupPrefix);
            await ShotAsync(2, "prefix-details", "orders-prefix-0001 is a full backup of the keys under order/00001");

            await _backups.OpenCaptureFormAsync();
            await _backups.NameCaptureAsync(Names.Backups.OrdersKey42);
            await _backups.ChooseCaptureTreeAsync(FactoryOrders);
            await _backups.ScopeToKeyAsync(Names.Orders.Order42);
            await ShotAsync(2, "key-form", "A full capture of the one key order/000042");

            await _backups.SubmitCaptureAsync();
            await CapturedAsync(2, "key", "Capture a full backup of " + FactoryOrders, Names.Backups.OrdersKey42);
            await Expect(_backups.Fact("Scope")).ToHaveTextAsync("One key: " + Names.Orders.Order42);
            await ShotAsync(2, "key-details", "orders-key-42 is a full backup of one key");
        });

        /// <summary>A10.3: <c>lines-full</c>, two edits through the entry editor, and the incremental <c>lines-inc-1</c> on it.</summary>
        public Task IncrementalChainAsync() => StepAsync(3, async () =>
        {
            await _backups.OpenCaptureFormAsync();
            await _backups.NameCaptureAsync(Names.Backups.LinesFull);
            await _backups.ChooseCaptureTreeAsync(FactoryLines);
            await _backups.SubmitCaptureAsync();
            _linesFull = await CapturedAsync(3, "full", "Capture a full backup of " + FactoryLines, Names.Backups.LinesFull);

            var first = true;
            foreach (var edited in StoryData.LinesEditedBeforeIncrement)
            {
                var rate = RateOf(edited.Json);
                await _entry.OpenAsync(FactoryLines, edited.Key);
                var editor = await _entry.EditAsync(edited.Json);
                if (first)
                {
                    await ShotAsync(3, "edit-line", $"The entry editor: {edited.Key}'s rate becomes {rate}");
                }

                await _entry.SaveAsync(editor, edited.Key, rate);
                first = false;
            }

            Coverage.Hit("command:data.edit-entry");
            await ShotAsync(3, "edits-saved", "line/03 and line/07 edited after lines-full was captured");

            await _backups.OpenCaptureFormAsync();
            await _backups.ChooseCaptureKindAsync("Incremental");
            await _backups.NameCaptureAsync(Names.Backups.LinesInc1);
            await _backups.ChooseCaptureTreeAsync(FactoryLines);
            await _backups.ChooseBaseAsync(Names.Backups.LinesFull);
            await ShotAsync(3, "incremental-form", "An incremental capture of factory-lines, built on lines-full");

            await _backups.SubmitCaptureAsync();
            await CapturedAsync(3, "incremental", "Capture an incremental backup of " + FactoryLines, Names.Backups.LinesInc1);
            await Expect(_backups.Fact("Kind")).ToHaveTextAsync("Incremental");
            await Expect(_backups.Fact("Built on")).ToHaveTextAsync(ShortId(_linesFull));
            await Expect(_backups.RestoreChain).ToHaveCountAsync(2);
            await Expect(_backups.RestoreChain.Nth(0)).ToHaveTextAsync(ShortId(_linesFull));
            await Expect(_backups.RestoreChain.Nth(1)).ToContainTextAsync("(this backup)");
            Coverage.Hit("use-case:backups/restore-chain");
            await ShotAsync(3, "restore-chain", "lines-inc-1's Restore chain: lines-full, then this backup");
        });

        /// <summary>A10.4: a set of the orders and the lines at one cross-tree consistency fence.</summary>
        public Task SetAsync() => StepAsync(4, async () =>
        {
            await _backups.OpenCaptureFormAsync();
            await _backups.ChooseCaptureKindAsync("Set of trees");
            await _backups.NameCaptureAsync(Names.Backups.FactorySet1);
            await _backups.AddSetTreeAsync(FactoryOrders);
            await _backups.AddSetTreeAsync(FactoryLines);
            await Expect(_backups.SetTrees).ToHaveCountAsync(2);
            await Expect(_backups.ConsistencyFence).ToBeCheckedAsync();
            await ShotAsync(4, "set-form", "A set of factory-orders and factory-lines, captured at one consistency fence");

            await _backups.SubmitCaptureAsync();
            await olivia.Shell.ExpectHeadingAsync("Capture a backup set of 2 trees");
            await _backups.ExpectOperationSucceededAsync();
            await Expect(_backups.Fact("Backups captured")).ToHaveTextAsync("2");
            await Expect(_backups.ResultLink("The backup of " + FactoryOrders)).ToHaveCountAsync(1);
            await Expect(_backups.ResultLink("The backup of " + FactoryLines)).ToHaveCountAsync(1);
            Coverage.Hit("use-case:backups/capture-kinds");
            await ShotAsync(4, "set-captured", "factory-set-1 succeeded: two backups, one for each tree");
        });

        /// <summary>A10.5: delete a line, then repair <c>factory-lines</c> in place from <c>lines-full</c>; the newer edits and the deletion both win.</summary>
        public Task RepairAsync() => StepAsync(5, async () =>
        {
            await _backups.OpenCatalogueOfAsync(FactoryLines);
            await Expect(_backups.CatalogueRow(Names.Backups.LinesInc1)).ToHaveCountAsync(1);
            await Expect(_backups.CatalogueRow(Names.Backups.LinesFull)).ToHaveCountAsync(1);
            await ShotAsync(5, "lines-catalogue", "factory-lines' backups: lines-inc-1 and its base lines-full both listed (#5450)");

            var deleted = StoryData.LineDeletedBeforeRepair;
            await _entry.OpenAsync(FactoryLines, deleted);
            var confirm = await _entry.RequestDeleteAsync(deleted);
            await ShotAsync(5, "delete-line", $"Deleting {deleted}, confirmed by typing its key");
            await _entry.ConfirmDeleteAsync(confirm, deleted);
            Coverage.Hit("command:data.delete-entry");

            await _backups.OpenCatalogueOfAsync(FactoryLines);
            await _backups.OpenListedBackupAsync(Names.Backups.LinesFull);
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Combobox, new() { Name = "Restore into tree", Exact = true })).ToHaveValueAsync(FactoryLines);
            await Expect(_backups.RestoreMode.Locator("option:checked")).ToHaveTextAsync(RepairMode);
            await ShotAsync(5, "repair-form", "Restore lines-full into factory-lines with Repair missing items (non-destructive)");

            await _backups.RequestRestoreAsync();
            await Expect(_backups.RestoreConfirmation).ToContainTextAsync("any key written since the backup keeps its newer value");
            await _backups.TypeRestoreConfirmationAsync(FactoryLines);
            await ShotAsync(5, "repair-confirmation", "Restore this backup? asks olivia to type factory-lines");

            await _backups.ConfirmRestoreAsync();
            await RestoredAsync(5, "repair", FactoryLines, replayed: 1, "The repair finished: one backup replayed into factory-lines");

            await _entry.OpenAsync(FactoryLines, deleted);
            await _entry.ExpectNoEntryAsync();
            await ShotAsync(5, "deletion-kept", $"{deleted} stays deleted: its deletion is newer than lines-full");
            foreach (var edited in StoryData.LinesEditedBeforeIncrement)
            {
                await _entry.OpenAsync(FactoryLines, edited.Key);
                await _entry.ExpectRateAsync(RateOf(edited.Json));
            }

            Coverage.Hit("use-case:backups/restore-repair");
            await ShotAsync(5, "edits-kept", "The edits survive the repair: line/07 still reads its edited rate");
        });

        /// <summary>A10.6: <c>lines-inc-1</c> replaced point-in-time into the new tree <c>factory-lines-restored</c>, at the chain's restore point.</summary>
        public Task PointInTimeAsync() => StepAsync(6, async () =>
        {
            var target = Names.Trees.FactoryLinesRestored;
            await _backups.OpenCatalogueOfAsync(FactoryLines);
            await _backups.OpenListedBackupAsync(Names.Backups.LinesInc1);
            await _backups.ChooseRestoreTargetAsync(target);
            await _backups.ChooseRestoreModeAsync(PointInTimeMode);
            await Expect(_backups.RestorePoint.Locator("option")).ToHaveCountAsync(2);
            await _backups.ChooseThisBackupAsRestorePointAsync();
            await ShotAsync(6, "point-in-time-form", "Point-in-time replace of lines-inc-1 into the new tree factory-lines-restored, at the chain's newest restore point");

            await _backups.RequestRestoreAsync();
            await Expect(_backups.RestoreConfirmation).ToContainTextAsync("Every write made after the backup was taken is dropped.");
            await _backups.TypeRestoreConfirmationAsync(target);
            await ShotAsync(6, "confirmation", "Restore this backup? names the consequence and asks for factory-lines-restored");

            await _backups.ConfirmRestoreAsync();
            await RestoredAsync(6, "restored", target, replayed: 2, "The point-in-time restore replayed the chain's two backups; it can be reverted");
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "Revert this restore" })).ToBeVisibleAsync();

            var edited = StoryData.LinesEditedBeforeIncrement[0];
            await _entry.OpenAsync(target, edited.Key);
            await _entry.ExpectRateAsync(RateOf(edited.Json));
            Coverage.Hit("use-case:backups/restore-point-in-time");
            await ShotAsync(6, "restored-tree", "factory-lines-restored holds line/03 as lines-inc-1 captured it");
        });

        /// <summary>A10.7: a cold restore of <c>lines-full</c> into <c>factory-lines-cold</c>, from the backup store alone.</summary>
        public Task ColdRestoreAsync() => StepAsync(7, async () =>
        {
            var target = Names.Trees.FactoryLinesCold;
            await _backups.OpenCatalogueOfAsync(FactoryLines);
            await _backups.OpenListedBackupAsync(Names.Backups.LinesFull);
            await _backups.ChooseRestoreTargetAsync(target);
            await _backups.ColdRestore.CheckAsync();
            await ShotAsync(7, "cold-form", "Restore lines-full into factory-lines-cold from the backup store alone");

            await _backups.RequestRestoreAsync();
            await _backups.TypeRestoreConfirmationAsync(target);
            await _backups.ConfirmRestoreAsync();
            await Expect(olivia.Shell.Heading).ToHaveTextAsync(new System.Text.RegularExpressions.Regex("^Cold-restore "));
            await RestoredAsync(7, "cold-restored", target, replayed: 1, "The cold restore ran as a tracked operation and succeeded");

            // lines-full was captured before the edits and the deletion.
            await _entry.OpenAsync(target, StoryData.LinesEditedBeforeIncrement[0].Key);
            await _entry.ExpectRateAsync(RateOf(StoryData.Lines[2].Json));
            await _entry.OpenAsync(target, StoryData.LineDeletedBeforeRepair);
            await _entry.ExpectRateAsync(RateOf(StoryData.Lines[4].Json));
            Coverage.Hit("use-case:backups/restore-cold");
            await ShotAsync(7, "cold-tree", "factory-lines-cold holds lines-full as captured: line/05 is there");
        });

        /// <summary>A10.8: the first capture's operation, reopened by its address, still reads finished.</summary>
        public Task ReopenOperationAsync() => StepAsync(8, async () =>
        {
            await olivia.Shell.GotoAsync(_prefixOperation);
            await olivia.Shell.ExpectHeadingAsync("Capture a full backup of " + FactoryOrders);
            await _backups.ExpectOperationSucceededAsync();
            Assert.That(await BackupsArea.BackupIdOfAsync(_backups.ResultLink("The captured backup")), Is.EqualTo(_prefixBackup), "the reopened operation names orders-prefix-0001");
            Coverage.Hit("use-case:backups/addresses");
            await ShotAsync(8, "reopened", "orders-prefix-0001's capture, reopened by its address, still reads Succeeded");
        });

        /// <summary>A10.9: an hourly incremental schedule on <c>factory-lines</c>.</summary>
        public Task IncrementalScheduleAsync() => StepAsync(9, async () =>
        {
            await _backups.OpenSchedulesAsync(FactoryLines);
            await Expect(_backups.ScheduleRow("Incremental")).ToContainTextAsync("None");
            await ShotAsync(9, "schedules", "factory-lines has no schedule yet");

            var interval = Contracts.Contoso.A10Contract.LinesScheduleInterval;
            await _backups.FillScheduleAsync("Incremental", (int)interval.TotalHours, interval.Minutes);
            await ShotAsync(9, "schedule-form", "An incremental schedule every hour");

            await _backups.SaveScheduleAsync("Scheduled an incremental backup every 1 h.");
            await Expect(_backups.ScheduleRow("Incremental")).ToContainTextAsync("Registered");
            await Expect(_backups.ScheduleRow("Incremental")).ToContainTextAsync("1 h");
            Coverage.Hit("use-case:backups/schedule-incremental");
            await ShotAsync(9, "saved", "The incremental schedule is registered, every 1 h");
        });

        private static async Task StepAsync(int step, Func<Task> body)
        {
            using var scope = Coverage.BeginScope(string.Create(CultureInfo.InvariantCulture, $"A10.{step}"));
            await body();
            scope.Complete();
        }

        private Task<JourneyManifestEntry> ShotAsync(int step, string substep, string title) =>
            olivia.SubstepAsync(_recorder, step.ToString(CultureInfo.InvariantCulture), substep, title);

        // A capture's operation page: its title, success and the link to the backup, whose id
        // is recorded at backup.<name>; then the backup's own page, named.
        private async Task<string> CapturedAsync(int step, string substep, string title, string name)
        {
            await olivia.Shell.ExpectHeadingAsync(title);
            await _backups.ExpectOperationSucceededAsync();
            var link = _backups.ResultLink("The captured backup");
            var id = await BackupsArea.BackupIdOfAsync(link);
            context.Record(StoryKeys.Backup(name), id);
            await ShotAsync(step, substep + "-captured", $"{name} captured: the cluster's operation succeeded");

            await link.ClickAsync();
            await olivia.Shell.ExpectHeadingAsync(name);
            await Expect(_backups.Fact("Backup id")).ToContainTextAsync(id);
            return id;
        }

        private async Task RestoredAsync(int step, string substep, string target, int replayed, string title)
        {
            await _backups.ExpectOperationSucceededAsync();
            await Expect(_backups.Fact("Tree")).ToHaveTextAsync(target);
            await Expect(_backups.Fact("Backups replayed")).ToHaveTextAsync(replayed.ToString(CultureInfo.InvariantCulture));
            await Expect(_backups.ResultLink("The restored backup")).ToHaveCountAsync(1);
            await ShotAsync(step, substep, title);
        }

        private static string ShortId(string backupId) => backupId.Length > 12 ? backupId[..12] : backupId;

        private static int RateOf(string json)
        {
            using var document = System.Text.Json.JsonDocument.Parse(json);
            return document.RootElement.GetProperty("rate").GetInt32();
        }
    }
}
