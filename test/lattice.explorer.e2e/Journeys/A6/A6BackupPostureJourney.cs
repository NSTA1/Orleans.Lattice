using System.Globalization;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A6;

/// <summary>
/// Journey A6 (#5377), backup posture: Olivia captures a baseline of the orders to the separate
/// backup account, cancels a capture she started by mistake, schedules "hourly" telemetry backups,
/// turns on monitoring for the baseline, and checks the catalogue against the blobs.
/// </summary>
/// <remarks>
/// <para>
/// Two actions are the harness's, as the external actors they model, and never the journey's
/// through a back door: in step 2 it reads the backup storage account directly to show the
/// captured artifacts landed there; in step 4 it holds the backup account's artifact writes, as a
/// congested storage account would, so the capture Olivia starts by mistake is still running when
/// she cancels it (a telemetry capture otherwise finishes in about a second). In step 8 it acts
/// as another administrator through the Auth API: it delegates backups to Priya, narrows the
/// grant while her capture form is open so the server refuses her capture, restores it, and
/// finally withdraws the delegation.
/// </para>
/// <para>
/// The "hourly" schedule is registered at the story's compressed hour,
/// <see cref="A6Contract.TelemetryScheduleInterval"/>, and once it has fired it is settled to a
/// real hour, <see cref="A6Contract.TelemetrySteadyInterval"/>, so a full capture of the growing
/// telemetry every minute does not saturate the estate for the rest of the story.
/// </para>
/// </remarks>
[JourneyChapter("A6", After = new[] { "A3", "A4" })]
public sealed class A6BackupPostureJourney : JourneyChapter
{
    /// <summary>The name of the capture Olivia starts by mistake, then cancels.</summary>
    public const string MistakenCapture = "telemetry-by-mistake";

    /// <summary>The name of the capture the server refuses Priya once her grant is narrowed.</summary>
    public const string PriyaCapture = "orders-by-priya";

    /// <summary>The scratch rule that delegates backups to Priya during step 8; withdrawn before the chapter ends.</summary>
    public const string DelegatedBackupRuleId = ContosoAccess.ScratchRulePrefix + "a6-priya-backups";

    private static readonly TimeSpan ScheduleFireTimeout = TimeSpan.FromMinutes(5);

    private static JourneyRecorder Recorder => JourneyRecorder.For("a6");

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var backup = ContractFacades.Of(context).Backup;
        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        var backups = new BackupsAreaPage(olivia);
        string baseline = null!;

        await StepAsync(1, async step =>
        {
            await backups.OpenFromSpineAsync();
            await backups.ExpectCatalogueEmptyAsync();
            await olivia.SubstepAsync(Recorder, step, "catalogue-empty", "The catalogue is empty: no backups yet");

            await backups.GoToAsync("Health", "Health");
            await Expect(backups.NoBackupsYet).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await Expect(olivia.Shell.Content.GetByText("Health applies once a backup exists.", new() { Exact = true })).ToBeVisibleAsync();
            Coverage.Hit("route:/backups/health");
            Coverage.Hit("subroute:backups/health");
            await olivia.SubstepAsync(Recorder, step, "health-empty", "Health shows No backups yet");
        });

        await StepAsync(2, async step =>
        {
            await backups.OpenCaptureFromPaletteAsync();
            await backups.FillCaptureAsync("Full", Names.Backups.OrdersBaseline, Names.Trees.FactoryOrders);
            await olivia.SubstepAsync(Recorder, step, "capture-form", "Capture backup: Full, the whole of factory-orders, named orders-baseline");

            await backups.SubmitCaptureAsync(Names.Trees.FactoryOrders);
            baseline = await backups.CapturedBackupIdAsync();
            context.Record(StoryKeys.OrdersBaselineBackup, baseline);
            await olivia.SubstepAsync(Recorder, step, "capture-succeeded", "The operation page tracks the capture to completion and links the captured backup");

            // Off-UI, as the storage account's owner would look: the manifest and every artifact
            // are blobs in the separate backup account.
            var described = await backup.DescribeBackupAsync(baseline)
                ?? throw new AssertionException($"The catalogue does not describe the captured backup {baseline}.");
            IReadOnlyList<string> blobs = [];
            await ContractWait.UntilAsync("the baseline's manifest blob in the backup account", async () =>
            {
                blobs = await estate.BackupBlobsAsync();
                return blobs.Contains("manifests/" + baseline, StringComparer.Ordinal);
            }, TimeSpan.FromSeconds(60));
            Assert.Multiple(() =>
            {
                Assert.That(described.Manifest.ContentDescriptors, Is.Not.Empty, "the baseline has artifacts");
                foreach (var content in described.Manifest.ContentDescriptors)
                {
                    Assert.That(blobs, Does.Contain("artifacts/" + content.ArtifactId), $"artifact {content.ArtifactId} is a blob in the backup account");
                }
            });
            Coverage.Hit("use-case:backups/capture-full");
        });

        await StepAsync(3, async step =>
        {
            await backups.BackToCatalogueAsync();
            await backups.FilterByNameAsync(Names.Backups.OrdersBaseline);
            await olivia.SubstepAsync(Recorder, step, "catalogue-lists-baseline", "The catalogue, filtered by name, lists orders-baseline");

            await backups.OpenBackupFromCatalogueAsync(Names.Backups.OrdersBaseline);
            var manifest = (await backup.DescribeBackupAsync(baseline))!.Manifest;
            await Expect(backups.Definition("Backup id")).ToContainTextAsync(baseline);
            await Expect(backups.Definition("Scope")).ToHaveTextAsync("Whole tree");
            await Expect(backups.Definition("Kind")).ToHaveTextAsync("Full");
            await Expect(backups.Definition("Tree")).ToContainTextAsync(Names.Trees.FactoryOrders);
            foreach (var content in manifest.ContentDescriptors)
            {
                await Expect(backups.Row(content.ArtifactId)).ToBeVisibleAsync();
            }

            if (!await olivia.Shell.IsCompactAsync())
            {
                var first = backups.Row(manifest.ContentDescriptors[0].ArtifactId).Locator("td");
                await Expect(first.Nth(1)).ToHaveTextAsync(manifest.ContentDescriptors[0].ChunkCount.ToString(CultureInfo.InvariantCulture));
            }

            Coverage.Hit("route:/backups/{p1}");
            await olivia.SubstepAsync(Recorder, step, "baseline-details", "The backup's page: its id, scope, kind, and its artifacts with their sizes and chunks");

            var artifact = manifest.ContentDescriptors[0];
            var file = await backups.ExportArtifactAsync(artifact.ArtifactId, Path.Combine(context.ArtifactsDirectory, "downloads", "a6"));
            Assert.That(file.Length, Is.EqualTo(artifact.ByteLength), $"the exported artifact {artifact.ArtifactId} streamed whole");
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Alert)).ToHaveCountAsync(0);
            await olivia.SubstepAsync(Recorder, step, "artifact-exported", $"Export streams artifact {artifact.ArtifactId} to a download of its full {artifact.ByteLength:N0} bytes");
        });

        await StepAsync(4, async step =>
        {
            await backups.BackToCatalogueAsync();
            await using (await estate.HoldBackupWritesAsync())
            {
                await backups.OpenCaptureFromToolbarAsync();
                await backups.FillCaptureAsync("Full", MistakenCapture, Names.Trees.FactoryTelemetry);
                await backups.SubmitCaptureAsync(Names.Trees.FactoryTelemetry);
                await backups.ExpectOperationStateAsync("Running");
                await olivia.SubstepAsync(Recorder, step, "telemetry-capture-running", "A full capture of factory-telemetry, started by mistake, is running");

                await backups.CancelOperationAsync();
                await backups.ExpectOperationStateAsync("Cancelled");
                await olivia.SubstepAsync(Recorder, step, "telemetry-capture-cancelled", "Cancel operation: the capture stops, cancelled");
            }

            await backups.BackToCatalogueAsync();
            var recorded = backups.ClusterOperations.Filter(new() { HasText = "Capture a full backup of " + Names.Trees.FactoryTelemetry });
            await Expect(recorded.First).ToContainTextAsync("Cancelled", new() { Timeout = 60_000 });
            await Expect(backups.Row(MistakenCapture)).ToHaveCountAsync(0);
            Coverage.Hit("use-case:backups/capture-cancel");
            await olivia.SubstepAsync(Recorder, step, "catalogue-records-cancelled", "The catalogue records the telemetry capture cancelled, and lists no backup from it");

            await backups.OpenCaptureFromToolbarAsync();
            await backups.FillCaptureAsync("Incremental", name: null, Names.Trees.FactoryOrders);
            var bases = await backups.FindFullBasesAsync();
            await Expect(bases).ToHaveValueAsync(baseline);
            await Expect(bases.Locator("option:checked")).ToContainTextAsync(Names.Backups.OrdersBaseline + ", ");
            Coverage.Hit("use-case:backups/catalogue-full-bases");
            await olivia.SubstepAsync(Recorder, step, "full-bases", "Incremental of factory-orders: Find full backups of this tree offers orders-baseline as the base");

            await backups.LeaveCaptureAsync();
        });

        await StepAsync(5, async step =>
        {
            await backups.GoToAsync("Schedules", "Schedules");
            await backups.ShowSchedulesAsync(Names.Trees.FactoryTelemetry);
            await Expect(backups.ScheduleRow("Full")).ToContainTextAsync("None");
            await olivia.SubstepAsync(Recorder, step, "telemetry-schedules", "Show schedules for factory-telemetry: no schedule yet");

            var compressed = A6Contract.TelemetryScheduleInterval;
            await backups.SaveScheduleAsync("Full", compressed.Hours, compressed.Minutes, "Scheduled a full backup every 1 min.");
            await Expect(backups.ScheduleRow("Full")).ToContainTextAsync("Registered");
            await olivia.SubstepAsync(Recorder, step, "hourly-schedule-saved", "Save schedule: an \"hourly\" Full schedule, at the story's compressed hour of one minute");

            await backups.WaitForFullScheduleToSucceedAsync(ScheduleFireTimeout);
            Coverage.Hit("use-case:backups/schedule-fires");
            await olivia.SubstepAsync(Recorder, step, "schedule-fired", "The compressed cadence fires it: Last run and Last success fill");

            var steady = A6Contract.TelemetrySteadyInterval;
            await backups.SaveScheduleAsync("Full", (int)steady.TotalHours, steady.Minutes, "Scheduled a full backup every 1 h.");
            Coverage.Hit("route:/backups/schedules");
            Coverage.Hit("subroute:backups/schedules");
            await olivia.SubstepAsync(Recorder, step, "schedule-settled", "Having fired, the schedule is saved again at a real hour");
        });

        await StepAsync(6, async step =>
        {
            await backups.GoToAsync("Health", "Health");
            await backups.OpenHealthOfAsync(Names.Backups.OrdersBaseline);
            await olivia.SubstepAsync(Recorder, step, "baseline-health", "/backups/health?backup= focuses orders-baseline");

            var verify = A6Contract.BaselineVerifyInterval;
            await backups.SaveMonitoringAsync((int)verify.TotalHours, verify.Minutes);
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Status).Filter(new() { HasText = "verified every 1 h." })).ToBeVisibleAsync();
            await olivia.SubstepAsync(Recorder, step, "monitoring-on", "Monitoring on, Verify every 1 hour, saved");

            await backups.CheckNowAsync();
            await Expect(backups.HealthStatus).ToHaveTextAsync("Healthy", new() { Timeout = 60_000 });
            Coverage.Hit("use-case:backups/health-monitoring");
            await olivia.SubstepAsync(Recorder, step, "healthy", "Check now: the check finishes and the health pill reads Healthy");
        });

        await StepAsync(7, async step =>
        {
            await backups.GoToAsync("Maintenance", "Maintenance");
            Coverage.Hit("route:/backups/maintenance");
            Coverage.Hit("subroute:backups/maintenance");
            await olivia.SubstepAsync(Recorder, step, "maintenance", "Maintenance: rebuild the catalogue, or check it against the store");

            var before = await CatalogueIdsAsync(backup);
            await backups.CheckCatalogueAsync();
            await Expect(backups.Definition("Orphan rows")).ToHaveTextAsync("0");
            await Expect(backups.Definition("Rows removed")).ToHaveTextAsync("0");
            await olivia.SubstepAsync(Recorder, step, "catalogue-checked", "Check catalogue: every catalogue row has its backup in the store");

            await backups.BackToCatalogueAsync();
            await backups.GoToAsync("Maintenance", "Maintenance");
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = "Every catalogue row has its backup in the store." })).ToBeVisibleAsync(new() { Timeout = 60_000 });
            var confirm = await backups.AskRebuildAsync();
            await olivia.SubstepAsync(Recorder, step, "rebuild-confirm", "Rebuild catalogue...: the confirmation");

            await backups.ConfirmRebuildAsync(confirm);
            await Expect(backups.Definition("Added to the catalogue")).ToHaveTextAsync("0");
            var after = await CatalogueIdsAsync(backup);
            Assert.That(after, Is.EquivalentTo(before), "the rebuild neither added nor removed a backup");
            Coverage.Hit("use-case:backups/maintenance");
            await olivia.SubstepAsync(Recorder, step, "catalogue-rebuilt", "Rebuild: every manifest is reconciled in place, nothing added, nothing changes");
        });

        await StepAsync(8, async step =>
        {
            // Off-UI, the harness acts as another administrator through the Auth API: it
            // delegates baseline captures to Priya for a while, narrows that grant while her
            // capture form is open, then restores it, and finally withdraws the delegation so
            // the estate leaves A6 exactly as A6's contract says.
            var auth = ContractFacades.Of(context).Auth;
            var priyaBackup = ContractFacades.Of(context, user: Personas.Priya.Id).Backup;
            var orders = BackupScopeSelector.WholeTree(Names.Trees.FactoryOrders);
            await auth.PutRuleAsync(new AuthPutRule { Rule = DelegatedBackup(withBackup: true) });
            await ContractWait.UntilAsync("Priya's delegated backup grant", async () => (await priyaBackup.ProbeCapabilitiesAsync(orders)).CanCapture);
            try
            {
                await using var priya = await estate.SignInAsAsync(Personas.Priya);
                var priyaBackups = new BackupsAreaPage(priya);
                await priyaBackups.OpenFromSpineAsync();
                await priyaBackups.OpenCaptureFromToolbarAsync();
                await priyaBackups.FillCaptureAsync("Full", PriyaCapture, Names.Trees.FactoryOrders);
                await priya.SubstepAsync(Recorder, step, "priya-capture-form", "Priya, holding a delegated backup grant, fills in a full capture of factory-orders");

                await auth.PutRuleAsync(new AuthPutRule { Rule = DelegatedBackup(withBackup: false) });
                await ContractWait.UntilAsync("Priya's narrowed grant", async () => !(await priyaBackup.ProbeCapabilitiesAsync(orders)).CanCapture);
                await priyaBackups.SubmitCaptureExpectingRefusalAsync(Names.Trees.FactoryOrders, "You are not permitted to do this.");
                Assert.That(await ContractCalls.FindBackupAsync(backup, PriyaCapture), Is.Null, "the refused capture produced no backup");
                Coverage.Hit("use-case:backups/server-authority");
                await priya.SubstepAsync(Recorder, step, "priya-capture-refused", "Her grant narrowed while the form was open: the server refuses the capture at its Check access stage, and the page says so");

                await auth.PutRuleAsync(new AuthPutRule { Rule = DelegatedBackup(withBackup: true) });
                await ContractWait.UntilAsync("Priya's restored grant", async () => (await priyaBackup.ProbeCapabilitiesAsync(orders)).CanCapture);
                await priyaBackups.BackToCatalogueAsync();
                await priyaBackups.GoToAsync("Schedules", "Schedules");
                await priyaBackups.ShowSchedulesOnceGrantedAsync(Names.Trees.FactoryOrders, TimeSpan.FromMinutes(2));
                await priyaBackups.ExpectSchedulesOfferedAsync();
                await priya.SubstepAsync(Recorder, step, "priya-grant-restored", "The grant restored: the server lets Priya read and schedule factory-orders' backups again");
            }
            finally
            {
                await auth.RemoveRuleAsync(new AuthRuleRef { TreeId = LatticeScope.ClusterWideTreeId, RuleId = DelegatedBackupRuleId });
            }

            Assert.That(
                (await ContractCalls.AllRulesAsync(auth)).Where(rule => rule.RuleId == DelegatedBackupRuleId),
                Is.Empty,
                "the delegation is withdrawn, so A6's produces are unchanged");
        });
    }

    private static LatticeAuthorizationRule DelegatedBackup(bool withBackup) => new(
        DelegatedBackupRuleId,
        LatticeSubjectSelector.User(Personas.Priya.Id),
        LatticeScope.Tree(LatticeScope.ClusterWideTreeId),
        ContosoAccess.ReadOps | (withBackup ? LatticeOperation.Backup : LatticeOperation.None),
        LatticeEffect.Allow);

    private static async Task StepAsync(int number, Func<string, Task> body)
    {
        using var scope = Coverage.BeginScope("A6." + number.ToString(CultureInfo.InvariantCulture));
        await body(number.ToString(CultureInfo.InvariantCulture));
        scope.Complete();
    }

    private static async Task<IReadOnlyList<string>> CatalogueIdsAsync(Orleans.Lattice.Api.Backup.Grpc.LatticeBackupApiGrpcClient backup) =>
        [.. (await ContractCalls.BackupsAsync(backup)).Select(manifest => manifest.Id).Order(StringComparer.Ordinal)];
}
