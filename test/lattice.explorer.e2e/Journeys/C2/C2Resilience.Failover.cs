using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;
using Trees = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Trees;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>C2.1 and C2.2: a silo lost behind the gateway, and the silo holding an operation lost.</summary>
public sealed partial class C2Resilience
{
    /// <summary>The line feed's sensors, in the order it writes them (its first is the one C2.1 follows).</summary>
    private const string FollowedSensor = "press-1";

    /// <summary>How many sensors the line feed writes round-robin.</summary>
    private const int Sensors = 4;

    /// <summary>How long the scan of C2.2 is held on the silo running it, so it is Running when its silo is killed.</summary>
    private static readonly TimeSpan ScanHold = TimeSpan.FromSeconds(150);

    /// <summary>What the Compliance tab says when it cannot read the followed scan's status.</summary>
    private const string ProgressUnreadable = "read the scan's progress";

    /// <summary>
    /// How long after the tab first fails to read the scan the record is given to reactivate on
    /// the surviving silo before C2.2 concludes it cannot (#5579). A healthy record reloads in a
    /// few seconds.
    /// </summary>
    private static readonly TimeSpan RecordReactivationWindow = TimeSpan.FromSeconds(60);

    /// <summary>
    /// C2.1: priya follows ten telemetry readings that the line feed will write over the next
    /// half minute, live; the harness kills west silo 1 under her. The connection indicator
    /// reports the blip, the gateway routes to silo 0, and all ten readings arrive: the line
    /// feed retries a refused reading, so a reading missing from her view would be one the
    /// console lost. The harness restarts the silo afterwards.
    /// </summary>
    private static async Task SiloFailoverAsync(JourneyEstate estate, PersonaSession priya, C2Step step)
    {
        var keys = new DataKeysView(priya);
        await keys.OpenTreeAsync(Trees.FactoryTelemetry);

        var feed = await estate.LineFeed.StatusAsync();
        Assert.That(feed is { Started: true, Paused: false }, Is.True, $"the line feed is writing telemetry ({feed})");

        // The readings of the followed sensor are reading/press-1/{n:D8}, one every four writes.
        // Follow the next whole decade of them: it starts after the kill and spans the outage.
        var next = (feed.TelemetryWritten + Sensors - 1) / Sensors;
        var decade = (next / 10) + 1;
        var prefix = $"reading/{FollowedSensor}/{decade.ToString("D7", CultureInfo.InvariantCulture)}";
        var expected = Enumerable.Range(0, 10).Select(i => $"{prefix}{i}").ToList();
        await keys.FilterAsync(prefix);
        await keys.ExpectLiveAsync();
        await step.CaptureAsync(priya, "following", $"priya follows the readings under {prefix}, live, before the line feed writes them");

        var watch = await ConnectionPillWatch.StartAsync(priya.Page);
        try
        {
            await estate.KillSiloAsync(JourneyEstate.West, FailoverSilo);
            step.Log($"killed west silo {FailoverSilo}; following {prefix}");
            await estate.WaitForGatewayToMatchSilosAsync(JourneyEstate.West, TimeSpan.FromMinutes(1));
            var routes = await estate.GatewayDestinationsAsync(JourneyEstate.West);
            Assert.That(routes.Where(route => route.Healthy).Select(route => route.Id), Is.EqualTo(new[] { "silo-0" }), "the gateway routes to silo 0 alone");

            var notes = await keys.WaitForKeysAsync(expected, TimeSpan.FromMinutes(4));
            step.Log("live updates resumed on their own; the tab's notes on the way: " + (notes.Count == 0 ? "none" : string.Join(" | ", notes)));
            await priya.Shell.ExpectConnectionAsync("Connected");
            await Expect(keys.Notes).ToHaveCountAsync(0);

            if (watch.IsWatching)
            {
                var history = await watch.HistoryAsync();
                step.Log("connection indicator read: " + string.Join(" -> ", history));
                Assert.That(
                    history.Any(reading => Blip().IsMatch(reading)),
                    Is.True,
                    $"the connection indicator reported the silo's loss (it read {string.Join(" -> ", history)})");
            }
            else
            {
                step.Log("the compact chrome folds the connection indicator into the closed Menu sheet, so its readings during the blip were not recorded");
            }

            await step.CaptureAsync(
                priya,
                "resumed",
                $"Live updates resumed on their own and all ten readings under {prefix} arrived; the indicator reads Connected");
            Coverage.Hit("tab:data/keys");
            Coverage.Hit("use-case:cluster/silo-failover");
        }
        finally
        {
            await RestoreSilosAsync(estate, JourneyEstate.West);
        }
    }

    /// <summary>
    /// C2.2: olivia scans the scratch tree's compliance; the harness locates the activation of
    /// the scan's operation record and kills the silo hosting it while the scan is Running. The
    /// tab then shows an honest end within a bound - the report, once the record reactivated on
    /// the surviving silo, or the reason the scan failed - and never keeps saying it is scanning.
    /// Until #5579 is fixed the record cannot reload from storage, so the step asserts that
    /// defect instead (see the <c>KnownDefect</c> below).
    /// </summary>
    private static async Task OperationSiloLostAsync(JourneyEstate estate, C2Step step)
    {
        await C2ScanTree.SeedAsync(estate);
        var schema = ContractFacades.Of(estate).Schema;
        int? killed = null;
        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        try
        {
            await estate.DelayComplianceScanAsync(C2ScanTree.TreeId, ScanHold);
            var view = new SchemaComplianceView(olivia);
            await view.StartScanAsync(C2ScanTree.TreeId);
            await view.ExpectScanningAsync();

            var running = await RunningScanAsync(schema);
            var record = await RecordOfAsync(estate, running.OperationId);
            var located = await estate.LocateActivationAsync(record.GrainType, record.Key);
            Assert.That(located, Is.EqualTo(record.Silo), $"the management grain and the activation listing agree where {record.GrainId} lives");

            // The precondition the step stands on: the scan is Running at the moment of the kill.
            var status = await schema.GetComplianceScanStatusAsync(running.OperationId);
            Assert.That(status?.State, Is.EqualTo(LatticeOperationState.Running), $"scan {running.OperationId} is Running when its silo is killed");
            await step.CaptureAsync(olivia, "scanning", $"olivia's scan of {C2ScanTree.TreeId} is Running; its record lives on west silo {record.Silo}");

            await estate.KillSiloAsync(JourneyEstate.West, record.Silo);
            killed = record.Silo;
            step.Log($"killed west silo {record.Silo}, which hosted {record.GrainId} while scan {running.OperationId} was Running");
            await estate.ClearComplianceScanDelayAsync(C2ScanTree.TreeId);

            // #5579: the record cannot reactivate from Azure Table storage (its TreeIds list is a
            // compiler-synthesized collection the JSON provider cannot rebuild), so the scan never
            // ends. Once that is fixed this wrapper fails: replace it with the honest-end check -
            // view.WaitForOutcomeAsync, then the cluster's terminal status matching the tab (the
            // report, or Failed with its reason).
            await new KnownDefect("#5579", "An operation record cannot be reloaded from Azure Table storage, so a scan whose record's silo was lost never ends").ReproducesAsync(async () =>
            {
                await Expect(view.Error).ToContainTextAsync(ProgressUnreadable, new() { Timeout = 120_000 });
                await Task.Delay(RecordReactivationWindow);
                Assert.That(await view.Scanning.IsVisibleAsync(), Is.True, "the tab still says the scan is running");
                Assert.That(
                    async () => await schema.GetComplianceScanStatusAsync(running.OperationId),
                    Throws.Exception,
                    $"scan {running.OperationId}'s record still cannot be read {RecordReactivationWindow.TotalSeconds:0} s after its silo was lost");
            });
            step.Log($"scan {running.OperationId} cannot end: its record does not reload from storage (#5579)");
            await step.CaptureAsync(
                olivia,
                "outcome",
                "The record's silo is gone and the record cannot be reloaded (#5579): the tab says it cannot read the scan's progress, and the scan never ends");
            Coverage.Hit("route:/schema");
            Coverage.Hit("route:/schema/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
            Coverage.Hit("use-case:cluster/operation-silo-lost");
        }
        finally
        {
            await TryAsync(() => estate.ClearComplianceScanDelayAsync(C2ScanTree.TreeId));
            await RestoreSilosAsync(estate, JourneyEstate.West);
            step.Log(killed is { } index ? $"restarted west silo {index}" : "no silo was killed");
            await C2ScanTree.PurgeAsync(estate);
        }
    }

    /// <summary>The newest scan of the scratch tree that has not ended, as the cluster lists it.</summary>
    private static async Task<LatticeOperationStatus> RunningScanAsync(LatticeSchemaApiGrpcClient schema)
    {
        LatticeOperationStatus? found = null;
        await ContractWait.UntilAsync($"the scan of {C2ScanTree.TreeId} to be listed", async () =>
        {
            var page = await schema.ListComplianceScansAsync(new LatticeOperationListRequest());
            found = page.Operations.FirstOrDefault(operation => !operation.IsTerminal && operation.Scope.TreeIds.Contains(C2ScanTree.TreeId));
            return found is not null;
        }, TimeSpan.FromSeconds(30));
        return found!;
    }

    /// <summary>The one activation of the operation's durable record, and the silo hosting it.</summary>
    private static async Task<JourneyActivation> RecordOfAsync(JourneyEstate estate, string operationId)
    {
        IReadOnlyList<JourneyActivation> records = [];
        await ContractWait.UntilAsync($"the record of operation {operationId} to be active", async () =>
        {
            records = [.. (await estate.FindActivationsAsync(operationId)).Where(activation => activation.GrainType.Contains("operation", StringComparison.OrdinalIgnoreCase))];
            return records.Count > 0;
        }, TimeSpan.FromSeconds(30));
        Assert.That(records, Has.Count.EqualTo(1), $"one record activation for operation {operationId}: {string.Join(", ", records)}");
        return records[0];
    }

    private static async Task TryAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"C2 cleanup step failed and was skipped: {ex.Message}");
        }
    }

    [GeneratedRegex(@"\b(Reconnecting|Disconnected|Connecting|Sign-in required)\b")]
    private static partial Regex Blip();
}
