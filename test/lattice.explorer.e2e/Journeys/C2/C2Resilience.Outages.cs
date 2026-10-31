using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Trees = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Trees;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>C2.4 to C2.6: a region outage, a telemetry outage, and the backup schedule after them.</summary>
public sealed partial class C2Resilience
{
    private const string Outbound = "Outbound";

    /// <summary>The Telemetry board C2.5 watches.</summary>
    private const string Board = "Throughput";

    /// <summary>The interval C2.6 tightens the telemetry schedule to, so its next run comes inside the step.</summary>
    private static readonly TimeSpan NextRunInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// C2.4: olivia watches factory-orders' outbound link to east on its Replication page, which
    /// refreshes itself. The harness isolates east: the link degrades, and its entries behind grow
    /// as the line feed keeps writing orders in west. The harness heals east, and the link
    /// returns to Healthy with nothing behind.
    /// </summary>
    private static async Task RegionOutageAsync(JourneyEstate estate, PersonaSession olivia, C2Step step)
    {
        var view = new ReplicationTreeView(olivia);
        await view.OpenAsync(Trees.FactoryOrders);
        await view.WaitForAsync(JourneyEstate.East, Outbound, "Healthy", link => link.IsHealthy, TimeSpan.FromMinutes(3));
        await step.CaptureAsync(olivia, "healthy", "factory-orders replicates to east: the outbound link is Healthy");

        try
        {
            await estate.IsolateRegionAsync(JourneyEstate.East);
            step.Log("isolated east: every east silo killed");
            var degraded = await view.WaitForAsync(
                JourneyEstate.East, Outbound, "a degraded health with entries behind",
                link => link.IsDegraded && link.EntriesBehind > 0, TimeSpan.FromMinutes(4));
            var growing = await view.WaitForAsync(
                JourneyEstate.East, Outbound, $"more than {degraded.EntriesBehind} entries behind",
                link => link.IsDegraded && link.EntriesBehind > degraded.EntriesBehind, TimeSpan.FromMinutes(2));
            step.Log($"degraded: {degraded.Text}; then {growing.Text}");
            await step.CaptureAsync(olivia, "east-isolated", $"East is isolated: the link reads {growing.Health} and {growing.EntriesBehind} entries behind, growing as west writes");
        }
        finally
        {
            await estate.HealRegionAsync(JourneyEstate.East);
            step.Log("healed east");
        }

        var caughtUp = await view.WaitForAsync(
            JourneyEstate.East, Outbound, "Healthy with nothing behind",
            link => link.IsHealthy && link.EntriesBehind == 0, TimeSpan.FromMinutes(6));
        step.Log($"recovered: {caughtUp.Text}");
        await step.CaptureAsync(olivia, "east-healed", "East is healed: the link is Healthy again and nothing is behind");
        Coverage.Hit("route:/replication/trees/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
        Coverage.Hit("use-case:replication/outage-and-recovery");
    }

    /// <summary>
    /// C2.5: olivia's Throughput board draws; the harness stops Prometheus and she refreshes it:
    /// every chart says the metrics backend did not answer and offers <b>Try again</b>. The
    /// harness starts Prometheus, she presses <b>Try again</b>, and the charts draw again.
    /// </summary>
    private static async Task TelemetryOutageAsync(JourneyEstate estate, PersonaSession olivia, C2Step step)
    {
        var board = new TelemetryBoardView(olivia);
        await board.OpenAsync(Board);
        await estate.WaitForPrometheusScrapesAsync();
        await board.ExpectDrawnAsync(TimeSpan.FromMinutes(2));
        await step.CaptureAsync(olivia, "drawn", $"The {Board} board draws from Prometheus");

        try
        {
            await estate.StopPrometheusAsync();
            step.Log("stopped Prometheus");
            await board.RefreshAsync();
            await board.ExpectBackendDownAsync(TimeSpan.FromMinutes(1));
            await step.CaptureAsync(olivia, "backend-down", "Prometheus is down: every chart says the metrics backend did not answer, with Try again");
        }
        finally
        {
            if (!estate.PrometheusIsRunning)
            {
                await estate.StartPrometheusAsync();
                step.Log("started Prometheus");
            }
        }

        await estate.WaitForPrometheusScrapesAsync();
        var retried = await board.TryAgainAsync();
        step.Log($"pressed Try again on {retried} chart(s)");
        await board.ExpectDrawnAsync(TimeSpan.FromMinutes(2));
        await step.CaptureAsync(olivia, "drawn-again", "Prometheus is back: after Try again, every chart draws again");
        Coverage.Hit("route:/telemetry/{p1}");
        Coverage.Hit("subroute:telemetry/throughput");
        Coverage.Hit("use-case:telemetry/outage");
    }

    /// <summary>
    /// C2.6: A6 settled factory-telemetry's Full schedule to an hour. olivia tightens it to a
    /// minute on the Schedules page so its next run comes inside the step, sees its last success
    /// move past the outages, and settles it back to an hour - in a <c>finally</c>, through the
    /// API when the page cannot, so a failure never leaves a one-minute schedule running.
    /// </summary>
    private static async Task ScheduleRecoversAsync(JourneyEstate estate, PersonaSession olivia, C2Step step, DateTimeOffset outagesOver)
    {
        var backup = ContractFacades.Of(estate).Backup;
        var schedules = new BackupSchedulesView(olivia);
        await schedules.OpenAsync(Trees.FactoryTelemetry);
        var before = await schedules.FullCellsAsync();
        step.Log("before: " + string.Join(" | ", before));
        await step.CaptureAsync(olivia, "before", "factory-telemetry's Full schedule as A6 left it: every hour");

        var settled = false;
        try
        {
            await schedules.ScheduleFullAsync(NextRunInterval);
            await step.CaptureAsync(olivia, "tightened", "olivia tightens the Full schedule to a minute, so its next run comes now");

            DateTimeOffset? lastSuccess = null;
            await ContractWait.UntilAsync("the telemetry schedule's next success after the outages", async () =>
            {
                lastSuccess = (await backup.GetScopeStatusAsync(A6Contract.TelemetryScope))?.LastFullSuccessUtc;
                return lastSuccess > outagesOver;
            }, TimeSpan.FromMinutes(5));
            step.Log($"the schedule succeeded at {lastSuccess:O}, after the outages ended at {outagesOver:O}");

            await schedules.ShowAsync(Trees.FactoryTelemetry);
            var after = await schedules.FullCellsAsync();
            step.Log("after: " + string.Join(" | ", after));
            Assert.That(after[LastSuccessColumn], Is.Not.EqualTo(before[LastSuccessColumn]), "the page shows the schedule's new last success");
            await step.CaptureAsync(olivia, "succeeded", "The schedule's next run after the outages succeeded: its last success has moved on");

            await schedules.ScheduleFullAsync(A6Contract.TelemetrySteadyInterval);
            settled = true;
            await step.CaptureAsync(olivia, "settled", "olivia settles the Full schedule back to every hour");
            Coverage.Hit("route:/backups/schedules");
        }
        finally
        {
            if (!settled)
            {
                await backup.ScheduleBackupAsync(A6Contract.TelemetryScope, incremental: false, A6Contract.TelemetrySteadyInterval);
                step.Log("settled the schedule back to an hour through the API after the step failed");
            }
        }
    }

    /// <summary>The Schedules table's Last success column: Kind, Schedule, Interval, Last run, Last success.</summary>
    private const int LastSuccessColumn = 4;
}
