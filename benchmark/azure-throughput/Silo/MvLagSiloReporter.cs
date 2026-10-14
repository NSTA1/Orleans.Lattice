// set-point-mv only: silo-side materialised-view apply-lag reporter.
//
// The view maintainer publishes orleans.lattice.view.apply_lag and
// orleans.lattice.view.backlog_depth on the silo that hosts its activation.
// On Layer 3 the ingest engine runs inside the Orleans-client producer, a
// separate process that never sees those instruments, so the producer-side
// ViewLagMeterProbe always reads its "no sample" sentinel there. This reporter
// runs the same passive probe inside every silo and prints the latest sample
// on a fixed cadence, so the Log Analytics harvest carries the lag from
// whichever silo hosts the maintainer. It adds no grain traffic.
//
//   [silo:mv-lag] t=20.0s view=bench-<treeId> applyLag=1234 backlog=56
//
// A silo that does not host the maintainer prints applyLag=-1 backlog=-1;
// readers take the series from the silo that reports samples.

using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// Periodically logs the materialised view's apply lag and backlog depth as
/// observed on this silo. Registered only for the <c>set-point-mv</c> workload.
/// </summary>
internal sealed class MvLagSiloReporter(string viewName) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var raw = Environment.GetEnvironmentVariable("BENCH_MV_LAG_REPORT_SEC");
        var reportSec = int.TryParse(raw, out var parsed) && parsed >= 0 ? parsed : 5;
        if (reportSec == 0)
        {
            Console.WriteLine("[silo:mv-lag] disabled (BENCH_MV_LAG_REPORT_SEC=0)");
            return;
        }

        using var probe = new ViewLagMeterProbe(viewName);
        var sw = Stopwatch.StartNew();
        Console.WriteLine($"[silo:mv-lag] reporting view={viewName} every {reportSec}s (silo-side; the producer process cannot observe the maintainer's instruments)");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(reportSec));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                Console.WriteLine($"[silo:mv-lag] t={sw.Elapsed.TotalSeconds:0.0}s view={viewName} applyLag={probe.LatestApplyLag} backlog={probe.LatestBacklogDepth}");
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }

        Console.WriteLine($"[silo:mv-lag] final t={sw.Elapsed.TotalSeconds:0.0}s view={viewName} applyLag={probe.LatestApplyLag} backlog={probe.LatestBacklogDepth}");
    }
}
