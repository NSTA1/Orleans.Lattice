using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The startup sweep that arms an approximate-index build coordinator for every
/// registered repository, and re-sweeps periodically so a repository added later
/// is picked up too.
/// <para>
/// <b>This is the part that actually fixes cold start.</b> Scheduling the build on
/// a durable coordinator makes it crash-safe, but a coordinator nobody arms is
/// still a build nobody starts - which was the original defect in a different
/// costume. The sweep removes the last dependence on traffic: a restored volume
/// with no client at all converges to a serving index, and the first query that
/// does arrive finds one already built rather than being the thing that triggers
/// the build and then paying for it.
/// </para>
/// <para>
/// Arming is idempotent, so re-sweeping costs a reminder re-registration per
/// repository. The Orleans silo is itself a hosted service, so a grain call from a
/// hosted service's start can race ahead of the silo becoming dispatch-ready; the
/// sweep therefore retries with backoff until it gets through or the host stops.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexSweepService(
    RepoContextStore store,
    RepoContextAnnIndexScheduler scheduler,
    RepoContextIndexingOptions options,
    ILogger<RepoContextAnnIndexSweepService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The floor on the re-sweep cadence. The sweep follows the reconcile interval
    /// so it stays in step with the pass that produces the vectors it schedules an
    /// index over, but a host that makes the reconcile near-continuous must not
    /// turn this into a hot loop of grain calls.
    /// </summary>
    private static readonly TimeSpan MinimumSweepInterval = TimeSpan.FromMinutes(1);

    private readonly RepoContextAnnIndexSweepReporter _reporter = new();

    /// <summary>
    /// The sweep's outcome counters, cumulative since process start. Exposed so a
    /// test can assert on the partition without standing up a meter listener.
    /// </summary>
    internal RepoContextAnnIndexSweepReporter Reporter => _reporter;

    /// <inheritdoc />
    public override void Dispose()
    {
        _reporter.Dispose();
        base.Dispose();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = options.ReconcileInterval > MinimumSweepInterval
            ? options.ReconcileInterval
            : MinimumSweepInterval;

        // Unconditional, and deliberately ahead of every branch. This is the only
        // signal that can separate "the sweep loop never started" from "it started
        // and has armed nothing yet": a counter cannot, because a loop that never
        // runs emits no measurements, so all of its series read zero exactly as they
        // do on a host that has only just come up. Emitting the line before the
        // branch rather than inside one also makes it structurally impossible for a
        // later edit to add a path that returns silently.
        logger.LogInformation(
            "Repository-context approximate-index build sweep entered. Scheduling is {SchedulingDecision}. "
            + "Configured sweep cadence {SweepInterval}. Outcomes are counted onto '{Instrument}'; the absence "
            + "of this line from a host's log means the sweep service never executed.",
            scheduler.DescribeSchedulingState(),
            interval,
            RepoContextAnnIndexSweepReporter.SweepInstrumentName);

        if (!scheduler.CanSchedule)
        {
            return;
        }

        var delay = InitialRetryDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            var outcome = await TrySweepAsync(stoppingToken).ConfigureAwait(false);
            if (outcome is null)
            {
                // Shutdown cancelled the sweep. Not an outcome, so nothing is
                // recorded: counting it would put a phantom success on the series.
                return;
            }

            // A failed sweep backs off and retries promptly, because until it gets
            // through nothing is scheduled at all. A completed one waits a full
            // interval, since re-arming a coordinator that is already running buys
            // nothing - and that includes a sweep that found nothing to arm, whose
            // remedy is a repository being registered, not a faster retry.
            var faulted = outcome == RepoContextAnnSweepOutcome.Faulted;
            var wait = faulted ? delay : interval;
            if (faulted)
            {
                delay = delay < MaxRetryDelay
                    ? TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks))
                    : MaxRetryDelay;
            }
            else
            {
                delay = InitialRetryDelay;
            }

            try
            {
                await Task.Delay(wait, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Runs one sweep, records its outcome, and announces the transitions worth a
    /// log line.
    /// <para>
    /// The fault arm used to log at debug and return a bare <see langword="false"/>.
    /// A deployment running at information level therefore emitted nothing at all
    /// for a sweep that threw on every attempt, forever - and nothing for a sweep
    /// that worked, and nothing for a sweep that never ran, which is three states
    /// behind one observation. Raising it to warning on the first fault of each run
    /// and counting the repetitions keeps the fault visible without writing a line
    /// every thirty seconds for as long as it lasts.
    /// </para>
    /// </summary>
    /// <returns>
    /// The outcome, or <see langword="null"/> when shutdown cancelled the sweep -
    /// which is not an outcome and is deliberately not recorded.
    /// </returns>
    private async Task<RepoContextAnnSweepOutcome?> TrySweepAsync(CancellationToken stoppingToken)
    {
        var armed = 0;
        try
        {
            // Only the ids are needed to arm a coordinator, so this deliberately
            // avoids ListReposAsync: a full summary reads a root marker per repository
            // and can schedule an out-of-band membership walk, none of which a sweep
            // uses.
            var repoIds = await store.ListRepoIdsAsync(stoppingToken).ConfigureAwait(false);
            foreach (var repoId in repoIds)
            {
                stoppingToken.ThrowIfCancellationRequested();
                if (await scheduler.TryArmAsync(repoId, stoppingToken).ConfigureAwait(false))
                {
                    armed++;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception ex)
        {
            Announce(RepoContextAnnSweepOutcome.Faulted, armed, ex);
            return RepoContextAnnSweepOutcome.Faulted;
        }

        var outcome = armed > 0 ? RepoContextAnnSweepOutcome.Armed : RepoContextAnnSweepOutcome.Empty;
        Announce(outcome, armed, exception: null);
        return outcome;
    }

    /// <summary>Records one outcome and writes the log line its transition warrants.</summary>
    private void Announce(RepoContextAnnSweepOutcome outcome, int armed, Exception? exception)
    {
        var report = _reporter.Record(outcome);
        switch (report.Announcement)
        {
            case RepoContextAnnSweepAnnouncement.FaultBegan:
                logger.LogWarning(
                    exception,
                    "Repository-context approximate-index sweep failed to arm the build coordinators; retrying "
                    + "with backoff up to {MaxRetryDelay}. Until a sweep gets through, no build is scheduled and "
                    + "semantic search cannot leave its bootstrapping fallback. Repeats of this fault are counted "
                    + "onto '{Instrument}' with outcome '{Outcome}' rather than logged per attempt.",
                    MaxRetryDelay,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName,
                    RepoContextAnnIndexSweepReporter.OutcomeFaultedTag);
                break;

            case RepoContextAnnSweepAnnouncement.Recovered:
                logger.LogInformation(
                    "Repository-context approximate-index sweep recovered after {ConsecutiveFaults} consecutive "
                    + "failed attempt(s) and armed {ArmedCount} build coordinator(s).",
                    report.ConsecutiveFaults,
                    armed);
                break;

            case RepoContextAnnSweepAnnouncement.FirstArmed:
                logger.LogInformation(
                    "Repository-context approximate-index sweep armed {ArmedCount} build coordinator(s) for the "
                    + "first time in this process. Later sweeps are counted onto '{Instrument}' rather than logged.",
                    armed,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName);
                break;

            case RepoContextAnnSweepAnnouncement.NoRepositories:
                logger.LogInformation(
                    "Repository-context approximate-index sweep completed with no repository to arm, so no build "
                    + "is scheduled. This is a successful sweep with nothing to do rather than a failure, and it "
                    + "reads identically to a working one in every signal except this line and the '{Outcome}' "
                    + "arm of '{Instrument}'.",
                    RepoContextAnnIndexSweepReporter.OutcomeEmptyTag,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName);
                break;

            default:
                break;
        }
    }
}
