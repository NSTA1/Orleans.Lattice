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
    RepoContextRetrievalReadinessState readiness,
    IRepoIndexRunAuthority runAuthority,
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
    /// Whether the readiness contradiction has already been announced for the
    /// episode in progress. Re-armed the moment the contradiction clears, so a
    /// recurrence is reported rather than silently absorbed.
    /// <para>
    /// Not synchronised, and does not need to be: every write goes through
    /// <see cref="Announce"/>, which is only ever reached from the single
    /// <see cref="ExecuteAsync"/> loop.
    /// </para>
    /// </summary>
    private bool _announcedContradiction;

    /// <summary>
    /// The repositories whose arming call has already been reported as deferred, so
    /// a build that legitimately runs for hours is announced once rather than on
    /// every sweep.
    /// <para>
    /// Not synchronised, for the same reason as
    /// <see cref="_announcedContradiction"/>: every access is on the single
    /// <see cref="ExecuteAsync"/> loop.
    /// </para>
    /// </summary>
    private readonly HashSet<string> _deferred = new(StringComparer.Ordinal);

    /// <summary>
    /// The repositories already named by the partial-sweep warning, so a repository
    /// that stays unarmed across many sweeps is announced once rather than on every
    /// pass. Cleared per repository the moment it arms, so a recurrence is announced
    /// again instead of being absorbed by the announcement it already made.
    /// <para>
    /// Deliberately separate from <see cref="_deferred"/> rather than reusing it,
    /// even though the two move together on most sweeps. They track different
    /// predicates and re-arm at different moments: a sweep where <b>every</b>
    /// coordinator defers is <see cref="RepoContextAnnSweepOutcome.Empty"/>, not a
    /// partial one, and it populates <see cref="_deferred"/> without ever being
    /// partial. Reusing that set would let the first genuinely partial sweep find
    /// every repository already recorded and announce nothing at all - which is the
    /// exact silence this change exists to remove.
    /// </para>
    /// <para>
    /// Not synchronised, for the same reason as <see cref="_announcedContradiction"/>:
    /// every access is on the single <see cref="ExecuteAsync"/> loop.
    /// </para>
    /// </summary>
    private readonly HashSet<string> _unarmed = new(StringComparer.Ordinal);

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
    /// <remarks>
    /// Internal rather than private so a test can drive individual passes. The loop
    /// waits a full <see cref="MinimumSweepInterval"/> after any non-faulted sweep,
    /// so the damping and re-announcement of the partial-sweep warning - both of
    /// which are defined across successive passes - are not reachable through
    /// <see cref="ExecuteAsync"/> inside a test's time budget. The type is itself
    /// internal, so this widens no public surface.
    /// </remarks>
    internal async Task<RepoContextAnnSweepOutcome?> TrySweepAsync(CancellationToken stoppingToken)
    {
        var armed = 0;
        var observed = 0;
        var deferred = 0;

        // The identities behind the counts. The 'armed' outcome tag can report that
        // at least one coordinator armed and nothing more, so a sweep that arms four
        // repositories out of five is indistinguishable on the counter from one that
        // arms all five. Attribution has to travel to the announcement seam or it is
        // lost: the loop is the only place that knows WHICH repository was left out.
        // Identity belongs in the log rather than on the instrument - see the durable
        // decision 'no-repo-tag-on-pass-arm-faults' and issue #2453.
        var unarmed = new List<string>();
        Exception? faulted = null;
        try
        {
            // Stamp the run authority's fixed identity onto the whole sweep, so both
            // the listing scan below and the arming calls it drives carry a subject
            // the access gate can authorize.
            //
            // Without this the sweep is anonymous, because a BackgroundService loop
            // is not a request and carries no ambient credential. On a host running a
            // default-deny gate that does NOT surface as an error: a denied range
            // read is enforced by ResolveRangeReadFilterAsync as a reject-all key
            // filter (`static _ => false`), not an exception - so the scan returns an
            // EMPTY list, cleanly, and the sweep reports "nothing to arm" on every
            // pass forever while every credentialed caller in the same process sees
            // the full set. The index is then never built and every semantic query
            // falls back to an exact brute-force scan.
            //
            // This is the same remedy RepoIndexRunner, RepoContextSelfIndexGrain, and
            // RepoContextGitSourceArmingService already apply for exactly this
            // reason; the sweep was the one background arming component that omitted
            // it. A host that registers no authority resolves null and the sweep's
            // ambient credential is left untouched, so an in-process host with no
            // access gate is unaffected. See issue #2406.
            var credential = runAuthority.Resolve();
            using var credentialScope = credential is null
                ? null
                : LatticeCredentialContext.With(credential);

            // Only the ids are needed to arm a coordinator, so this deliberately
            // avoids ListReposAsync: a full summary reads a root marker per repository
            // and can schedule an out-of-band membership walk, none of which a sweep
            // uses.
            var repoIds = await store.ListRepoIdsAsync(stoppingToken).ConfigureAwait(false);
            observed = repoIds.Count;
            foreach (var repoId in repoIds)
            {
                stoppingToken.ThrowIfCancellationRequested();

                // Per-repository, and deliberately so. Arming is a call into a
                // NON-REENTRANT build coordinator, so while that grain is inside a
                // long build turn the call waits behind it and expires on Orleans'
                // default call timeout. Before this catch existed that timeout
                // escaped the loop, which had two consequences that between them
                // account for the whole shape of issue #2252: every repository
                // ordered after the busy one was never visited at all, and the
                // sweep recorded 'faulted' - a failure signal - for a coordinator
                // that was in fact doing exactly the work it was armed to do.
                //
                // A build over a large corpus legitimately runs for hours. On this
                // box the coordinator for a 161,840-vector repository was measured
                // mid-ingest with every checkpoint cursor advancing between two
                // heap captures six minutes apart, while the sweep counted a fault
                // roughly twice a minute against it. So a timeout here is not
                // evidence of a broken coordinator and must not be reported as one;
                // it is the expected answer from a healthy one that is busy.
                try
                {
                    if (await scheduler.TryArmAsync(repoId, stoppingToken).ConfigureAwait(false))
                    {
                        armed++;
                        if (_deferred.Remove(repoId))
                        {
                            logger.LogInformation(
                                "Repo {RepoId}: approximate-index build coordinator answered the arming call again "
                                + "after previously deferring it.",
                                repoId);
                        }

                        // Re-arm the partial-sweep damping. A repository that arms
                        // now and stops arming later must be named again, or the
                        // second episode is silently absorbed by the first.
                        _unarmed.Remove(repoId);
                    }
                    else
                    {
                        // Reachable only through the scheduler's process-global
                        // guard (no embedder, or scheduling switched off), which
                        // ExecuteAsync already checks before entering this loop and
                        // which cannot be true for one repository and false for
                        // another. Recorded anyway so 'did not arm' is derived from
                        // what happened rather than from the assumption that a
                        // non-timeout is always a success.
                        unarmed.Add(repoId);
                    }
                }
                catch (TimeoutException ex)
                {
                    deferred++;
                    unarmed.Add(repoId);

                    // Announced once per repository per episode, then counted. A
                    // coordinator busy for hours would otherwise warn on every
                    // sweep for as long as it is doing useful work.
                    if (_deferred.Add(repoId))
                    {
                        logger.LogWarning(
                            ex,
                            "Repo {RepoId}: the approximate-index build coordinator did not answer the arming call "
                            + "within the grain call timeout, so this sweep is leaving it alone. This is the "
                            + "expected answer from a coordinator already inside a long build turn, and it is NOT "
                            + "counted as a fault. Arming is idempotent, so the next sweep retries it. Further "
                            + "deferrals for this repository are not logged until it answers again.",
                            repoId);
                    }
                    else
                    {
                        logger.LogDebug(
                            "Repo {RepoId}: approximate-index arming deferred again; the coordinator is still busy.",
                            repoId);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Any other failure IS a fault, but it is this repository's
                    // fault and not the sweep's. Record it, name the repository the
                    // counter cannot name, and keep going so a single bad
                    // repository cannot hide every repository behind it.
                    faulted ??= ex;
                    logger.LogWarning(
                        ex,
                        "Repo {RepoId}: arming the approximate-index build coordinator failed. The sweep is "
                        + "continuing to the remaining repositories and will report this fault once they have all "
                        + "been attempted.",
                        repoId);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception ex)
        {
            Announce(RepoContextAnnSweepOutcome.Faulted, armed, observed, deferred, unarmed, ex);
            return RepoContextAnnSweepOutcome.Faulted;
        }

        if (faulted is not null)
        {
            Announce(RepoContextAnnSweepOutcome.Faulted, armed, observed, deferred, unarmed, faulted);
            return RepoContextAnnSweepOutcome.Faulted;
        }

        var outcome = armed > 0 ? RepoContextAnnSweepOutcome.Armed : RepoContextAnnSweepOutcome.Empty;
        Announce(outcome, armed, observed, deferred, unarmed, exception: null);
        return outcome;
    }

    /// <summary>Records one outcome and writes the log line its transition warrants.</summary>
    private void Announce(
        RepoContextAnnSweepOutcome outcome,
        int armed,
        int observed,
        int deferred,
        IReadOnlyList<string> unarmed,
        Exception? exception)
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
                    + "first time in this process, out of {ObservedRepositoryCount} repository id(s) observed in "
                    + "the store listing, {DeferredRepositoryCount} of which did not answer the arming call within "
                    + "the grain call timeout. Read the three numbers together: an armed count equal to the "
                    + "observed count is a complete sweep, whereas a smaller one means some repository's "
                    + "approximate index is not being built. The '{Outcome}' arm of '{Instrument}' records only "
                    + "that at least one coordinator armed, so it cannot make that distinction and later sweeps "
                    + "are counted onto it rather than logged. A repository left out is named by its own warning.",
                    armed,
                    observed,
                    deferred,
                    RepoContextAnnIndexSweepReporter.OutcomeArmedTag,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName);
                break;

            case RepoContextAnnSweepAnnouncement.ArmedNothing:
                logger.LogInformation(
                    "Repository-context approximate-index sweep completed without arming anything, so no build is "
                    + "scheduled. It observed {ObservedRepositoryCount} repository id(s) in the store listing, and "
                    + "{DeferredRepositoryCount} of them did not answer the arming call within the grain call "
                    + "timeout. Read those two numbers together: equal and non-zero means every coordinator is busy "
                    + "in a build turn and nothing is wrong, whereas an observed count of zero is the empty-listing "
                    + "defect issue #2406 records. Reporting only the outcome collapses those two into one "
                    + "observation and they need opposite responses. This line reports what the sweep observed "
                    + "rather than what the store contains, because those two differ exactly when the listing is "
                    + "itself wrong. Repetitions are counted onto the '{Outcome}' arm of '{Instrument}' rather than "
                    + "logged.",
                    observed,
                    deferred,
                    RepoContextAnnIndexSweepReporter.OutcomeEmptyTag,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName);
                break;

            default:
                break;
        }

        // Deliberately outside the switch, because the switch is once-per-process
        // and this must not be. FirstArmed fires on the first armed sweep and never
        // again, so a repository that arms normally for an hour and then stops would
        // fall entirely inside the silent steady state - which is precisely the
        // failure this exists to make audible. Damped per repository instead of per
        // process, in the same shape as the deferral warning above.
        if (outcome == RepoContextAnnSweepOutcome.Armed && unarmed.Count > 0)
        {
            AnnouncePartialSweep(armed, observed, deferred, unarmed);
        }

        // Deliberately outside the switch, and deliberately not gated on the
        // once-per-process announcement above: the retrieval plane usually reaches
        // 'serving' well AFTER the first sweep that armed nothing, so a contradiction
        // folded into that first-of-kind line would be unreachable in precisely the
        // case it exists to catch.
        if (outcome != RepoContextAnnSweepOutcome.Faulted)
        {
            AnnounceReadinessContradiction(observed);
        }
    }

    /// <summary>
    /// Names the repositories a sweep did not arm, on a sweep that armed something
    /// else.
    /// <para>
    /// <b>Why this is a log line and not a metric dimension.</b> The remedy for an
    /// aggregate that cannot be decomposed is attribution, and attribution here is
    /// an identity. Repositories are registered at runtime through
    /// <c>repocontext_add_repo</c>, so a repository dimension on the sweep counter
    /// would have no compile-time bound, and every instrument on this meter is
    /// documented as carrying low-cardinality tags and never a repository id. The
    /// standing rule is that identity dimensions belong in logs and outcome
    /// dimensions belong in metrics; see the durable decision
    /// <c>no-repo-tag-on-pass-arm-faults</c>. So the counter keeps its shape and the
    /// attribution goes here.
    /// </para>
    /// <para>
    /// <b>Why a partial sweep is worth a warning at all.</b> Nothing is permanently
    /// lost: arming is idempotent and the next sweep retries, so this is a
    /// diagnosability defect rather than a correctness one. The cost is entirely in
    /// what an operator can see. A repository whose coordinator never arms while its
    /// siblings arm normally produces a steadily rising <c>armed</c> counter, no
    /// warning, and a box that looks healthy, while that repository's approximate
    /// index is never built and its searches silently stay on the fallback path. The
    /// counter answers "did at least one coordinator arm" when the question an
    /// operator has is "did every coordinator arm".
    /// </para>
    /// <para>
    /// Announced once per repository per episode and re-armed the moment that
    /// repository arms, in the same shape as the deferral warning, so a coordinator
    /// legitimately busy for hours is named once rather than on every sweep.
    /// </para>
    /// </summary>
    private void AnnouncePartialSweep(int armed, int observed, int deferred, IReadOnlyList<string> unarmed)
    {
        foreach (var repoId in unarmed)
        {
            if (_unarmed.Add(repoId))
            {
                logger.LogWarning(
                    "Repo {RepoId}: the approximate-index build coordinator was not armed by a sweep that armed "
                    + "{ArmedCount} of the {ObservedRepositoryCount} repository id(s) it observed, "
                    + "{DeferredRepositoryCount} of which did not answer the arming call within the grain call "
                    + "timeout. The '{Outcome}' arm of '{Instrument}' records only that at least one coordinator "
                    + "armed, so a sweep that arms some repositories is indistinguishable there from one that arms "
                    + "all of them, and this line is the only surface that names which repository was left out. "
                    + "Arming is idempotent, so the next sweep retries it and nothing is permanently lost; a "
                    + "repository named here sweep after sweep is one whose approximate index is never built while "
                    + "the counter continues to read as success. Further partial sweeps are not logged for this "
                    + "repository until it arms again.",
                    repoId,
                    armed,
                    observed,
                    deferred,
                    RepoContextAnnIndexSweepReporter.OutcomeArmedTag,
                    RepoContextAnnIndexSweepReporter.SweepInstrumentName);
            }
            else
            {
                logger.LogDebug(
                    "Repo {RepoId}: still not armed by a partial sweep; already announced for this episode.",
                    repoId);
            }
        }
    }

    /// <summary>
    /// Compares the sweep's own observation against the retrieval plane's readiness,
    /// and warns when the two contradict each other.
    /// <para>
    /// A sweep that listed no repository while readiness reports
    /// <see cref="RepoContextRetrievalReadinessPhase.Serving"/> is not an ambiguity,
    /// it is a flat contradiction: that phase is reached only where a semantic
    /// retrieval demonstrably succeeded, which requires indexed content the listing
    /// says is not there. The host has held both halves of this comparison all along
    /// and has never made it, which is why a sweep counted onto the <c>empty</c> arm
    /// could be read as a successful no-op for 189 iterations while the store held
    /// two repositories.
    /// </para>
    /// <para>
    /// <see cref="RepoContextRetrievalReadinessState.Phase"/> applies the fault
    /// hold-down, so a plane proven serving with a fault episode open still reads
    /// <see cref="RepoContextRetrievalReadinessPhase.Serving"/> inside the window.
    /// That is the reading this check wants: the plane demonstrably served, and a
    /// transient fault does not make the listing's emptiness any less contradictory.
    /// </para>
    /// <para>
    /// Announced once per episode and re-armed when the contradiction clears, in the
    /// same shape as the fault-episode pacing above, so a condition that persists for
    /// hours does not write a line per sweep.
    /// </para>
    /// <para>
    /// Internal rather than private so a test can drive the episode across several
    /// passes - announce, hold, clear, announce again - without waiting out
    /// <see cref="MinimumSweepInterval"/> once per transition. The re-arm is the half
    /// that only matters at the <i>second</i> incident, so leaving it to a timing
    /// seam would leave it permanently unproven.
    /// </para>
    /// </summary>
    /// <param name="observed">How many repository ids this sweep's listing yielded.</param>
    internal void AnnounceReadinessContradiction(int observed)
    {
        var phase = readiness.Phase;
        if (observed != 0 || phase != RepoContextRetrievalReadinessPhase.Serving)
        {
            _announcedContradiction = false;
            return;
        }

        if (_announcedContradiction)
        {
            return;
        }

        _announcedContradiction = true;
        logger.LogWarning(
            "Repository-context approximate-index sweep listed no repository while retrieval readiness reports "
            + "'{ReadinessPhase}'. Those two observations contradict each other: readiness reaches that phase only "
            + "where a semantic retrieval demonstrably succeeded, which requires indexed content the listing says "
            + "is not there. One of the two is wrong, so a sweep counted onto the '{Outcome}' arm of "
            + "'{Instrument}' must not be read as a successful sweep with nothing to do until it is resolved. See "
            + "issue #2406.",
            phase,
            RepoContextAnnIndexSweepReporter.OutcomeEmptyTag,
            RepoContextAnnIndexSweepReporter.SweepInstrumentName);
    }
}
