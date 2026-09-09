using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What <see cref="RepoContextExactScanBudget"/> concluded for one query. The
/// distinction between the three non-skipping values is the whole point of the
/// enumeration: they all clear the gather, but for reasons an operator must be
/// able to tell apart.
/// </summary>
internal enum RepoContextExactScanBudgetDecision
{
    /// <summary>
    /// The tree's configuration disables the bound, so the budget cannot skip
    /// anything however large the corpus is. The guard is present but inert by
    /// configuration.
    /// </summary>
    Unbounded = 0,

    /// <summary>
    /// The bound applies, but no corpus count exists yet, so the budget fails
    /// open. This is the guard being asked and declining to act, which reads
    /// identically to never being asked unless it is recorded. See issue #2231.
    /// </summary>
    CorpusUnknown = 1,

    /// <summary>The corpus is counted and fits inside the budget.</summary>
    WithinBudget = 2,

    /// <summary>
    /// The corpus is counted and does not fit, so the gather was skipped. This is
    /// the only value under which the budget actually suppresses work.
    /// </summary>
    Exceeded = 3,
}

/// <summary>
/// A point-in-time reading of one repository's retrieval-ladder guard counters,
/// cumulative since process start.
/// </summary>
/// <param name="Searches">Semantic searches this index served for the repository.</param>
/// <param name="PlaneServed">Searches the approximate plane answered, so neither guard was consulted.</param>
/// <param name="PlaneExhaustive">
/// Of <paramref name="PlaneServed"/>, those answered by exhaustive scan of the vectors the
/// plane holds, because it has no trained partitioning yet.
/// </param>
/// <param name="PlaneApproximate">
/// Of <paramref name="PlaneServed"/>, those answered from the plane's trained partitioning.
/// This is the only counter that rises when the approximate index is doing the job it
/// exists to do, and issue #2252 was filed because nothing reported it.
/// </param>
/// <param name="BudgetUnbounded">Budget evaluations that found no bound configured.</param>
/// <param name="BudgetCorpusUnknown">Budget evaluations that declined because the corpus was uncounted.</param>
/// <param name="BudgetWithinBudget">Budget evaluations that cleared a counted corpus.</param>
/// <param name="BudgetExceeded">Budget evaluations that skipped the gather as unaffordable.</param>
/// <param name="LastCorpus">The corpus size the most recent budget evaluation read.</param>
/// <param name="LastAffordable">The affordable vector count the most recent budget evaluation read.</param>
/// <param name="BreakerTrips">Gathers that stalled and opened the breaker.</param>
/// <param name="BreakerRepeatSkips">Gathers suppressed because the breaker was already open.</param>
/// <param name="BreakerResets">Times a serving plane closed an open breaker.</param>
/// <param name="BreakerProbes">Half-open probes the breaker granted, each of which ran a gather it would otherwise have suppressed.</param>
/// <param name="BreakerProbeRecoveries">Half-open probes that completed and closed the breaker without the plane ever serving.</param>
internal readonly record struct RepoContextRetrievalGuardSnapshot(
    long Searches,
    long PlaneServed,
    long PlaneExhaustive,
    long PlaneApproximate,
    long BudgetUnbounded,
    long BudgetCorpusUnknown,
    long BudgetWithinBudget,
    long BudgetExceeded,
    int LastCorpus,
    int LastAffordable,
    long BreakerTrips,
    long BreakerRepeatSkips,
    long BreakerResets,
    long BreakerProbes = 0,
    long BreakerProbeRecoveries = 0)
{
    /// <summary>
    /// How many times the budget was actually asked. A zero here and a zero in
    /// <see cref="BudgetExceeded"/> mean different things, and keeping them apart
    /// is the reason this type exists: zero evaluations is a guard that was never
    /// reached, whereas evaluations with no skips is a guard that was reached and
    /// declined.
    /// </summary>
    public long BudgetEvaluations
        => BudgetUnbounded + BudgetCorpusUnknown + BudgetWithinBudget + BudgetExceeded;

    /// <summary>
    /// Searches that fell through to the fallback ladder, where the guards live.
    /// </summary>
    public long Bootstrapping => Searches - PlaneServed;

    /// <summary>Whether anything at all has been recorded for the repository.</summary>
    public bool IsEmpty => Searches == 0;
}

/// <summary>
/// Counts what the two retrieval-ladder guards actually did, per repository, and
/// paces a periodic summary of it so a container running at information level can
/// be read without a debug-level restart.
/// <para>
/// <b>The defect this exists to remove.</b> A guard whose steady state is
/// invisible cannot be verified, because its silence is indistinguishable from
/// its absence. Both guards on this ladder had that shape:
/// <see cref="RepoContextExactScanBudget"/> logged only when it skipped a gather,
/// so an operator could not tell a budget that had never been reached from one
/// that was reached every query and declined; and
/// <see cref="RepoContextExactScanBreaker"/> logged its repeat-skip path at debug,
/// so the container's information-level output was silent whether the breaker was
/// holding or had done nothing at all. Those two zeros have opposite evidential
/// status - one is evidence of absence, the other absence of evidence - and
/// collapsing them is what produced issue #2253.
/// </para>
/// <para>
/// <b>Announce each distinct decision once, then summarise.</b> The repeat-skip
/// and the budget skip both run on every query once they engage, so logging them
/// per query at an operator-visible level would trade one unreadable state for a
/// flood. Instead every distinct decision is announced once per repository at
/// information level - which is what makes a path's first execution visible, the
/// breaker's reset in particular - and the steady state is carried by a summary
/// emitted no more than once per <see cref="SummaryInterval"/>. A counter that
/// reads zero after a non-zero number of evaluations is therefore a positive
/// statement, not a silence.
/// </para>
/// <para>
/// <b>Cumulative, not windowed.</b> The counters run from process start, so the
/// question the issue actually asks - "did this guard ever act?" - is answered by
/// one line rather than by correlating a window against an uptime. Consecutive
/// summaries differ to give the rate.
/// </para>
/// <para>
/// <b>The plane's own answer is partitioned, not merely counted.</b> Issue #2252
/// asks a question the aggregate <see cref="RepoContextRetrievalGuardSnapshot.PlaneServed"/>
/// cannot answer: the approximate plane serving at all
/// (<see cref="RepoContextAnnServingState.Exhaustive"/>) and the approximate plane
/// serving <i>from its trained partitioning</i>
/// (<see cref="RepoContextAnnServingState.Approximate"/>) are different facts, and
/// only the second means the index is doing the job it exists to do. The state was
/// computed per query and then discarded one frame up, which made the trained path
/// unobservable - and an unobservable path is indistinguishable from a dead one.
/// Every plane outcome is now counted under its own state and published on
/// <see cref="AnnSearchInstrumentName"/>.
/// </para>
/// <para>
/// <b>Why a zero on that instrument is evidence rather than silence.</b> A counter
/// that only rose when the trained path served would read a structural zero at the
/// highest rate of the very hazard it is meant to catch, which manufactures false
/// reassurance rather than removing it. This instrument instead <b>partitions every
/// plane outcome</b>, <see cref="RepoContextAnnServingState.Bootstrapping"/>
/// included, so the total is independently non-zero whenever queries are arriving.
/// A reading of <c>state=bootstrapping</c> climbing while <c>state=approximate</c>
/// stays at zero is therefore a positive statement - "queries are being served and
/// none of them by the trained plane" - and is loudest exactly when the hazard is
/// occurring. The one state it cannot distinguish is no traffic at all, where every
/// series is legitimately zero; the readiness probe on
/// <see cref="RepoContextRetrievalReadinessState"/> covers that case, because it
/// converges without waiting for a query.
/// </para>
/// </summary>
internal sealed class RepoContextRetrievalGuardReporter : IDisposable
{
    /// <summary>
    /// The counter name partitioning every approximate-plane outcome by the state
    /// that answered it. Tagged by <see cref="StateTagKey"/>.
    /// </summary>
    internal const string AnnSearchInstrumentName = "repocontext.retrieval.ann.search";

    /// <summary>The low-cardinality tag key carrying the serving state that answered a query.</summary>
    internal const string StateTagKey = "state";

    /// <summary>Tag value for a query the plane could not answer, so the fallback ladder ran.</summary>
    internal const string StateBootstrappingTag = "bootstrapping";

    /// <summary>Tag value for a query the plane answered by exhaustive scan of the vectors it holds.</summary>
    internal const string StateExhaustiveTag = "exhaustive";

    /// <summary>Tag value for a query the plane answered from its trained partitioning.</summary>
    internal const string StateApproximateTag = "approximate";

    /// <summary>
    /// How often a repository's summary may be emitted. One line per minute per
    /// repository is small against the query volume that produces it, and is
    /// frequent enough that an operator watching a live container sees the state
    /// without waiting.
    /// </summary>
    internal static readonly TimeSpan DefaultSummaryInterval = TimeSpan.FromMinutes(1);

    private readonly ConcurrentDictionary<string, RepoCounters> _byRepo =
        new(StringComparer.Ordinal);

    private readonly TimeProvider _time;

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering the two throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _annSearches;

    /// <summary>Creates the reporter.</summary>
    /// <param name="timeProvider">
    /// The clock the summary cadence is paced against. Defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <param name="summaryInterval">
    /// The minimum spacing between summaries for one repository. Defaults to
    /// <see cref="DefaultSummaryInterval"/>. <see cref="TimeSpan.Zero"/> emits one
    /// per search, which is what a test wants and no deployment does.
    /// </param>
    public RepoContextRetrievalGuardReporter(
        TimeProvider? timeProvider = null, TimeSpan? summaryInterval = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        var interval = summaryInterval ?? DefaultSummaryInterval;
        SummaryInterval = interval < TimeSpan.Zero ? TimeSpan.Zero : interval;

        // Published under the same meter name as the rest of the repocontext surface
        // so a single scraper subscription covers it.
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _annSearches = _meter.CreateCounter<long>(
            AnnSearchInstrumentName,
            unit: "{query}",
            description:
                "Semantic searches partitioned by the approximate-plane state that answered them: "
                + "'bootstrapping' (the plane could not answer and the fallback ladder ran), 'exhaustive' "
                + "(the plane answered by scanning the vectors it holds), or 'approximate' (the plane "
                + "answered from its trained partitioning). Because every outcome is counted, a zero on one "
                + "state alongside a non-zero total is a measured absence rather than an absent measurement.");
    }

    /// <summary>The minimum spacing between summaries for one repository.</summary>
    public TimeSpan SummaryInterval { get; }

    /// <summary>Records that a search was served for a repository.</summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void RecordSearch(string repoId) => Counters(repoId).Searches();

    /// <summary>
    /// Records which state the approximate plane answered a query from, for every
    /// query including the ones it could not answer. Recording the whole partition
    /// rather than only the serving half is what makes a zero on any one state
    /// readable: it is denominated by a total that rises with traffic.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="state">The state that answered.</param>
    /// <returns>
    /// <see langword="true"/> when this repository has not reached this state before,
    /// so the caller announces it once at an operator-visible level. The first
    /// <see cref="RepoContextAnnServingState.Approximate"/> outcome is the transition
    /// issue #2252 exists to make visible.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool RecordPlaneOutcome(string repoId, RepoContextAnnServingState state)
    {
        var first = Counters(repoId).PlaneOutcome(state);
        _annSearches.Add(
            1,
            new KeyValuePair<string, object?>(StateTagKey, DescribeState(state)),
            LatticeTenantLabel.Platform);
        return first;
    }

    /// <summary>
    /// The bounded tag value for a serving state. Resolved against a closed set so an
    /// unrecognised value can never reach the meter as unbounded-cardinality text.
    /// </summary>
    /// <param name="state">The state to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeState(RepoContextAnnServingState state) => state switch
    {
        RepoContextAnnServingState.Approximate => StateApproximateTag,
        RepoContextAnnServingState.Exhaustive => StateExhaustiveTag,
        _ => StateBootstrappingTag,
    };

    /// <summary>Records one exact-scan budget evaluation.</summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="decision">What the budget concluded.</param>
    /// <param name="corpus">The corpus size the evaluation read.</param>
    /// <param name="affordable">The affordable vector count the evaluation read.</param>
    /// <returns>
    /// <see langword="true"/> when this repository has not reported this decision
    /// before, so the caller announces it once at an operator-visible level.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool RecordBudgetDecision(
        string repoId, RepoContextExactScanBudgetDecision decision, int corpus, int affordable)
        => Counters(repoId).Budget(decision, corpus, affordable);

    /// <summary>Records that a stalled gather opened the breaker.</summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void RecordBreakerTrip(string repoId) => Counters(repoId).BreakerTrip();

    /// <summary>
    /// Records that an open breaker suppressed a gather. This is the path that runs
    /// on every subsequent query and that logged only at debug before issue #2253.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> the first time this repository suppresses a gather, so
    /// the caller can prove the path executed at all without logging every query.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool RecordBreakerRepeatSkip(string repoId) => Counters(repoId).BreakerRepeatSkip();

    /// <summary>Records that a serving plane closed an open breaker.</summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void RecordBreakerReset(string repoId) => Counters(repoId).BreakerReset();

    /// <summary>
    /// Records that the breaker granted a half-open probe, running a gather it
    /// would otherwise have suppressed. Counted separately from a repeat-skip
    /// because the two are the guard's opposite verdicts on the same query, and a
    /// probe count that stays at zero while trips climb is the readable signature
    /// of an exit that is not being taken.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> the first time this repository is probed, so the
    /// caller can prove the path executed at all without logging every window.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool RecordBreakerProbe(string repoId) => Counters(repoId).BreakerProbe();

    /// <summary>
    /// Records that a half-open probe completed and closed the breaker with no
    /// help from the plane. This is the transition issue #2362 exists to make
    /// reachable, so it is counted apart from
    /// <see cref="RecordBreakerReset(string)"/>: the two closures are evidence
    /// from different subsystems, and merging them would hide which one recovered.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void RecordBreakerProbeRecovery(string repoId) => Counters(repoId).BreakerProbeRecovery();

    /// <summary>
    /// Records that a repository's breaker has been open across enough consecutive
    /// stalls, with the plane never once serving, to be called wedged rather than
    /// merely contended.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> the first time, so the single-line diagnosis is
    /// emitted once per process rather than on every query that re-observes it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool RecordBreakerStuck(string repoId) => Counters(repoId).BreakerStuck();

    /// <summary>
    /// Reads a repository's counters without disturbing the summary cadence. A
    /// repository nothing has been recorded for reads as all zeros, which is the
    /// honest "never asked" answer rather than an absence.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public RepoContextRetrievalGuardSnapshot Snapshot(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _byRepo.TryGetValue(repoId, out var counters)
            ? counters.Read()
            : default;
    }

    /// <summary>
    /// Takes a repository's summary when one is due, and arms the next one. Returns
    /// <see langword="false"/> when the interval has not elapsed or nothing has been
    /// recorded, so a quiet repository emits nothing rather than repeating itself.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="snapshot">The counters when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the caller should emit a summary.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool TryTakeSummary(string repoId, out RepoContextRetrievalGuardSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        snapshot = default;
        if (!_byRepo.TryGetValue(repoId, out var counters))
        {
            return false;
        }

        if (!counters.TryTakeSummarySlot(_time.GetUtcNow(), SummaryInterval))
        {
            return false;
        }

        snapshot = counters.Read();
        return !snapshot.IsEmpty;
    }

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();

    private RepoCounters Counters(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _byRepo.GetOrAdd(repoId, static _ => new RepoCounters());
    }

    /// <summary>
    /// One repository's counters. Every field is written with an interlocked
    /// operation because searches for a repository run concurrently, and a summary
    /// is a report rather than a ledger, so a torn read across two counters is
    /// acceptable where a lost increment would not be.
    /// </summary>
    private sealed class RepoCounters
    {
        private long _searches;
        private long _planeServed;
        private long _planeExhaustive;
        private long _planeApproximate;
        private long _budgetUnbounded;
        private long _budgetCorpusUnknown;
        private long _budgetWithinBudget;
        private long _budgetExceeded;
        private long _breakerTrips;
        private long _breakerRepeatSkips;
        private long _breakerResets;
        private long _breakerProbes;
        private long _breakerProbeRecoveries;
        private int _lastCorpus;
        private int _lastAffordable;

        // A bitmask of the decisions already announced, so the first of each kind
        // can be logged once without a per-decision flag or a lock.
        private int _announced;

        // The earliest UTC tick a summary may next be emitted at. Zero means "now",
        // so the first search after a repository is first seen reports immediately
        // rather than waiting out an interval it was never in.
        private long _nextSummaryTicks;

        public void Searches() => Interlocked.Increment(ref _searches);

        public bool PlaneOutcome(RepoContextAnnServingState state)
        {
            switch (state)
            {
                case RepoContextAnnServingState.Approximate:
                    Interlocked.Increment(ref _planeApproximate);
                    Interlocked.Increment(ref _planeServed);
                    break;
                case RepoContextAnnServingState.Exhaustive:
                    Interlocked.Increment(ref _planeExhaustive);
                    Interlocked.Increment(ref _planeServed);
                    break;
                default:
                    // Bootstrapping is counted on the instrument but not into
                    // PlaneServed: the plane did not answer, so the fallback ladder
                    // ran and the snapshot's Bootstrapping figure must keep meaning
                    // "reached the guards".
                    break;
            }

            // Bits 16-18, clear of the budget decisions (0-3) and the breaker
            // repeat-skip (8) that already use this mask.
            return Announce(1 << (16 + (int)state));
        }

        public bool Budget(RepoContextExactScanBudgetDecision decision, int corpus, int affordable)
        {
            Interlocked.Exchange(ref _lastCorpus, corpus);
            Interlocked.Exchange(ref _lastAffordable, affordable);
            switch (decision)
            {
                case RepoContextExactScanBudgetDecision.Unbounded:
                    Interlocked.Increment(ref _budgetUnbounded);
                    break;
                case RepoContextExactScanBudgetDecision.CorpusUnknown:
                    Interlocked.Increment(ref _budgetCorpusUnknown);
                    break;
                case RepoContextExactScanBudgetDecision.WithinBudget:
                    Interlocked.Increment(ref _budgetWithinBudget);
                    break;
                default:
                    Interlocked.Increment(ref _budgetExceeded);
                    break;
            }

            return Announce(1 << (int)decision);
        }

        public void BreakerTrip() => Interlocked.Increment(ref _breakerTrips);

        public bool BreakerRepeatSkip()
        {
            Interlocked.Increment(ref _breakerRepeatSkips);
            return Announce(1 << 8);
        }

        public void BreakerReset() => Interlocked.Increment(ref _breakerResets);

        public bool BreakerProbe()
        {
            Interlocked.Increment(ref _breakerProbes);

            // Bits 9 and 10, clear of the budget decisions (0-3), the breaker
            // repeat-skip (8) and the plane outcomes (16-18) already on this mask.
            return Announce(1 << 9);
        }

        public void BreakerProbeRecovery() => Interlocked.Increment(ref _breakerProbeRecoveries);

        public bool BreakerStuck() => Announce(1 << 10);

        public RepoContextRetrievalGuardSnapshot Read() => new(
            Interlocked.Read(ref _searches),
            Interlocked.Read(ref _planeServed),
            Interlocked.Read(ref _planeExhaustive),
            Interlocked.Read(ref _planeApproximate),
            Interlocked.Read(ref _budgetUnbounded),
            Interlocked.Read(ref _budgetCorpusUnknown),
            Interlocked.Read(ref _budgetWithinBudget),
            Interlocked.Read(ref _budgetExceeded),
            Volatile.Read(ref _lastCorpus),
            Volatile.Read(ref _lastAffordable),
            Interlocked.Read(ref _breakerTrips),
            Interlocked.Read(ref _breakerRepeatSkips),
            Interlocked.Read(ref _breakerResets),
            Interlocked.Read(ref _breakerProbes),
            Interlocked.Read(ref _breakerProbeRecoveries));

        public bool TryTakeSummarySlot(DateTimeOffset now, TimeSpan interval)
        {
            var nowTicks = now.UtcTicks;
            while (true)
            {
                var next = Interlocked.Read(ref _nextSummaryTicks);
                if (nowTicks < next)
                {
                    return false;
                }

                var armed = nowTicks + interval.Ticks;
                if (Interlocked.CompareExchange(ref _nextSummaryTicks, armed, next) == next)
                {
                    return true;
                }
            }
        }

        private bool Announce(int bit)
        {
            while (true)
            {
                var seen = Volatile.Read(ref _announced);
                if ((seen & bit) != 0)
                {
                    return false;
                }

                if (Interlocked.CompareExchange(ref _announced, seen | bit, seen) == seen)
                {
                    return true;
                }
            }
        }
    }
}
