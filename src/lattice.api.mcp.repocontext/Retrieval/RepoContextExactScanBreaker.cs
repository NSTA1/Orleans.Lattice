using System.Collections.Concurrent;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Remembers, per repository, that an exact k-nearest-neighbour gather has
/// already proved it cannot finish, so the next query does not have to prove it
/// again - and, after a backing-off delay, lets exactly one query prove the
/// opposite.
/// <para>
/// <b>Why observing beats predicting.</b>
/// <see cref="RepoContextExactScanBudget"/> predicts affordability from a corpus
/// size, and a prediction is only as good as the count it reads. The count is
/// published by the approximate plane's build, so it is <c>0</c> for the whole
/// interval between process start and the build's first published progress -
/// which is exactly the interval a doomed gather has to be prevented in - and
/// even once published it covers a single embedding space while the gather scans
/// the repository's whole vector prefix. Both errors point the same way: the
/// number compared against the threshold is smaller than the quantity being
/// bounded, so the budget clears a scan it was written to skip. A breaker reads
/// no count at all. It reads the <see cref="ScanPageStalledException"/> the
/// gather actually threw, which is the failure itself rather than a proxy for it,
/// and no miscount can defeat it.
/// </para>
/// <para>
/// <b>The half-open probe, and why it is load-bearing.</b> Until issue #2362 the
/// only way out of a trip was the approximate plane answering for itself. That
/// reads as safe, and it is not: it makes the exit conditional on the very
/// subsystem whose absence caused the trip. On a host where the plane never
/// serves - because no build was ever scheduled, or because the build cannot
/// count a corpus the same stall is preventing it from reading - the trip is
/// permanent, and every surface above it reports that permanent state as the
/// transient one, "still building". So a trip now carries an expiry. After
/// <see cref="InitialProbeDelay"/>, and after a doubling delay for each further
/// stall up to <see cref="MaxProbeDelay"/>, <see cref="Evaluate"/> grants one
/// caller a <see cref="RepoContextExactScanBreakerDecision.Probe"/>. If that
/// gather completes, the breaker closes on its own evidence, with no build having
/// happened at all. If it stalls again the delay grows, so the cost of probing a
/// genuinely wedged repository decays towards one stall ceiling per
/// <see cref="MaxProbeDelay"/>.
/// </para>
/// <para>
/// <b>Only one probe per window.</b> The grant re-arms the delay as it is issued,
/// under the episode's own monitor, so concurrent queries during a probe window
/// still see <see cref="RepoContextExactScanBreakerDecision.Open"/>, and a probe
/// that never reports an outcome (a cancelled query, a dropped request) cannot
/// leave the breaker spending a full stall ceiling on every query.
/// </para>
/// <para>
/// <b>Keyed by repository, because the scan is.</b> The gather range-scans
/// <see cref="RepoContextKeys.VectorsPrefix(string)"/> and filters by embedding
/// space in memory, so every space in a repository walks identical rows and
/// stalls identically. Keying the breaker per <c>(repository, space)</c> would
/// make each space pay its own victim query for a fact the first one already
/// established.
/// </para>
/// <para>
/// <b>It only ever governs the fallback.</b> A trip is consulted solely while the
/// plane reports <see cref="RepoContextAnnServingState.Bootstrapping"/>, and is
/// cleared the moment the plane answers for itself - so a repository that starts
/// serving gets its exact fallback back with no delay to wait out.
/// </para>
/// </summary>
internal sealed class RepoContextExactScanBreaker
{
    /// <summary>
    /// How long after a stall the first half-open probe is allowed. Short enough
    /// that a stall caused by transient contention clears in about a minute, which
    /// is the common case: the gather competes with a build streaming the same
    /// tree, and that contention ends when the build does.
    /// </summary>
    public static readonly TimeSpan DefaultInitialProbeDelay = TimeSpan.FromSeconds(60);

    /// <summary>
    /// The ceiling the probe delay doubles up to. Against the shipped 25 second
    /// stall ceiling this bounds a permanently-wedged repository's probing cost at
    /// under three percent of one repository's scan capacity, which buys a
    /// guaranteed exit for a rounding error.
    /// </summary>
    public static readonly TimeSpan DefaultMaxProbeDelay = TimeSpan.FromMinutes(15);

    private readonly ConcurrentDictionary<string, Episode> _open = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    /// <summary>Creates the breaker.</summary>
    /// <param name="timeProvider">
    /// The clock the probe delay is measured against. Injected rather than read
    /// from a wall clock so the exit is deterministic and testable; defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <param name="initialProbeDelay">
    /// The delay before the first half-open probe; defaults to
    /// <see cref="DefaultInitialProbeDelay"/>. A non-positive value probes on the
    /// next query.
    /// </param>
    /// <param name="maxProbeDelay">
    /// The ceiling the delay doubles up to; defaults to
    /// <see cref="DefaultMaxProbeDelay"/>. Clamped up to the initial delay, so a
    /// misconfigured pair can never shorten the backoff below its own first step.
    /// </param>
    public RepoContextExactScanBreaker(
        TimeProvider? timeProvider = null,
        TimeSpan? initialProbeDelay = null,
        TimeSpan? maxProbeDelay = null)
    {
        _time = timeProvider ?? TimeProvider.System;

        var initial = initialProbeDelay ?? DefaultInitialProbeDelay;
        InitialProbeDelay = initial < TimeSpan.Zero ? TimeSpan.Zero : initial;

        var max = maxProbeDelay ?? DefaultMaxProbeDelay;
        MaxProbeDelay = max < InitialProbeDelay ? InitialProbeDelay : max;
    }

    /// <summary>The delay before the first half-open probe after a stall.</summary>
    public TimeSpan InitialProbeDelay { get; }

    /// <summary>The ceiling the probe delay doubles up to.</summary>
    public TimeSpan MaxProbeDelay { get; }

    /// <summary>
    /// Whether a stall is on record for this repository, whatever the probe window
    /// currently permits. This is the breaker's reported open/closed state, and is
    /// deliberately <b>not</b> the gate a query consults - a query calls
    /// <see cref="Evaluate"/>, so that the probe can never be skipped by reading
    /// the state instead of the decision.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the breaker is open.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool IsTripped(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_open.TryGetValue(repoId, out var episode))
        {
            return false;
        }

        lock (episode)
        {
            return episode.Stalls > 0;
        }
    }

    /// <summary>
    /// Decides what this query may do, granting the single half-open probe when
    /// the current delay has elapsed. Issuing the grant re-arms the delay, so this
    /// is a mutating read: call it once per query rather than consulting it
    /// speculatively.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The decision.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public RepoContextExactScanBreakerDecision Evaluate(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_open.TryGetValue(repoId, out var episode))
        {
            return RepoContextExactScanBreakerDecision.Closed;
        }

        lock (episode)
        {
            if (episode.Stalls == 0)
            {
                return RepoContextExactScanBreakerDecision.Closed;
            }

            var now = _time.GetUtcNow().UtcTicks;
            if (now < episode.NextProbeTicks)
            {
                return RepoContextExactScanBreakerDecision.Open;
            }

            // Re-arm before returning, so exactly one caller probes per window even
            // when the probe never reports an outcome.
            episode.NextProbeTicks = now + DelayTicks(episode.Stalls);
            return RepoContextExactScanBreakerDecision.Probe;
        }
    }

    /// <summary>
    /// Records that an exact gather over this repository stalled, growing the
    /// delay before the next probe.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when this call opened the breaker, so only the
    /// query that actually paid the ceiling reports it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool Trip(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var episode = _open.GetOrAdd(repoId, static _ => new Episode());
        lock (episode)
        {
            var now = _time.GetUtcNow().UtcTicks;
            var first = episode.Stalls == 0;
            if (first)
            {
                episode.OpenedAtTicks = now;
            }

            episode.Stalls++;
            episode.NextProbeTicks = now + DelayTicks(episode.Stalls);
            return first;
        }
    }

    /// <summary>
    /// Closes the breaker for a repository, restoring the exact fallback. Called
    /// when the plane answers for itself, and when a half-open probe completes -
    /// the two independent pieces of evidence that the contention which stalled
    /// the gather is gone. The second is what keeps the exit reachable on a host
    /// whose plane never serves.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when an open breaker was closed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool Reset(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (_open.IsEmpty || !_open.TryRemove(repoId, out var episode))
        {
            return false;
        }

        lock (episode)
        {
            return episode.Stalls > 0;
        }
    }

    /// <summary>
    /// How many gathers have stalled in the repository's current open episode, or
    /// <c>0</c> when the breaker is closed. Read to tell a repository riding out
    /// transient contention from one that is genuinely wedged.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The consecutive stall count.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public int ConsecutiveStalls(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_open.TryGetValue(repoId, out var episode))
        {
            return 0;
        }

        lock (episode)
        {
            return episode.Stalls;
        }
    }

    /// <summary>
    /// How long the breaker has been continuously open for a repository, or
    /// <see langword="null"/> when it is closed. Reported in the stuck-state
    /// warning so the line an operator reads carries the duration rather than
    /// leaving it to be inferred by correlating timestamps.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The open duration, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public TimeSpan? OpenFor(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_open.TryGetValue(repoId, out var episode))
        {
            return null;
        }

        lock (episode)
        {
            if (episode.Stalls == 0)
            {
                return null;
            }

            var elapsed = _time.GetUtcNow().UtcTicks - episode.OpenedAtTicks;
            return new TimeSpan(elapsed < 0 ? 0 : elapsed);
        }
    }

    /// <summary>
    /// How long until the next half-open probe is due, or <see langword="null"/>
    /// when the breaker is closed. <see cref="TimeSpan.Zero"/> means the next
    /// query probes.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The remaining delay, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public TimeSpan? ProbeDueIn(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (!_open.TryGetValue(repoId, out var episode))
        {
            return null;
        }

        lock (episode)
        {
            if (episode.Stalls == 0)
            {
                return null;
            }

            var remaining = episode.NextProbeTicks - _time.GetUtcNow().UtcTicks;
            return new TimeSpan(remaining < 0 ? 0 : remaining);
        }
    }

    /// <summary>
    /// The delay after <paramref name="stalls"/> consecutive stalls: the initial
    /// delay doubled once per stall past the first, capped at
    /// <see cref="MaxProbeDelay"/>. The doubling is tested against the cap before
    /// it is applied, so a long-lived wedged repository cannot shift the delay
    /// into an overflowed negative one.
    /// </summary>
    /// <param name="stalls">The consecutive stall count, at least one.</param>
    /// <returns>The delay in ticks.</returns>
    private long DelayTicks(int stalls)
    {
        var initialTicks = InitialProbeDelay.Ticks;
        if (initialTicks <= 0)
        {
            return 0;
        }

        // 62 shifts is already far past any representable TimeSpan, so clamping the
        // exponent there keeps the shift defined without bounding the stall count.
        var maxTicks = MaxProbeDelay.Ticks;
        var doublings = Math.Min(Math.Max(stalls - 1, 0), 62);
        return initialTicks > maxTicks >> doublings ? maxTicks : initialTicks << doublings;
    }

    /// <summary>
    /// One repository's open episode. Mutated under its own monitor rather than
    /// with interlocked operations because the probe grant must test the delay and
    /// re-arm it as a single act - two callers each observing an elapsed delay and
    /// both probing is exactly what the window exists to prevent.
    /// </summary>
    private sealed class Episode
    {
        /// <summary>Consecutive stalls in this episode; zero means closed.</summary>
        public int Stalls;

        /// <summary>UTC ticks the episode's first stall was recorded at.</summary>
        public long OpenedAtTicks;

        /// <summary>UTC ticks before which no probe is granted.</summary>
        public long NextProbeTicks;
    }
}
