using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shared, thread-safe record of whether this host can serve <b>semantic</b>
/// retrieval, and how long it took to get there. It is the state the vector-plane
/// readiness component of <c>/health/ready</c> reads, so a box that cannot serve its
/// headline capability stops reporting itself fully ready.
/// <para>
/// <b>Derived at the narrowest seam.</b> The authoritative input is the retrieval path
/// a real query actually took (<see cref="Observe(string?)"/>), fed from the one place
/// every search funnels through. A semantic answer proves the plane serves; a
/// vector-plane or index failure proves it does not; "no embedder bound" is not a
/// failure at all. A readiness probe may supply the same signals out of band so a host
/// with no traffic still converges instead of waiting for a query that never comes.
/// </para>
/// <para>
/// <b>It does not flap.</b> Once the plane has served, a fault does not immediately
/// revoke readiness: the fault must persist for <see cref="FaultHoldDown"/> before the
/// phase falls back to <see cref="RepoContextRetrievalReadinessPhase.Building"/>, and
/// any successful retrieval inside that window clears the episode outright. The window
/// is measured with the injected <see cref="TimeProvider"/>, never a wall clock or a
/// timer, so the behaviour is deterministic and testable.
/// </para>
/// <para>
/// <b>It does not deadlock.</b> A host with no embedding provider bound reports
/// <see cref="RepoContextRetrievalReadinessPhase.KeywordOnly"/>, which is ready: there
/// is no vector plane to wait for.
/// </para>
/// </summary>
public sealed class RepoContextRetrievalReadinessState : IDisposable
{
    /// <summary>
    /// The histogram name recording, once per process, the seconds from this state's
    /// construction (host start) to the retrieval plane first reporting ready. This is
    /// the time-to-retrieval-ready figure a cold-start benchmark measures.
    /// </summary>
    internal const string ReadySecondsInstrumentName = "repocontext.retrieval.ready_seconds";

    /// <summary>The counter name incremented once per observed vector-plane fault episode.</summary>
    internal const string UnavailableInstrumentName = "repocontext.retrieval.unavailable";

    /// <summary>The low-cardinality tag key carrying the readiness phase first reached.</summary>
    internal const string PhaseTagKey = "phase";

    /// <summary>The low-cardinality tag key carrying the cause of a fault episode.</summary>
    internal const string CauseTagKey = "cause";

    /// <summary>Phase tag value recorded when the plane first reported <see cref="RepoContextRetrievalReadinessPhase.Serving"/>.</summary>
    internal const string PhaseServingTag = "serving";

    /// <summary>Phase tag value recorded when the host first reported <see cref="RepoContextRetrievalReadinessPhase.KeywordOnly"/>.</summary>
    internal const string PhaseKeywordOnlyTag = "keyword_only";

    /// <summary>
    /// The cause a readiness probe supplies to <see cref="MarkUnavailable(string?)"/>
    /// when it, rather than a real query, observed the plane unable to serve.
    /// </summary>
    public const string ProbeCause = "probe";

    /// <summary>The cause recorded when a supplied cause is absent or not a recognised local value.</summary>
    internal const string UnknownCause = "unknown";

    /// <summary>
    /// The default window a vector-plane fault must persist for before readiness is
    /// revoked, long enough to ride out a transient fault without oscillating.
    /// </summary>
    public static readonly TimeSpan DefaultFaultHoldDown = TimeSpan.FromSeconds(30);

    private const int BuildingRaw = (int)RepoContextRetrievalReadinessPhase.Building;
    private const int ServingRaw = (int)RepoContextRetrievalReadinessPhase.Serving;
    private const int KeywordOnlyRaw = (int)RepoContextRetrievalReadinessPhase.KeywordOnly;
    private const long NoFault = long.MinValue;
    private const long NotReady = -1L;

    private readonly TimeProvider _timeProvider;
    private readonly long _holdDownTicks;
    private readonly long _startedTicks;
    private readonly object _gate = new();
    private readonly Meter _meter;
    private readonly Histogram<double> _readySeconds;
    private readonly Counter<long> _unavailable;

    private int _phase = BuildingRaw;
    private long _faultSinceTicks = NoFault;
    private long _readyElapsedTicks = NotReady;

    /// <summary>Creates the readiness state, starting the time-to-ready clock.</summary>
    /// <param name="timeProvider">The clock driving the fault hold-down and the time-to-ready measurement. Must not be <see langword="null"/>.</param>
    /// <param name="faultHoldDown">How long an observed fault must persist before readiness is revoked; defaults to <see cref="DefaultFaultHoldDown"/>. A non-positive value revokes readiness on the first observed fault.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is null.</exception>
    public RepoContextRetrievalReadinessState(TimeProvider timeProvider, TimeSpan? faultHoldDown = null)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        FaultHoldDown = faultHoldDown ?? DefaultFaultHoldDown;
        _holdDownTicks = FaultHoldDown.Ticks < 0 ? 0 : FaultHoldDown.Ticks;
        _startedTicks = timeProvider.GetUtcNow().UtcTicks;

        // Publish under the same meter name as the rest of the repocontext surface so a
        // single scraper subscription covers it.
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _readySeconds = _meter.CreateHistogram<double>(
            ReadySecondsInstrumentName,
            unit: "s",
            description: "Seconds from host start to the retrieval plane first reporting ready, tagged by the phase it reached.");
        _unavailable = _meter.CreateCounter<long>(
            UnavailableInstrumentName,
            unit: "{event}",
            description: "Observed vector-plane fault episodes that made semantic retrieval unavailable, tagged by cause.");
    }

    /// <summary>
    /// How long an observed fault must persist before readiness is revoked. A
    /// successful retrieval inside the window clears the episode outright.
    /// </summary>
    public TimeSpan FaultHoldDown { get; }

    /// <summary>
    /// The current phase, with the fault hold-down applied: a plane that has served
    /// still reports <see cref="RepoContextRetrievalReadinessPhase.Serving"/> until an
    /// outstanding fault has persisted for <see cref="FaultHoldDown"/>.
    /// </summary>
    public RepoContextRetrievalReadinessPhase Phase
    {
        get
        {
            var raw = Volatile.Read(ref _phase);
            if (raw != ServingRaw)
            {
                return (RepoContextRetrievalReadinessPhase)raw;
            }

            var faultSince = Volatile.Read(ref _faultSinceTicks);
            if (faultSince == NoFault)
            {
                return RepoContextRetrievalReadinessPhase.Serving;
            }

            return _timeProvider.GetUtcNow().UtcTicks - faultSince >= _holdDownTicks
                ? RepoContextRetrievalReadinessPhase.Building
                : RepoContextRetrievalReadinessPhase.Serving;
        }
    }

    /// <summary>
    /// <see langword="true"/> when the host can serve the retrieval it is configured
    /// for - the vector plane is serving, or no embedder is bound and keyword recall is
    /// the intended steady state.
    /// </summary>
    public bool IsReady => Phase != RepoContextRetrievalReadinessPhase.Building;

    /// <summary>
    /// The elapsed time from this state's construction to the moment the host first
    /// reported ready, or <see langword="null"/> while it has never been ready. This is
    /// the same figure published on the <c>repocontext.retrieval.ready_seconds</c>
    /// histogram.
    /// </summary>
    public TimeSpan? TimeToReady
    {
        get
        {
            var ticks = Volatile.Read(ref _readyElapsedTicks);
            return ticks == NotReady ? null : new TimeSpan(ticks);
        }
    }

    /// <summary>
    /// Folds an observed retrieval path into the readiness signal. This is the single
    /// seam every real query funnels through, so readiness reflects what retrieval
    /// actually did rather than what configuration promised. An unrecognised value
    /// changes nothing (fail closed: it never invents readiness).
    /// </summary>
    /// <param name="retrievalPath">A <see cref="RepoContextRetrievalPath"/> value, which may be <see langword="null"/>.</param>
    public void Observe(string? retrievalPath)
    {
        if (RepoContextRetrievalPath.IsSemantic(retrievalPath))
        {
            MarkServing();
            return;
        }

        if (string.Equals(retrievalPath, RepoContextRetrievalPath.KeywordNoEmbedder, StringComparison.Ordinal))
        {
            MarkKeywordOnly();
            return;
        }

        if (string.Equals(retrievalPath, RepoContextRetrievalPath.KeywordVectorPlaneUnavailable, StringComparison.Ordinal)
            || string.Equals(retrievalPath, RepoContextRetrievalPath.KeywordIndexDegraded, StringComparison.Ordinal))
        {
            MarkUnavailable(retrievalPath);
        }
    }

    /// <summary>
    /// Records that the vector plane demonstrably served semantic retrieval. Clears any
    /// outstanding fault episode, and promotes a keyword-only host that has acquired a
    /// working plane. Idempotent.
    /// </summary>
    public void MarkServing()
    {
        // Steady state: already serving with no outstanding fault. No lock, no
        // allocation, no clock read - this is the per-query path.
        if (Volatile.Read(ref _phase) == ServingRaw && Volatile.Read(ref _faultSinceTicks) == NoFault)
        {
            return;
        }

        lock (_gate)
        {
            var wasReady = _phase != BuildingRaw;
            Volatile.Write(ref _phase, ServingRaw);
            Volatile.Write(ref _faultSinceTicks, NoFault);
            if (!wasReady)
            {
                StampReady(PhaseServingTag);
            }
        }
    }

    /// <summary>
    /// Records that no embedding provider is bound, so keyword recall is the intended
    /// steady state and the host is ready. Ignored once the plane has been proven
    /// serving, so a stale observation can never demote a working plane. Idempotent.
    /// </summary>
    public void MarkKeywordOnly()
    {
        if (Volatile.Read(ref _phase) != BuildingRaw)
        {
            return;
        }

        lock (_gate)
        {
            if (_phase != BuildingRaw)
            {
                return;
            }

            Volatile.Write(ref _faultSinceTicks, NoFault);
            Volatile.Write(ref _phase, KeywordOnlyRaw);
            StampReady(PhaseKeywordOnlyTag);
        }
    }

    /// <summary>
    /// Records that the vector plane could not serve semantic retrieval. The first
    /// observation of an episode opens the <see cref="FaultHoldDown"/> window and is
    /// metered; readiness is revoked only once that window elapses without a successful
    /// retrieval, so a transient fault does not make readiness oscillate. Ignored in
    /// <see cref="RepoContextRetrievalReadinessPhase.KeywordOnly"/>, where there is no
    /// vector plane to be unavailable.
    /// </summary>
    /// <param name="cause">Why the plane could not serve - a <see cref="RepoContextRetrievalPath"/> keyword value or <see cref="ProbeCause"/>. Any other value is metered as <c>"unknown"</c> so an unbounded tag can never reach the meter.</param>
    public void MarkUnavailable(string? cause = null)
    {
        var raw = Volatile.Read(ref _phase);
        if (raw == KeywordOnlyRaw)
        {
            return;
        }

        // An episode is already open: keep its original timestamp so the hold-down
        // measures the fault, not the latest report of it.
        if (Volatile.Read(ref _faultSinceTicks) != NoFault)
        {
            return;
        }

        var now = _timeProvider.GetUtcNow().UtcTicks;
        bool opened;
        lock (_gate)
        {
            opened = _phase != KeywordOnlyRaw && _faultSinceTicks == NoFault;
            if (opened)
            {
                Volatile.Write(ref _faultSinceTicks, now);
            }
        }

        if (opened)
        {
            // Two struct tags (string values, no boxing) - no array is allocated. The
            // cause is normalised against local constants first, so a caller can never
            // put unbounded-cardinality text on the meter.
            _unavailable.Add(
                1,
                new KeyValuePair<string, object?>(CauseTagKey, NormalizeCause(cause)),
                LatticeTenantLabel.Platform);
        }
    }

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();

    /// <summary>
    /// Stamps and publishes the time-to-retrieval-ready figure on the first transition
    /// into a ready phase. Called while holding the transition lock.
    /// </summary>
    private void StampReady(string phaseTag)
    {
        if (_readyElapsedTicks != NotReady)
        {
            return;
        }

        var elapsed = _timeProvider.GetUtcNow().UtcTicks - _startedTicks;
        var clamped = elapsed < 0 ? 0 : elapsed;
        Volatile.Write(ref _readyElapsedTicks, clamped);
        _readySeconds.Record(
            new TimeSpan(clamped).TotalSeconds,
            new KeyValuePair<string, object?>(PhaseTagKey, phaseTag),
            LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Resolves a supplied cause against the closed set of local values, so the meter's
    /// tag cardinality is bounded no matter what a caller passes.
    /// </summary>
    private static string NormalizeCause(string? cause)
    {
        if (string.Equals(cause, RepoContextRetrievalPath.KeywordVectorPlaneUnavailable, StringComparison.Ordinal))
        {
            return RepoContextRetrievalPath.KeywordVectorPlaneUnavailable;
        }

        if (string.Equals(cause, RepoContextRetrievalPath.KeywordIndexDegraded, StringComparison.Ordinal))
        {
            return RepoContextRetrievalPath.KeywordIndexDegraded;
        }

        return string.Equals(cause, ProbeCause, StringComparison.Ordinal) ? ProbeCause : UnknownCause;
    }
}
