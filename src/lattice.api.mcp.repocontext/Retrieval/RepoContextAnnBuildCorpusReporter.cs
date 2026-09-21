using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How much of the vector corpus the access gate admitted to one approximate-index
/// build that reached <c>Ready</c>. The five values are exhaustive over a build
/// that completed, which is what lets them be counted as a partition rather than
/// as five unrelated tallies.
/// <para>
/// <see cref="NonEmpty"/> is the arm that makes the rest interpretable. A counter
/// that recorded only the empty builds would read zero on every arm both when the
/// plane is healthy and when nothing ever built, so a zero on
/// <see cref="Denied"/> would be silence rather than a measured absence. Counting
/// every completed build makes the total a denominator, which is the same shape
/// <see cref="RepoContextAnnIndexSweepReporter"/> settled on and the shape issue
/// #2314 declined the alternative to.
/// </para>
/// </summary>
internal enum RepoContextAnnBuildCorpusCoverage
{
    /// <summary>
    /// The build reached <c>Ready</c> holding at least one vector, so the corpus
    /// read plainly succeeded and no coverage probe was needed or taken.
    /// </summary>
    NonEmpty = 0,

    /// <summary>
    /// The build reached <c>Ready</c> holding nothing, and the gate admits the
    /// whole vector prefix. The repository genuinely has no vectors: an honest
    /// empty store, and a legitimate converged state.
    /// </summary>
    Unrestricted = 1,

    /// <summary>
    /// The build reached <c>Ready</c> holding nothing, and the gate narrowed the
    /// vector prefix with a per-key filter. The authority resolved correctly and
    /// legitimately returned a subset, so this is a complete and correct read of
    /// what the caller is permitted to see, and it converges.
    /// </summary>
    Filtered = 2,

    /// <summary>
    /// The build reached <c>Ready</c> holding nothing because the gate denied the
    /// vector prefix outright. The read did not happen, so the index state is not
    /// empty - it is <b>unknown</b>, and banking a converged index on it is the
    /// fail-open-into-silence issue #2426 exists to remove.
    /// </summary>
    Denied = 3,

    /// <summary>
    /// The build reached <c>Ready</c> holding nothing and the coverage probe could
    /// not answer, so whether the read was admitted is unknown. Classified here
    /// rather than as <see cref="Unrestricted"/> because a probe that fails must
    /// never be read as permission granted.
    /// </summary>
    Unknown = 4,
}

/// <summary>
/// A point-in-time reading of the build-corpus counters, cumulative since process
/// start.
/// </summary>
/// <param name="NonEmpty">Completed builds that held at least one vector.</param>
/// <param name="Unrestricted">Completed builds that held nothing against an unrestricted prefix.</param>
/// <param name="Filtered">Completed builds that held nothing against a filtered prefix.</param>
/// <param name="Denied">Completed builds that held nothing because the prefix was denied.</param>
/// <param name="Unknown">Completed builds that held nothing and whose coverage could not be probed.</param>
/// <param name="TerminalDenials">
/// Coordinators that crossed the consecutive-denial threshold and parked on the
/// capped retry interval.
/// </param>
internal readonly record struct RepoContextAnnBuildCorpusSnapshot(
    long NonEmpty,
    long Unrestricted,
    long Filtered,
    long Denied,
    long Unknown,
    long TerminalDenials);

/// <summary>
/// Meters what the approximate-index build coordinator actually read, so an
/// authorization denial on the corpus can never again be mistaken for an empty
/// repository.
/// <para>
/// <b>Why this exists.</b> A denied <b>point</b> read throws, but a denied
/// <b>range</b> read resolves to a reject-all key filter and returns a clean,
/// successful, empty result: no exception, no log, every instrument healthy. The
/// build then counted zero vectors, ingested zero, declined to partition, reached
/// <c>Ready</c>, recorded <c>Converged</c>, logged a <i>success</i> line and stood
/// the coordinator down - so a refused read became durably indistinguishable from
/// a repository that genuinely had nothing to index, and nothing re-drove it. Two
/// full deployment gates on the repocontext-reliability line ended at an empty
/// corpus without being able to say whether authorization was the cause, because
/// no series anywhere could answer it.
/// </para>
/// <para>
/// <b>Why prose was not enough.</b> This bucket has direct evidence that a warning
/// log line documenting a blindness was ignored for three hours while a counter on
/// the dashboard read success. Prose that contradicts a metric loses every time
/// when only one of the two is being watched, so the remedy has to be a series.
/// </para>
/// <para>
/// <b>Why a zero here is evidence and not silence.</b> The partition is total over
/// every build that reached <c>Ready</c>, including the ordinary non-empty ones, so
/// the total advances whenever the plane builds at all. A zero on
/// <c>coverage=denied</c> beside a rising total is therefore a <i>measured</i>
/// absence of denial. Had the counter recorded only empty builds, every arm would
/// read zero both on a healthy plane and on one that never ran, and the zero would
/// have carried no information - the failure shape declined on issue #2314 and
/// re-learned on #2406.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tag is the closed coverage set. No
/// repository id, no key, no bound. A coverage classification is deliberately not
/// the withheld keys, which is the same disclosure argument that keeps
/// <see cref="GatedMultiReadResult.PrunedByAccessGate"/> a count, and the same
/// reason <see cref="LatticeRangeReadGateCoverage"/> reports a class rather than a
/// reason.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host <c>coverage=denied</c> is
/// present and reads <c>0</c> rather than being absent. That is a strictly
/// stronger statement than absence: an assertion that can only fire positively
/// cannot distinguish "the change landed" from "the check is broken".
/// </para>
/// </summary>
internal sealed class RepoContextAnnBuildCorpusReporter : IDisposable
{
    /// <summary>
    /// The counter of completed approximate-index builds, partitioned by how much
    /// of the vector corpus the access gate admitted.
    /// </summary>
    internal const string CorpusInstrumentName = "repocontext.ann.build.corpus";

    /// <summary>
    /// The counter of build coordinators that crossed the consecutive-denial
    /// threshold and parked on the capped retry interval. Separate from the
    /// coverage partition because a rising <c>coverage=denied</c> arm cannot on its
    /// own distinguish a denial that is being retried from a host that is
    /// permanently refused.
    /// </summary>
    internal const string TerminalDenialInstrumentName = "repocontext.ann.build.denial_terminal";

    /// <summary>The tag key carrying the coverage partition.</summary>
    internal const string CoverageTagKey = "coverage";

    /// <summary>The tag value for a build that held at least one vector.</summary>
    internal const string CoverageNonEmptyTag = "nonempty";

    /// <summary>The tag value for an empty build against an unrestricted prefix.</summary>
    internal const string CoverageUnrestrictedTag = "unrestricted";

    /// <summary>The tag value for an empty build against a filtered prefix.</summary>
    internal const string CoverageFilteredTag = "filtered";

    /// <summary>The tag value for an empty build against a denied prefix.</summary>
    internal const string CoverageDeniedTag = "denied";

    /// <summary>The tag value for an empty build whose coverage could not be probed.</summary>
    internal const string CoverageUnknownTag = "unknown";

    // Declared above the instruments it constructs, and both instruments are built
    // from this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _corpusCoverage;
    private readonly Counter<long> _terminalDenials;

    private readonly Lock _gate = new();
    private long _nonEmpty;
    private long _unrestricted;
    private long _filtered;
    private long _denied;
    private long _unknown;
    private long _terminal;

    /// <summary>Creates the reporter, its instruments, and every one of their series.</summary>
    public RepoContextAnnBuildCorpusReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _corpusCoverage = _meter.CreateCounter<long>(
            CorpusInstrumentName,
            unit: "{build}",
            description:
                "Approximate-index builds that reached Ready, partitioned by how much of the repository's vector "
                + "prefix the read-path access gate admitted: 'nonempty' (the build holds vectors, so the corpus "
                + "read plainly succeeded), 'unrestricted' (it holds nothing and the whole prefix is admitted, so "
                + "the repository genuinely has no vectors), 'filtered' (it holds nothing and the gate narrowed "
                + "the prefix, so an unknown subset was withheld), 'denied' (it holds nothing because the gate "
                + "refused the prefix outright, so the read never happened and the index state is unknown rather "
                + "than empty), or 'unknown' (it holds nothing and the coverage probe could not answer). Every "
                + "completed build is counted, so the total is a denominator and a zero on 'denied' beside a "
                + "rising total is a measured absence of denial rather than an absent measurement. A denied range "
                + "read returns a clean empty result rather than throwing, so without this partition an "
                + "authorization failure and an empty repository are the same observation.");
        _terminalDenials = _meter.CreateCounter<long>(
            TerminalDenialInstrumentName,
            unit: "{coordinator}",
            description:
                "Approximate-index build coordinators that observed enough consecutive denied corpus reads to "
                + "conclude the host is refusing them, and have parked on the capped retry interval rather than "
                + "retrying on the phase cadence. Counted once per denial episode, so it separates 'a denial "
                + "happened and is being retried' from 'this deployment is permanently refused and the "
                + "approximate plane will never build', which a monotonically rising denied arm cannot. A "
                + "non-zero value always warrants an operator: it means an index that would otherwise exist does "
                + "not, and it will not appear on its own.");

        // Pre-mint every series with a zero-valued add, so a correctly configured
        // host reports coverage=denied at 0 rather than omitting it. An absent
        // series and a series reading zero look identical on a dashboard but are
        // very different claims, and only the second is falsifiable.
        _corpusCoverage.Add(0, new KeyValuePair<string, object?>(CoverageTagKey, CoverageNonEmptyTag), LatticeTenantLabel.Platform);
        _corpusCoverage.Add(0, new KeyValuePair<string, object?>(CoverageTagKey, CoverageUnrestrictedTag), LatticeTenantLabel.Platform);
        _corpusCoverage.Add(0, new KeyValuePair<string, object?>(CoverageTagKey, CoverageFilteredTag), LatticeTenantLabel.Platform);
        _corpusCoverage.Add(0, new KeyValuePair<string, object?>(CoverageTagKey, CoverageDeniedTag), LatticeTenantLabel.Platform);
        _corpusCoverage.Add(0, new KeyValuePair<string, object?>(CoverageTagKey, CoverageUnknownTag), LatticeTenantLabel.Platform);
        _terminalDenials.Add(0, LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records one approximate-index build that reached <c>Ready</c>, partitioned
    /// by the coverage its corpus read was admitted under.
    /// </summary>
    /// <param name="coverage">How much of the vector prefix the gate admitted.</param>
    public void RecordCoverage(RepoContextAnnBuildCorpusCoverage coverage)
    {
        _corpusCoverage.Add(
            1,
            new KeyValuePair<string, object?>(CoverageTagKey, DescribeCoverage(coverage)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            switch (coverage)
            {
                case RepoContextAnnBuildCorpusCoverage.NonEmpty:
                    _nonEmpty++;
                    break;
                case RepoContextAnnBuildCorpusCoverage.Unrestricted:
                    _unrestricted++;
                    break;
                case RepoContextAnnBuildCorpusCoverage.Filtered:
                    _filtered++;
                    break;
                case RepoContextAnnBuildCorpusCoverage.Denied:
                    _denied++;
                    break;
                default:
                    _unknown++;
                    break;
            }
        }
    }

    /// <summary>
    /// Records that one coordinator has parked on the capped retry interval after
    /// a run of consecutive denials. Called once per episode by the coordinator,
    /// which owns the episode state because it is single-threaded and scoped to
    /// exactly one repository and embedding space.
    /// </summary>
    public void RecordTerminalDenial()
    {
        _terminalDenials.Add(1, LatticeTenantLabel.Platform);
        lock (_gate)
        {
            _terminal++;
        }
    }

    /// <summary>Reads the cumulative counters.</summary>
    /// <returns>The snapshot.</returns>
    public RepoContextAnnBuildCorpusSnapshot Read()
    {
        lock (_gate)
        {
            return new RepoContextAnnBuildCorpusSnapshot(
                _nonEmpty, _unrestricted, _filtered, _denied, _unknown, _terminal);
        }
    }

    /// <summary>
    /// The bounded tag value for a coverage class. Resolved against a closed set so
    /// an unrecognised value can never reach the meter as unbounded-cardinality
    /// text, and so a new enum member fails closed onto <c>unknown</c> rather than
    /// onto a permissive arm.
    /// </summary>
    /// <param name="coverage">The coverage class to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeCoverage(RepoContextAnnBuildCorpusCoverage coverage) => coverage switch
    {
        RepoContextAnnBuildCorpusCoverage.NonEmpty => CoverageNonEmptyTag,
        RepoContextAnnBuildCorpusCoverage.Unrestricted => CoverageUnrestrictedTag,
        RepoContextAnnBuildCorpusCoverage.Filtered => CoverageFilteredTag,
        RepoContextAnnBuildCorpusCoverage.Denied => CoverageDeniedTag,
        _ => CoverageUnknownTag,
    };

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
