using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Why a bootstrap pass reached the embedding-coverage verdict it did.
/// <para>
/// The three non-converged members are the whole point of the partition. Before
/// issue #3340 the pass collapsed them into one boolean, so "I measured a gap" and
/// "I could not measure at all" produced the identical control decision - escalate
/// the gap scan to every pass - and the identical scrape reading. Under load the
/// second cause is self-sustaining: the escalated whole-corpus sweep is itself the
/// load that refuses the next probe, so the pass never measures again and never
/// stands down.
/// </para>
/// </summary>
internal enum RepoContextCoverageVerdict
{
    /// <summary>
    /// The pass measured coverage and found it complete. The gap scan backs off to
    /// its periodic cadence.
    /// <para>
    /// Present, rather than left implicit as the absence of the other three,
    /// because this instrument's purpose is to distinguish "the scan stood down
    /// because the corpus is covered" from "the scan stood down because the pass
    /// could not look". An instrument that only writes on detection cannot report
    /// its own liveness, so silence would be unreadable.
    /// </para>
    /// </summary>
    Converged = 0,

    /// <summary>
    /// An ingestion arm faulted, so the pass's coverage facts are not admissible
    /// whatever they said. Clears convergence; the next pass decides whether a
    /// measured gap or unmeasurable probe keeps the every-pass scan armed.
    /// </summary>
    ArmFailure = 1,

    /// <summary>
    /// The pass measured coverage and found a real gap. Clears convergence and
    /// holds the scan armed on every pass until the corpus is observed clean,
    /// which is the behaviour the escalation exists for.
    /// </summary>
    GapFound = 2,

    /// <summary>
    /// The pass could not measure coverage: the membership probe failed or was
    /// pruned, the embed work was deferred under saturation, or the gap scan was
    /// skipped by the arm's own back-off. This is an <b>unknown</b>, not a gap.
    /// <para>
    /// It neither asserts nor clears convergence - the standing verdict is carried
    /// forward - and it backs the scan off to the periodic cadence, because
    /// scanning harder cannot heal a reading that could not be taken and, on a
    /// saturated store, is what prevents the next one.
    /// </para>
    /// </summary>
    ProbeUnmeasurable = 3,
}

/// <summary>
/// Publishes <c>repocontext.bootstrap.coverage_verdict</c>: one reading per
/// bootstrap pass that actually evaluated embedding coverage, partitioned by the
/// reason the verdict came out the way it did.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this instrument exists, and why it is a release gate rather than a
/// nicety.</b> The fix for issue #3340 stops an unmeasurable probe from clearing
/// <c>CoverageConverged</c>, so the gap scan stands down where it used to escalate.
/// Standing down is the correct behaviour and it is also indistinguishable, from
/// outside, from the repository genuinely being converged - both look like a quiet
/// pass that scans on the cadence. Without this partition the fix would convert a
/// loud pathology into a silent one, which is the same defect class as issues #3320
/// and #2656. The <see cref="RepoContextCoverageVerdict.ProbeUnmeasurable"/> arm is
/// the reading that makes "we stopped escalating because we cannot see" falsifiable.
/// </para>
/// <para>
/// <b>It does not duplicate <c>repocontext.bootstrap.coverage_probe</c>.</b> That
/// instrument counts <i>probe</i> outcomes per ingestion arm - conclusive,
/// gate-pruned, failed - and keeps doing so unchanged. This one counts <i>pass
/// control decisions</i>: what the pass concluded about the repository and
/// therefore what cadence the next pass runs at. A failed probe continues to be
/// charged as <c>probe_failed</c> there while ceasing to force a re-scan here, and
/// reading the two side by side is what shows the escalation loop is broken:
/// <c>probe_failed</c> non-zero with <c>probe_unmeasurable</c> non-zero and
/// <c>gap_found</c> flat is the fixed state, whereas <c>gap_found</c> rising in
/// step with <c>probe_failed</c> would be the defect reappearing.
/// </para>
/// <para>
/// All four series are pre-minted at zero in the constructor, on the same seam that
/// charges them, so an arm reading zero and an arm being absent are different
/// observations. All four zero means no pass ever reached the coverage verdict.
/// Deliberately a counter and not a histogram: the quantity of interest is how many
/// passes concluded each way, and priming a histogram would fabricate a zero-valued
/// sample that reads as a real measurement.
/// </para>
/// </remarks>
internal sealed class RepoContextCoverageVerdictReporter : IDisposable
{
    /// <summary>
    /// The counter of bootstrap embedding-coverage verdicts, partitioned by the
    /// reason the pass reached that verdict.
    /// </summary>
    internal const string VerdictInstrumentName = "repocontext.bootstrap.coverage_verdict";

    /// <summary>The tag key carrying the verdict-reason partition.</summary>
    internal const string ReasonTagKey = "reason";

    /// <summary>The tag value for a pass that measured coverage and found it complete.</summary>
    internal const string ReasonConvergedTag = "converged";

    /// <summary>The tag value for a pass whose ingestion arm faulted.</summary>
    internal const string ReasonArmFailureTag = "arm_failure";

    /// <summary>The tag value for a pass that measured coverage and found a real gap.</summary>
    internal const string ReasonGapFoundTag = "gap_found";

    /// <summary>
    /// The tag value for a pass that could not measure coverage at all, and so
    /// carried the standing verdict forward instead of asserting a gap.
    /// </summary>
    internal const string ReasonProbeUnmeasurableTag = "probe_unmeasurable";

    private const int VerdictCount = 4;

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than publishing
    // an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _verdicts;

    private readonly Lock _gate = new();
    private readonly long[] _counts = new long[VerdictCount];

    /// <summary>Creates the reporter, its instrument, and every one of its four series.</summary>
    public RepoContextCoverageVerdictReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _verdicts = _meter.CreateCounter<long>(
            VerdictInstrumentName,
            unit: "{verdict}",
            description:
                "Bootstrap embedding-coverage verdicts, partitioned by why the pass concluded as it did: "
                + "coverage measured complete, an ingestion arm faulted, a real gap was measured, or "
                + "coverage could not be measured at all.");

        // Each priming names the tag value literally, and the four are not folded
        // into a loop or a helper taking the reason as a parameter, for the same
        // reason Record below is four literal cases: the priming-enrolment gate
        // resolves a tag value by reading the call site and discards any it cannot
        // resolve to exactly one literal. A helper primes the series perfectly at run
        // time and presents nothing to the gate, so the enrolment would be demoted
        // and the instrument would silently lose its guard.
        _verdicts.Add(0, new KeyValuePair<string, object?>(ReasonTagKey, ReasonConvergedTag), LatticeTenantLabel.Platform);
        _verdicts.Add(0, new KeyValuePair<string, object?>(ReasonTagKey, ReasonArmFailureTag), LatticeTenantLabel.Platform);
        _verdicts.Add(0, new KeyValuePair<string, object?>(ReasonTagKey, ReasonGapFoundTag), LatticeTenantLabel.Platform);
        _verdicts.Add(0, new KeyValuePair<string, object?>(ReasonTagKey, ReasonProbeUnmeasurableTag), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Classifies one pass's coverage facts into the verdict they support.
    /// </summary>
    /// <param name="armFaulted">
    /// Whether an ingestion arm faulted during the pass. Checked first, so a real
    /// arm fault is never re-labelled as a measurement outcome.
    /// </param>
    /// <param name="outcome">The file arm's reported coverage facts for the pass.</param>
    /// <returns>The verdict the pass is entitled to reach.</returns>
    /// <remarks>
    /// The gap arm is gated on <see cref="RepoFileVectorIngestOutcome.CoverageEstablished"/>
    /// so that only a <i>measured</i> gap escalates. A pass that never established
    /// coverage reports <c>GapsSelected == 0</c> for the same reason it reports
    /// everything else as zero - it did not look - and reading that zero as
    /// "no gaps" or its absence as "a gap" are both fabrications.
    /// </remarks>
    public static RepoContextCoverageVerdict Classify(bool armFaulted, RepoFileVectorIngestOutcome outcome)
    {
        if (armFaulted)
        {
            return RepoContextCoverageVerdict.ArmFailure;
        }

        if (!outcome.CoverageEstablished || outcome.Deferred || outcome.GapScanSkipped)
        {
            return RepoContextCoverageVerdict.ProbeUnmeasurable;
        }

        return outcome.GapsSelected > 0
            ? RepoContextCoverageVerdict.GapFound
            : RepoContextCoverageVerdict.Converged;
    }

    /// <summary>Records one bootstrap coverage verdict.</summary>
    /// <param name="verdict">What the pass concluded, and why.</param>
    public void Record(RepoContextCoverageVerdict verdict)
    {
        lock (_gate)
        {
            _counts[(int)verdict]++;
        }

        // One emission site per reason, each naming the tag constant literally,
        // rather than one site passing a switch-selected local. The four sites are
        // not duplication and must not be folded back together: the priming gate
        // resolves an instrument's tag domain by reading its emission sites, and a
        // computed tag value is unresolvable, so a single parameterised site leaves
        // the domain ambiguous and the gate skips the instrument in silence.
        switch (verdict)
        {
            case RepoContextCoverageVerdict.Converged:
                _verdicts.Add(
                    1,
                    new KeyValuePair<string, object?>(ReasonTagKey, ReasonConvergedTag),
                    LatticeTenantLabel.Platform);
                break;
            case RepoContextCoverageVerdict.ArmFailure:
                _verdicts.Add(
                    1,
                    new KeyValuePair<string, object?>(ReasonTagKey, ReasonArmFailureTag),
                    LatticeTenantLabel.Platform);
                break;
            case RepoContextCoverageVerdict.GapFound:
                _verdicts.Add(
                    1,
                    new KeyValuePair<string, object?>(ReasonTagKey, ReasonGapFoundTag),
                    LatticeTenantLabel.Platform);
                break;
            default:
                _verdicts.Add(
                    1,
                    new KeyValuePair<string, object?>(ReasonTagKey, ReasonProbeUnmeasurableTag),
                    LatticeTenantLabel.Platform);
                break;
        }
    }

    /// <summary>
    /// Reads the counters accumulated since process start. Present so a test can
    /// assert the partition without standing up a meter listener.
    /// </summary>
    /// <returns>A point-in-time reading of all four arms.</returns>
    public RepoContextCoverageVerdictSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RepoContextCoverageVerdictSnapshot((long[])_counts.Clone());
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}

/// <summary>
/// A point-in-time reading of the coverage-verdict counters, cumulative since
/// process start.
/// </summary>
internal readonly struct RepoContextCoverageVerdictSnapshot
{
    private readonly long[] _counts;

    internal RepoContextCoverageVerdictSnapshot(long[] counts) => _counts = counts;

    /// <summary>Reads one verdict tally.</summary>
    /// <param name="verdict">The verdict whose tally to read.</param>
    /// <returns>How many passes reached that verdict since process start.</returns>
    public long Count(RepoContextCoverageVerdict verdict)
        => _counts is null ? 0 : _counts[(int)verdict];

    /// <summary>
    /// How many verdicts were reached in total, across every reason. One verdict is
    /// published per pass that actually scanned, so this doubles as the count of
    /// passes that looked - which is what distinguishes "scanned and found nothing"
    /// from "did not scan".
    /// </summary>
    public long Total
    {
        get
        {
            if (_counts is null)
            {
                return 0;
            }

            var total = 0L;
            foreach (var count in _counts)
            {
                total += count;
            }

            return total;
        }
    }
}
