using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Which bootstrap ingestion path resolved the coverage this reading describes.
/// <para>
/// The three arms are <b>consumers</b> of one membership read, not three separate
/// grants: <see cref="RepoContextVectorWriter.ProbeCoverageAsync"/>,
/// <see cref="RepoContextVectorWriter.ProbeEmbeddedMembersAsync"/> and
/// <see cref="RepoContextVectorWriter.ProbeCoveredSourceIdsAsync"/> all funnel
/// through the same bounded membership probe. So the arm says which ingestion
/// path stood down and therefore what is degraded; it does <b>not</b> localise
/// which grant is missing, because there is only one.
/// </para>
/// </summary>
internal enum RepoContextCoverageProbeArm
{
    /// <summary>
    /// The file arm's per-pass coverage resolution. On a pruned read the gap sweep
    /// over unchanged files stands down for the pass and only changed files are
    /// embedded.
    /// </summary>
    File = 0,

    /// <summary>
    /// The symbol arm's per-page coverage resolution. On a pruned read the page is
    /// skipped rather than re-embedded, so this arm advances once per page and not
    /// once per pass.
    /// </summary>
    Symbol = 1,

    /// <summary>
    /// The always-on self-heal sweep's per-page coverage resolution. The strongest
    /// of the three: on a pruned read the sweep does not merely fall silent, it
    /// ends the walk and returns a positive <c>GapFound: false</c> that the
    /// self-index grain then consumes as a control decision.
    /// </summary>
    Sweep = 2,
}

/// <summary>
/// How one bootstrap coverage resolution ended. The three values are exhaustive
/// over a resolution that was attempted, which is what lets them be counted as a
/// partition rather than as three unrelated tallies.
/// </summary>
internal enum RepoContextCoverageProbeOutcome
{
    /// <summary>
    /// Coverage was resolved and its absences are conclusive, so the arm can
    /// classify. The steady state on a correctly granted deployment.
    /// <para>
    /// This means "the arm resolved coverage it can trust", <b>not</b> "a network
    /// probe succeeded": the file arm may be served by the per-page coverage digest
    /// rather than by a per-source membership probe, and both resolve conclusively.
    /// </para>
    /// </summary>
    Conclusive = 0,

    /// <summary>
    /// The read answered, but the store's read-path access gate removed keys before
    /// fan-out, so absence from the result is not evidence of a missing embedding
    /// (issue #2277) and the arm stands down. <b>This does not clear on the next
    /// pass.</b> A non-zero value here is a positive, measured signal of a standing
    /// misconfiguration.
    /// </summary>
    GatePruned = 1,

    /// <summary>
    /// The coverage read failed outright, so the gate check below it was never
    /// evaluated. Transient and self-clearing in itself; recorded because it is
    /// what makes a zero on <see cref="GatePruned"/> readable. See the reporter's
    /// remarks.
    /// </summary>
    ProbeFailed = 2,
}

/// <summary>
/// Meters whether the store's read-path access gate is standing bootstrap
/// ingestion coverage <b>down</b>, and does so in a way that distinguishes a
/// measured zero from a seam that was never reached.
/// <para>
/// <b>Why this exists.</b> Three ingestion paths resolve embedding coverage before
/// deciding what to re-embed. When the access gate prunes keys from that read, each
/// stands down rather than guessing - which is the correct and safe behaviour - but
/// the condition <b>never clears on its own</b>, and nothing else about the
/// deployment looks wrong. Read from the scrape alone, a repository in this state
/// produces a flat demand curve indistinguishable from a converged, healthy
/// bootstrap, so the failure is not merely undiagnosable but affirmatively
/// misreported as success (issue #2964).
/// </para>
/// <para>
/// <b>The <see cref="RepoContextCoverageProbeArm.Sweep"/> arm is the strongest of
/// the three and is worth naming separately.</b> The file and symbol arms stand
/// down by falling silent, and the silence is only later misread as health by a
/// human. The sweep instead ends its walk and returns <c>GapFound: false</c> - a
/// positive claim of health manufactured by the failure, which the self-heal grain
/// consumes as a control decision with no reader involved at all. A fabricated
/// affirmative that a machine acts on is a category above an absence a reader
/// misinterprets.
/// </para>
/// <para>
/// <b>Why <see cref="RepoContextCoverageProbeOutcome.ProbeFailed"/> is an arm here
/// even though a probe failure is transient.</b> At every one of the three sites
/// the gate check sits structurally <b>below</b> the probe-failure branch: the file
/// arm conjoins the two in one condition, the symbol arm continues to the next page
/// before reaching the check, and the sweep's throw is caught a frame up. So if the
/// coverage read is failing, the gate-pruned arm reads a flat zero forever and that
/// zero looks exactly like health. Without this arm the instrument could be zero for
/// two opposite reasons - nothing was pruned, or nothing was ever evaluated - which
/// is the very defect family it was written to remove.
/// </para>
/// <para>
/// <b>The four-way read this instrument exists to support.</b>
/// </para>
/// <list type="bullet">
/// <item><description><b>All nine series zero</b> - no bootstrap coverage
/// resolution was reached at all. Not health; no evidence either way.</description></item>
/// <item><description><b><c>conclusive</c> only</b> - the arms resolved coverage
/// they can trust. Healthy.</description></item>
/// <item><description><b><c>gate_pruned</c> greater than zero</b> - a standing
/// misconfiguration, actionable now, and it will not clear by waiting. The ingestor
/// must be able to read its own membership keys.</description></item>
/// <item><description><b><c>probe_failed</c> greater than zero with
/// <c>gate_pruned</c> at zero <i>in the same arm</i></b> - that zero <b>proves
/// nothing</b>, because the gate check is downstream of the failure and was never
/// evaluated.</description></item>
/// </list>
/// <para>
/// <b>What this instrument does not claim.</b> It covers the three sites that
/// genuinely stand an ingestion arm down. Four further sites branch on the same
/// gate-pruning condition and are deliberately excluded: the memory arm backstops
/// with its marker set and continues, the back-fill read-back suppresses only a
/// diagnostic line, and both coverage-digest sites fall back to a sound path. Their
/// absence from this instrument is <b>not</b> evidence that the gate is not pruning
/// there.
/// </para>
/// <para>
/// <b>All arms are pre-minted, on the same path they are charged from.</b> Every one
/// of the nine series is created with a zero-valued add in the constructor, so on a
/// correctly configured host every arm is PRESENT and reads <c>0</c>. Absence then
/// means the build did not ship, which is a different fact from "it shipped and
/// nothing was pruned".
/// </para>
/// <para>
/// <b>Deliberately a counter and not a histogram.</b> The quantity of interest is
/// how many resolutions ended each way, not how long one took. Priming a histogram
/// would fabricate a zero-valued sample that reads as a real measurement of an
/// instantaneous operation, destroying the distribution such an instrument would
/// exist to report.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tags are the two closed sets below.
/// No repository id, no probed key, no continuation token: a key is corpus content
/// and naming one in a metric label would put it on the metrics endpoint.
/// </para>
/// </summary>
internal sealed class RepoContextCoverageProbeReporter : IDisposable
{
    /// <summary>
    /// The counter of bootstrap coverage resolutions, partitioned by the ingestion
    /// arm that resolved and by whether the result was trustworthy, pruned by the
    /// store's read-path access gate, or never obtained.
    /// </summary>
    internal const string ProbeInstrumentName = "repocontext.bootstrap.coverage_probe";

    /// <summary>The tag key carrying the ingestion-arm partition.</summary>
    internal const string ArmTagKey = "arm";

    /// <summary>The tag value for the file arm's per-pass coverage resolution.</summary>
    internal const string ArmFileTag = "file";

    /// <summary>The tag value for the symbol arm's per-page coverage resolution.</summary>
    internal const string ArmSymbolTag = "symbol";

    /// <summary>The tag value for the self-heal sweep's per-page coverage resolution.</summary>
    internal const string ArmSweepTag = "sweep";

    /// <summary>The tag key carrying the outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for a resolution whose absences can be trusted.</summary>
    internal const string OutcomeConclusiveTag = "conclusive";

    /// <summary>
    /// The tag value for a resolution the store's read-path access gate pruned, so
    /// the arm stood down. The standing condition this instrument exists to surface.
    /// </summary>
    internal const string OutcomeGatePrunedTag = "gate_pruned";

    /// <summary>
    /// The tag value for a resolution that failed before the gate check could be
    /// evaluated. Present so that a zero on <see cref="OutcomeGatePrunedTag"/> in the
    /// same arm is readable rather than ambiguous.
    /// </summary>
    internal const string OutcomeProbeFailedTag = "probe_failed";

    private const int ArmCount = 3;
    private const int OutcomeCount = 3;

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than publishing
    // an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _probes;

    private readonly Lock _gate = new();
    private readonly long[] _counts = new long[ArmCount * OutcomeCount];

    /// <summary>Creates the reporter, its instrument, and every one of its nine series.</summary>
    public RepoContextCoverageProbeReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _probes = _meter.CreateCounter<long>(
            ProbeInstrumentName,
            unit: "{probe}",
            description:
                "Bootstrap embedding-coverage resolutions, partitioned by the ingestion arm that resolved "
                + "and by whether the result was conclusive, answered but pruned by the store's read-path "
                + "access gate, or never obtained because the read failed.");

        // Every one of the nine series is pre-minted here, at the single seam that
        // also charges them, so an absent series means the build did not ship and a
        // zero means the seam was reached and nothing was pruned. Priming some arms
        // here and others at their call sites would destroy exactly that distinction,
        // which is the whole value of the instrument.
        //
        // Each priming names BOTH tag values literally, and the nine are not folded
        // into a loop or a helper taking the arm as a parameter, for the same reason
        // Record below is nine literal cases: the priming-enrolment gate resolves a
        // tag value by reading the call site, and discards any pair whose value it
        // cannot resolve to exactly one literal. A helper primes the series perfectly
        // at run time and presents NOTHING to the gate, so the enrolment would be
        // demoted and the instrument would silently lose its guard - a failure whose
        // symptom is a green build.
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag), LatticeTenantLabel.Platform);
        _probes.Add(0, new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag), new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records one bootstrap coverage resolution.
    /// </summary>
    /// <param name="arm">The ingestion path that resolved coverage.</param>
    /// <param name="outcome">Whether the resolution was trustworthy, gate-pruned, or failed.</param>
    public void Record(RepoContextCoverageProbeArm arm, RepoContextCoverageProbeOutcome outcome)
    {
        lock (_gate)
        {
            _counts[Index(arm, outcome)]++;
        }

        // One emission site per (arm, outcome) pair, each naming both tag constants
        // literally, rather than one site passing switch-selected locals. The nine
        // sites are not duplication and must NOT be folded back together: the priming
        // gate resolves an instrument's tag domain by reading its emission sites, and
        // a computed tag value is unresolvable, so a single parameterised site leaves
        // the domain ambiguous and the gate SKIPS the instrument in silence. That
        // would leave the priming above enrolled but never verified - the exact
        // condition the priming rule exists to prevent - and the skip is invisible,
        // because a gate that excludes an instrument passes.
        switch (arm, outcome)
        {
            case (RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.Conclusive):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.GatePruned):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.ProbeFailed):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmFileTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.Symbol, RepoContextCoverageProbeOutcome.Conclusive):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.Symbol, RepoContextCoverageProbeOutcome.GatePruned):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.Symbol, RepoContextCoverageProbeOutcome.ProbeFailed):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSymbolTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.Conclusive):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeConclusiveTag),
                    LatticeTenantLabel.Platform);
                break;
            case (RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.GatePruned):
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeGatePrunedTag),
                    LatticeTenantLabel.Platform);
                break;
            default:
                _probes.Add(
                    1,
                    new KeyValuePair<string, object?>(ArmTagKey, ArmSweepTag),
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeProbeFailedTag),
                    LatticeTenantLabel.Platform);
                break;
        }
    }

    /// <summary>
    /// Reads the counters accumulated since process start. Present so a test can
    /// assert the partition without standing up a meter listener.
    /// </summary>
    public RepoContextCoverageProbeSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RepoContextCoverageProbeSnapshot((long[])_counts.Clone());
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private static int Index(RepoContextCoverageProbeArm arm, RepoContextCoverageProbeOutcome outcome)
        => ((int)arm * OutcomeCount) + (int)outcome;
}

/// <summary>
/// A point-in-time reading of the coverage-probe counters, cumulative since process
/// start.
/// </summary>
internal readonly struct RepoContextCoverageProbeSnapshot
{
    private readonly long[] _counts;

    internal RepoContextCoverageProbeSnapshot(long[] counts) => _counts = counts;

    /// <summary>Reads one arm-and-outcome tally.</summary>
    /// <param name="arm">The ingestion path.</param>
    /// <param name="outcome">The resolution outcome.</param>
    /// <returns>How many resolutions on that arm ended that way since process start.</returns>
    public long Count(RepoContextCoverageProbeArm arm, RepoContextCoverageProbeOutcome outcome)
        => _counts is null ? 0 : _counts[((int)arm * 3) + (int)outcome];
}
