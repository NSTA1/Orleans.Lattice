using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How one walk of the embedded-memory-key marker range ended. The three values
/// are exhaustive over a walk that was made, which is what lets them be counted as
/// a partition rather than as three unrelated tallies.
/// </summary>
internal enum RepoContextMemoryMarkerScanOutcome
{
    /// <summary>
    /// The walk reached the end of the range in a single pass, having consumed no
    /// banked progress. The steady state on a healthy plane.
    /// </summary>
    Complete = 0,

    /// <summary>
    /// The walk reached the end of the range after consuming progress banked by an
    /// earlier pass that faulted. This is the arm the resumable cursor exists to
    /// produce, and the only positive evidence that it works end to end.
    /// </summary>
    Resumed = 1,

    /// <summary>
    /// A page faulted, so the walk banked the pages already read and will resume
    /// from them on the next pass. The pass itself continues - this is a degraded
    /// reading, not a failed one.
    /// </summary>
    Banked = 2,
}

/// <summary>
/// Meters whether the embedded-memory-key marker scan <b>converges</b> or banks
/// progress forever without ever exhausting its range.
/// <para>
/// <b>Why this exists.</b> The marker scan walks its range in small resumable
/// pages: a page fault banks what has been read and the next reconcile pass
/// resumes from it, so the walk finishes within a bounded number of passes instead
/// of restarting from the beginning and never finishing at all (#2071). Whether it
/// actually converges is currently observable only as the presence or absence of a
/// warning in the host log, and an absence there is not interpretable.
/// </para>
/// <para>
/// <b>The ambiguity this resolves is already named in the source it instruments.</b>
/// The call site records that "the warning stopped" is a much weaker signal than
/// "the range was exhausted", because the warning also stops when the scan is
/// never reached at all. That is a three-state question being read off a one-bit
/// observable. These arms make the same argument machine-readable: all three zero
/// means the scan was never reached, which no log grep can distinguish from a scan
/// that ran and completed.
/// </para>
/// <para>
/// <b>Three arms, not two, and the third is load-bearing.</b> Splitting completion
/// on whether banked progress was consumed keeps
/// <see cref="RepoContextMemoryMarkerScanOutcome.Complete"/> from conflating
/// "never needed to bank" with "banked and recovered". Those answer opposite
/// questions about whether the resumable cursor is live, so folding them together
/// would leave the mechanism unobservable precisely when it is working.
/// <see cref="RepoContextMemoryMarkerScanOutcome.Banked"/> with neither completion
/// arm ever advancing is the thrash the cursor exists to prevent.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host every arm is PRESENT and
/// reads <c>0</c> rather than being absent. Absence then means the build did not
/// ship, which is a different fact from "it shipped and never banked".
/// </para>
/// <para>
/// <b>Deliberately a counter and not a histogram.</b> The interesting quantity is
/// how many walks ended each way, not how long one took. Priming a histogram would
/// fabricate a zero-valued sample, which reads as a real measurement of an
/// instantaneous operation and destroys exactly the distribution such an
/// instrument would exist to report.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tag is the closed outcome set. No
/// repository id, no marker key, no continuation token. A continuation token is a
/// store key, and naming one in a metric label would put corpus content in the
/// metrics endpoint.
/// </para>
/// </summary>
internal sealed class RepoContextMemoryMarkerScanReporter : IDisposable
{
    /// <summary>
    /// The counter of embedded-memory-key marker range walks, partitioned by
    /// whether the walk completed outright, completed by resuming banked progress,
    /// or banked a partial read for the next pass.
    /// </summary>
    internal const string ScanInstrumentName = "repocontext.bootstrap.memory_marker_scan";

    /// <summary>The tag key carrying the outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for a walk that exhausted the range in one pass.</summary>
    internal const string OutcomeCompleteTag = "complete";

    /// <summary>The tag value for a walk that exhausted the range after resuming banked progress.</summary>
    internal const string OutcomeResumedTag = "resumed";

    /// <summary>
    /// The tag value for a walk that banked a partial read for the next pass.
    /// <para>
    /// Deliberately <c>banked</c> and not <c>faulted</c>, which is what the
    /// structurally similar arm on <c>repocontext.ann.index.load</c> is called. The
    /// sibling's exit propagates and its caller sees a failed load, so <c>faulted</c>
    /// is a true description there; here the fault is swallowed and the pass
    /// continues successfully with a usable skip signal, so <c>faulted</c> would make
    /// the scrape assert something false and would be believed precisely because it
    /// matches the sibling. Do not rename this for consistency: matching names are a
    /// virtue only when the thing named is the same thing, and a false equivalence is
    /// harder to detect than an inconsistency because it prompts no question.
    /// </para>
    /// </summary>
    internal const string OutcomeBankedTag = "banked";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _markerScans;

    private readonly Lock _gate = new();
    private long _complete;
    private long _resumed;
    private long _banked;

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextMemoryMarkerScanReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _markerScans = _meter.CreateCounter<long>(
            ScanInstrumentName,
            unit: "{walk}",
            description:
                "Embedded-memory-key marker range walks, partitioned by whether the walk exhausted the range "
                + "outright, exhausted it after resuming progress banked by an earlier pass, or banked a "
                + "partial read for the next pass.");

        // Pre-minted so that every arm is PRESENT and reads zero on a host whose
        // marker scan has simply not faulted yet. An arm that appears only once it
        // is non-zero cannot distinguish a converging scan from one never reached.
        _markerScans.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeCompleteTag),
            LatticeTenantLabel.Platform);
        _markerScans.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
            LatticeTenantLabel.Platform);
        _markerScans.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeBankedTag),
            LatticeTenantLabel.Platform);
    }

    /// <summary>Records one walk of the marker range.</summary>
    /// <param name="outcome">How the walk ended.</param>
    public void Record(RepoContextMemoryMarkerScanOutcome outcome)
    {
        lock (_gate)
        {
            switch (outcome)
            {
                case RepoContextMemoryMarkerScanOutcome.Complete:
                    _complete++;
                    break;
                case RepoContextMemoryMarkerScanOutcome.Resumed:
                    _resumed++;
                    break;
                default:
                    _banked++;
                    break;
            }
        }

        // One emission site per arm, each naming its tag constant literally, rather
        // than one site passing a switch-selected local. The three sites are not
        // duplication and must not be folded back together: the priming gate resolves
        // an instrument's tag domain by reading its emission sites, and a computed tag
        // value is unresolvable, so a single parameterised site leaves the domain
        // ambiguous and the gate SKIPS the instrument silently. That would leave the
        // zero-priming below enrolled but never verified, which is the exact condition
        // the priming rule exists to prevent - and the skip is invisible, because a
        // gate that excludes an instrument passes.
        switch (outcome)
        {
            case RepoContextMemoryMarkerScanOutcome.Complete:
                _markerScans.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeCompleteTag),
                    LatticeTenantLabel.Platform);
                break;
            case RepoContextMemoryMarkerScanOutcome.Resumed:
                _markerScans.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
                    LatticeTenantLabel.Platform);
                break;
            default:
                _markerScans.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeBankedTag),
                    LatticeTenantLabel.Platform);
                break;
        }
    }

    /// <summary>
    /// Reads the counters accumulated since process start. Present so a test can
    /// assert the partition without standing up a meter listener.
    /// </summary>
    public RepoContextMemoryMarkerScanSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RepoContextMemoryMarkerScanSnapshot(_complete, _resumed, _banked);
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}

/// <summary>
/// A point-in-time reading of the marker-scan counters, cumulative since process
/// start.
/// </summary>
/// <param name="Complete">Walks that exhausted the range in a single pass.</param>
/// <param name="Resumed">Walks that exhausted the range after resuming banked progress.</param>
/// <param name="Banked">Walks that banked a partial read for the next pass.</param>
internal readonly record struct RepoContextMemoryMarkerScanSnapshot(
    long Complete,
    long Resumed,
    long Banked);
