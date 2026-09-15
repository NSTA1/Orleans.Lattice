using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How one walk of the symbol arm's whole-symbol-space range ended. The three
/// values are exhaustive over a walk that was made, which is what lets them be
/// counted as a partition rather than as three unrelated tallies.
/// </summary>
internal enum RepoContextSymbolWalkOutcome
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
    /// A page faulted, so the walk banked the continuation token it had reached and
    /// the next pass resumes from it instead of restarting at the range head.
    /// </summary>
    Banked = 2,
}

/// <summary>
/// Meters whether the symbol arm's whole-symbol-space walk <b>converges</b> or
/// restarts from the range head on every pass without ever exhausting itself.
/// <para>
/// <b>Why this exists.</b> The symbol arm selects work by paging the entire symbol
/// range. Before issue #2953 that walk began at <c>token = null</c> on every
/// reconcile pass, so a page fault - which on a cold, WAL-replay-bound tree is the
/// normal outcome, not the exceptional one - discarded every page already read. The
/// next pass then re-issued exactly the same leaf reads against exactly the same
/// cold leaves, which is itself the permit demand that keeps those leaves cold. The
/// walk therefore consumed an unbounded number of passes without ever finishing,
/// and the re-drive regenerated the demand it had just consumed.
/// </para>
/// <para>
/// <b>The ambiguity this resolves.</b> A resumable walk that silently restarts is
/// byte-for-byte indistinguishable, from outside, from one that never resumed: both
/// show a pass that read pages and did not finish. Nothing in the arm's existing
/// logs or counters separates them, so the cursor would be unfalsifiable exactly
/// where it matters. These arms make the distinction machine-readable.
/// </para>
/// <para>
/// <b>Three arms, not two, and the third is load-bearing.</b> Splitting completion
/// on whether banked progress was consumed keeps
/// <see cref="RepoContextSymbolWalkOutcome.Complete"/> from conflating "never needed
/// to bank" with "banked and recovered". Those answer opposite questions about
/// whether the cursor is live, so folding them together would leave the mechanism
/// unobservable precisely when it is working.
/// <see cref="RepoContextSymbolWalkOutcome.Banked"/> advancing while neither
/// completion arm ever does is the non-convergence the cursor exists to prevent,
/// and is the reading that falsifies the fix.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host every arm is PRESENT and
/// reads <c>0</c> rather than being absent. Absence then means the build did not
/// ship, which is a different fact from "it shipped and never banked". This is the
/// trap issue #2938 records: an unarmed arm and a genuinely zero one scrape
/// identically, so a zero is evidence only once the arm is known to be armed.
/// </para>
/// <para>
/// <b>Deliberately a counter and not a histogram.</b> The interesting quantity is
/// how many walks ended each way, not how long one took. Priming a histogram would
/// fabricate a zero-valued sample, which reads as a real measurement and destroys
/// exactly the distribution such an instrument would exist to report.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tag is the closed outcome set. No
/// repository id, no symbol key, no continuation token. A continuation token is a
/// store key, and naming one in a metric label would put corpus content in the
/// metrics endpoint.
/// </para>
/// </summary>
internal sealed class RepoContextSymbolWalkReporter : IDisposable
{
    /// <summary>
    /// The counter of symbol-space range walks, partitioned by whether the walk
    /// completed outright, completed by resuming banked progress, or banked a
    /// partial read for the next pass.
    /// </summary>
    internal const string WalkInstrumentName = "repocontext.bootstrap.symbol_walk";

    /// <summary>The tag key carrying the outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for a walk that exhausted the range in one pass.</summary>
    internal const string OutcomeCompleteTag = "complete";

    /// <summary>The tag value for a walk that exhausted the range after resuming banked progress.</summary>
    internal const string OutcomeResumedTag = "resumed";

    /// <summary>
    /// The tag value for a walk that banked its continuation token for the next
    /// pass.
    /// <para>
    /// Deliberately <c>banked</c> and not <c>faulted</c>, matching the sibling
    /// <c>repocontext.bootstrap.memory_marker_scan</c> rather than
    /// <c>repocontext.ann.index.load</c>. The distinction is real: this arm records
    /// that progress was PRESERVED, which is a statement about the cursor, not about
    /// whether the caller saw a failure. The pass does still surface its fault to the
    /// bootstrap run - see the arm's own source - so reading this arm as "the pass
    /// succeeded" would be wrong in the other direction.
    /// </para>
    /// </summary>
    internal const string OutcomeBankedTag = "banked";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _symbolWalks;

    private readonly Lock _gate = new();
    private long _complete;
    private long _resumed;
    private long _banked;

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextSymbolWalkReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _symbolWalks = _meter.CreateCounter<long>(
            WalkInstrumentName,
            unit: "{walk}",
            description:
                "Symbol-space range walks by the bootstrap symbol arm, partitioned by whether the walk exhausted "
                + "the range outright, exhausted it after resuming progress banked by an earlier pass, or banked "
                + "its continuation token for the next pass.");

        // Pre-minted so that every arm is PRESENT and reads zero on a host whose
        // symbol walk has simply not faulted yet. An arm that appears only once it
        // is non-zero cannot distinguish a converging walk from one never reached.
        _symbolWalks.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeCompleteTag),
            LatticeTenantLabel.Platform);
        _symbolWalks.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
            LatticeTenantLabel.Platform);
        _symbolWalks.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeBankedTag),
            LatticeTenantLabel.Platform);
    }

    /// <summary>Records one walk of the symbol range.</summary>
    /// <param name="outcome">How the walk ended.</param>
    public void Record(RepoContextSymbolWalkOutcome outcome)
    {
        lock (_gate)
        {
            switch (outcome)
            {
                case RepoContextSymbolWalkOutcome.Complete:
                    _complete++;
                    break;
                case RepoContextSymbolWalkOutcome.Resumed:
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
        // zero-priming above enrolled but never verified, which is the exact condition
        // the priming rule exists to prevent - and the skip is invisible, because a
        // gate that excludes an instrument passes.
        switch (outcome)
        {
            case RepoContextSymbolWalkOutcome.Complete:
                _symbolWalks.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeCompleteTag),
                    LatticeTenantLabel.Platform);
                break;
            case RepoContextSymbolWalkOutcome.Resumed:
                _symbolWalks.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
                    LatticeTenantLabel.Platform);
                break;
            default:
                _symbolWalks.Add(
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
    public RepoContextSymbolWalkSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RepoContextSymbolWalkSnapshot(_complete, _resumed, _banked);
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}

/// <summary>
/// A point-in-time reading of the symbol-walk counters, cumulative since process
/// start.
/// </summary>
/// <param name="Complete">Walks that exhausted the range in a single pass.</param>
/// <param name="Resumed">Walks that exhausted the range after resuming banked progress.</param>
/// <param name="Banked">Walks that banked a continuation token for the next pass.</param>
internal readonly record struct RepoContextSymbolWalkSnapshot(
    long Complete,
    long Resumed,
    long Banked);
