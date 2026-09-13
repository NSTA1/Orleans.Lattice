using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How one attempt to load the durable approximate index ended. The three values
/// are exhaustive over an attempt that was made, which is what lets them be
/// counted as a partition rather than as three unrelated tallies.
/// </summary>
internal enum RepoContextAnnIndexLoadOutcome
{
    /// <summary>
    /// The attempt started from nothing and completed. Either the first attempt on
    /// a healthy plane, or a deliberate reload.
    /// </summary>
    Fresh = 0,

    /// <summary>
    /// The attempt continued progress banked by an earlier attempt that faulted,
    /// and completed. This is the arm the resumption exists to produce.
    /// </summary>
    Resumed = 1,

    /// <summary>
    /// The attempt faulted partway. Its progress is banked, so the next attempt
    /// should record <see cref="Resumed"/>.
    /// </summary>
    Faulted = 2,
}

/// <summary>
/// Meters whether a faulted approximate-index load is <b>resumed</b> or silently
/// restarted from the beginning.
/// <para>
/// <b>Why this exists.</b> Opening the durable index walks the whole identifier
/// key map, which is O(corpus) and is the single most timeout-prone read on the
/// plane. When that walk faulted, the partially built index was discarded with the
/// instance holding it and the next phase tick reissued the entire walk - so on a
/// tree whose leaves are themselves slow to activate, every attempt regenerated
/// the identical demand and the plane could not converge (#2953). The fix banks
/// the progress; this counter is how anyone can tell that it did.
/// </para>
/// <para>
/// <b>A resumed load is byte-identical to a restarted one in its result.</b> Both
/// end with the same mapping, the same phase and the same vector count, so no
/// existing series anywhere distinguishes them and a regression would be
/// completely silent. The whole value of this instrument is that
/// <see cref="RepoContextAnnIndexLoadOutcome.Resumed"/> is observable at all.
/// </para>
/// <para>
/// <b>It is a two-sided discriminator, and that is deliberate.</b> Counting only
/// resumptions would make a zero ambiguous between "nothing ever faulted, so
/// nothing needed resuming" - the healthy case - and "everything faulted and
/// none of it resumed" - the defect restored. Counting
/// <see cref="RepoContextAnnIndexLoadOutcome.Faulted"/> alongside it makes the
/// pair conclusive: faults with no resumptions is the defect, and no faults at all
/// is health.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host <c>outcome=resumed</c> is
/// present and reads <c>0</c> rather than being absent. Absence then means the
/// build did not ship, which is a different fact from "it shipped and never
/// resumed" - and the epic has already lost a two-sided discriminator it had paid
/// for by leaving arms unprimed (#2952).
/// </para>
/// <para>
/// <b>Deliberately a counter and not a histogram.</b> The interesting quantity is
/// how many attempts resumed, not how long one took. Priming a histogram would
/// fabricate a zero-valued sample, which reads as a real measurement of an
/// instantaneous operation and destroys exactly the distribution the instrument
/// would exist to report.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tag is the closed outcome set. No
/// repository id, no key, no cursor value. A cursor is a store key and naming one
/// in a metric label would put corpus content in the metrics endpoint.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexLoadReporter : IDisposable
{
    /// <summary>
    /// The counter of durable approximate-index load attempts, partitioned by
    /// whether the attempt started fresh, resumed banked progress, or faulted.
    /// </summary>
    internal const string LoadInstrumentName = "repocontext.ann.index.load";

    /// <summary>The tag key carrying the outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for an attempt that started from nothing and completed.</summary>
    internal const string OutcomeFreshTag = "fresh";

    /// <summary>The tag value for an attempt that continued banked progress and completed.</summary>
    internal const string OutcomeResumedTag = "resumed";

    /// <summary>The tag value for an attempt that faulted partway, banking its progress.</summary>
    internal const string OutcomeFaultedTag = "faulted";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _loads;

    private readonly Lock _gate = new();
    private long _fresh;
    private long _resumed;
    private long _faulted;

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextAnnIndexLoadReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _loads = _meter.CreateCounter<long>(
            LoadInstrumentName,
            unit: "{attempt}",
            description:
                "Durable approximate-index load attempts, partitioned by whether the attempt started fresh, "
                + "resumed progress banked by an earlier faulted attempt, or faulted itself.");

        // Pre-minted so that every arm is PRESENT and reads zero on a host that has
        // simply not faulted yet. An arm that appears only once it is non-zero
        // cannot distinguish a healthy plane from a build that never shipped.
        _loads.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeFreshTag),
            LatticeTenantLabel.Platform);
        _loads.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
            LatticeTenantLabel.Platform);
        _loads.Add(
            0,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeFaultedTag),
            LatticeTenantLabel.Platform);
    }

    /// <summary>Records one load attempt.</summary>
    /// <param name="outcome">How the attempt ended.</param>
    public void Record(RepoContextAnnIndexLoadOutcome outcome)
    {
        lock (_gate)
        {
            switch (outcome)
            {
                case RepoContextAnnIndexLoadOutcome.Fresh:
                    _fresh++;
                    break;
                case RepoContextAnnIndexLoadOutcome.Resumed:
                    _resumed++;
                    break;
                default:
                    _faulted++;
                    break;
            }
        }

        // One emission site per arm, each naming its tag constant literally, rather
        // than one site passing a switch-selected local. Not duplication, and it must
        // not be folded back together: the priming gate resolves an instrument's tag
        // domain from its emission sites, so a computed tag leaves the domain ambiguous
        // and the gate SKIPS this instrument entirely - deleting the zero-priming above
        // would then redden nothing. Verified by perturbation: with the single
        // parameterised site, removing all three primed arms left the gate green.
        switch (outcome)
        {
            case RepoContextAnnIndexLoadOutcome.Fresh:
                _loads.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeFreshTag),
                    LatticeTenantLabel.Platform);
                break;
            case RepoContextAnnIndexLoadOutcome.Resumed:
                _loads.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeResumedTag),
                    LatticeTenantLabel.Platform);
                break;
            default:
                _loads.Add(
                    1,
                    new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeFaultedTag),
                    LatticeTenantLabel.Platform);
                break;
        }
    }

    /// <summary>
    /// Reads the counters accumulated since process start. Present so a test can
    /// assert the partition without standing up a meter listener.
    /// </summary>
    public RepoContextAnnIndexLoadSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RepoContextAnnIndexLoadSnapshot(_fresh, _resumed, _faulted);
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}

/// <summary>
/// A point-in-time reading of the load counters, cumulative since process start.
/// </summary>
/// <param name="Fresh">Attempts that started from nothing and completed.</param>
/// <param name="Resumed">Attempts that continued banked progress and completed.</param>
/// <param name="Faulted">Attempts that faulted partway, banking their progress.</param>
internal readonly record struct RepoContextAnnIndexLoadSnapshot(
    long Fresh,
    long Resumed,
    long Faulted);
