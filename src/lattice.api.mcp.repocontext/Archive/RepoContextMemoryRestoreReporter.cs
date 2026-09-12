using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Publishes what each durable-memory restore attempt did, partitioned by outcome, so
/// the three states a restore can leave the tree in reach a machine-readable surface
/// rather than only a log line.
/// <para>
/// <b>Why a metric and not just the log.</b> The restore outcome already reached a log
/// line before issue #2641, and that was not enough: a state that is computed,
/// recorded on a result object and logged, but never published, answers "is this
/// observable" with a source-level yes while remaining unqueryable by anything that
/// has to act on it. The failure this reports is one an operator has to be told about
/// without knowing to look - a tree left holding a partial import presents as a
/// populated store and will not announce itself.
/// </para>
/// <para>
/// <b>Why every series is pre-minted.</b> An absent series and a series reading zero
/// look identical on a dashboard and are very different claims. Only the second is
/// falsifiable, and the whole family of defects this belongs to is measurands that are
/// never exercised, whose silence therefore reads as good news.
/// </para>
/// </summary>
internal sealed class RepoContextMemoryRestoreReporter : IDisposable
{
    /// <summary>The instrument name carrying the per-outcome restore count.</summary>
    internal const string InstrumentName = "lattice.repocontext.memory.restore";

    /// <summary>The tag key partitioning the count by outcome.</summary>
    internal const string OutcomeTagKey = "outcome";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than publishing
    // an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _restores;

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextMemoryRestoreReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _restores = _meter.CreateCounter<long>(
            InstrumentName,
            unit: "{attempt}",
            description:
                "Durable-memory restore attempts, partitioned by what the attempt did to the memory tree: "
                + "'restored' (a snapshot was imported and the tree was then counted and found to hold at "
                + "least what the snapshot carried), 'partial' (an import wrote records and did not finish, so "
                + "the tree holds strictly more than nothing and strictly less than the archive), "
                + "'nothingtorestore' (the tree already holds records that no incomplete restore put there), "
                + "'notattempted' (restore is off, or no snapshot exists yet), or 'failed' (every candidate "
                + "snapshot was refused and nothing was written). A non-zero 'partial' always warrants an "
                + "operator: the tree is short of the archive, it presents as a populated store, and nothing "
                + "else will report it. Every attempt is counted, so the total is a denominator and a zero on "
                + "'partial' beside a rising total is a measured absence of damage rather than an absent "
                + "measurement.");

        foreach (var outcome in Enum.GetValues<RepoContextMemoryRestoreOutcome>())
        {
            _restores.Add(0, Tag(outcome), LatticeTenantLabel.Platform);
        }
    }

    /// <summary>Records one completed restore attempt.</summary>
    /// <param name="outcome">What the attempt did.</param>
    public void Record(RepoContextMemoryRestoreOutcome outcome) =>
        _restores.Add(1, Tag(outcome), LatticeTenantLabel.Platform);

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private static KeyValuePair<string, object?> Tag(RepoContextMemoryRestoreOutcome outcome) =>
        new(OutcomeTagKey, outcome.ToString().ToLowerInvariant());
}
