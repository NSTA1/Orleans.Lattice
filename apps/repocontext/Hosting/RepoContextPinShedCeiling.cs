using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Bounds how long a single durable pin shard may shed steady-state pin reports
/// <b>continuously</b> before one report is forced through regardless. The
/// library ships this disarmed and is right to; this host arms it because issue
/// #3310 was measured here, on this deployment's own trees.
/// </summary>
/// <remarks>
/// <para>
/// The defect this closes: <c>LeafCursorReporter</c> sheds the steady-state
/// per-checkpoint pin write under durable pin-store pressure (issue #2014), and
/// that shed path is the <b>only</b> one carrying an advancing
/// <c>CheckpointOffset</c>. The non-sheddable birth-seed and deactivation-flush
/// paths feed the same pressure signal they are exempt from, so under continuous
/// churn the window re-arms faster than it lapses and never closes. The durable
/// pin offset then freezes, the WAL GC offset floor has nothing fresher to stand
/// on, and retained WAL grows without bound while the checkpoint runs away above
/// it. On <c>repo-context-vector-index</c> the floor sat frozen at one offset for
/// forty minutes while the WAL added 175 MB, and the shed counter for that tree
/// read 12,987 against 12, 2 and 0 on its siblings.
/// </para>
/// <para>
/// Why forcing is safe, and why it is emphatically <b>not</b> the issue #3300
/// failure in disguise: this does not lower, weaken, or bypass the retention
/// floor. It only makes an already-true value reach the pin store sooner. The
/// leaf clamps every reported offset to <c>min(checkpoint, covered)</c> in
/// <c>ResolveDurablePinForPartition</c> before the report is constructed, so a
/// forced report is arithmetically incapable of publishing an offset the leaf
/// has not proven durable. Issue #3300 is the same seam releasing WAL it cannot
/// prove durable; fixing growth by dropping a floor would convert this bug into
/// that one, so the bound is imposed on <em>when</em> a true value is published
/// and never on <em>what</em> is published.
/// </para>
/// <para>
/// The real hazard of over-arming is the opposite one. Too small a ceiling
/// forces reports through faster than the pin store can absorb them and
/// re-saturates the queue that issue #2012 shedding exists to protect, so the
/// value is a ceiling on <em>stall</em>, not a target. At the default, each pin
/// shard contributes at most one forced write per period, which against eight
/// shards is a handful of writes a minute - negligible against the thousands of
/// reports a busy tree sheds in the same window.
/// </para>
/// <para>
/// Rollback is setting the variable to <c>0</c>, which disarms the ceiling and
/// restores the pre-#3310 behaviour exactly. That is a rollback to unbounded
/// retention growth rather than to data loss, which is the correct direction for
/// this seam to fail.
/// </para>
/// </remarks>
public static class RepoContextPinShedCeiling
{
    /// <summary>Environment variable overriding the pin-shed ceiling, in seconds.</summary>
    public const string PinShedCeilingKey = "LATTICE_WAL_PIN_SHED_CEILING_SECONDS";

    /// <summary>
    /// The ceiling this host applies when <see cref="PinShedCeilingKey"/> is unset,
    /// in seconds. Two minutes bounds the durable-floor stall to roughly one WAL GC
    /// cadence plus the ceiling, while keeping the forced-write duty cycle at one
    /// report per pin shard per two minutes - far below the rate at which forcing
    /// could itself become the pressure it is relieving.
    /// </summary>
    public const int DefaultPinShedCeilingSeconds = 120;

    /// <summary>
    /// The largest accepted ceiling, in seconds. An hour is already far longer than
    /// any plausible pin-store recovery, so a larger value is indistinguishable from
    /// disarming and is better expressed as <c>0</c>, which says so explicitly.
    /// </summary>
    public const int MaxPinShedCeilingSeconds = 3600;

    /// <summary>
    /// Resolves the ceiling in seconds from <paramref name="configuration"/>, falling
    /// back to <see cref="DefaultPinShedCeilingSeconds"/> when
    /// <see cref="PinShedCeilingKey"/> is absent or blank. A value of <c>0</c> is
    /// accepted and means disarmed.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved ceiling in seconds; <c>0</c> means disarmed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The host
    /// refuses to start rather than silently ignoring an operator's intent - a typo
    /// that quietly resolved to the default would leave the deployment reporting a
    /// ceiling nobody chose.
    /// </exception>
    public static int ResolveCeilingSeconds(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[PinShedCeilingKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultPinShedCeilingSeconds;
        }

        if (!int.TryParse(raw.Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            || parsed < 0
            || parsed > MaxPinShedCeilingSeconds)
        {
            throw new InvalidOperationException(
                $"{PinShedCeilingKey} must be an integer between 0 and {MaxPinShedCeilingSeconds} (0 disarms the ceiling); was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved ceiling as a <b>global</b> Lattice option on the silo.
    /// Deliberately global rather than per-tree, matching
    /// <see cref="RepoContextPinBucketing"/>: the shed window is keyed by pin shard,
    /// and a pin shard answers for a set of consumers that can span trees, so a
    /// per-tree ceiling would be applied to a gate no single tree owns.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextPinShedCeiling(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var seconds = ResolveCeilingSeconds(configuration);
        silo.ConfigureLattice(options => options.WalMaterialiserPinShedCeiling =
            seconds <= 0 ? null : TimeSpan.FromSeconds(seconds));
        return silo;
    }
}
