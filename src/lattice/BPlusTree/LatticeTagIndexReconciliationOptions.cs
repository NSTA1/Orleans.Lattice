namespace Orleans.Lattice;

/// <summary>
/// Configuration for the per-index background tag-index reconciliation
/// coordinator. Register a named instance to override settings for a specific
/// index:
/// <code>
/// siloBuilder.ConfigureLatticeTagIndexReconciliation("my-index", o => o.Interval = TimeSpan.FromMinutes(15));
/// </code>
/// The unnamed (default) instance applies to every index that does not have a
/// named override. The coordinator resolves the per-index instance via
/// <c>IOptionsMonitor&lt;LatticeTagIndexReconciliationOptions&gt;.Get(indexName)</c>,
/// mirroring the per-tree <see cref="LatticeOptions"/> pattern.
/// </summary>
public class LatticeTagIndexReconciliationOptions
{
    /// <summary>
    /// Whether the background reconciliation coordinator is enabled for the
    /// index. Default <c>true</c>: a tag index reconciles automatically on the
    /// configured cadence with no operator action. Set <c>false</c> to disable
    /// the scheduled backstop entirely (the on-demand <c>ReconcileAsync</c> API
    /// is unaffected).
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Cadence between digest-gated sweeps. Default one hour. Floored at
    /// <see cref="MinimumInterval"/> (the Orleans reminder minimum) when the
    /// schedule reminder is registered, so a smaller value is clamped rather
    /// than rejected.
    /// </summary>
    public TimeSpan Interval { get; set; } = DefaultInterval;

    /// <summary>
    /// Maximum number of covered trees processed per phase-timer tick, bounding
    /// the work a single tick performs so a large index never blocks a grain
    /// turn. Must be at least 1. Default <see cref="DefaultChunkSize"/>.
    /// </summary>
    public int ChunkSize { get; set; } = DefaultChunkSize;

    /// <summary>
    /// Audit-only escape hatch. When <c>true</c> the coordinator probes and
    /// reports digest mismatches but never repairs (no membership rows are
    /// deleted and the digest baseline is not advanced, so a divergent tree
    /// keeps being reported each sweep). Default <c>false</c>.
    /// </summary>
    public bool ProbeOnly { get; set; }

    /// <summary>Default value for <see cref="Interval"/> (one hour).</summary>
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// The smallest interval the schedule reminder honours (the Orleans
    /// reminder minimum). <see cref="Interval"/> is clamped up to this value.
    /// </summary>
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromMinutes(1);

    /// <summary>Default value for <see cref="ChunkSize"/>.</summary>
    public const int DefaultChunkSize = 16;
}
