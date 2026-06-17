namespace Orleans.Lattice;

/// <summary>
/// Per-view tuning, resolved through the named-options pattern
/// (<c>IOptionsMonitor&lt;LatticeViewOptions&gt;.Get(viewName)</c>), mirroring
/// how replication resolves <c>LatticeReplicationOptions</c> per tree. The
/// unnamed (default) instance applies to every view that has no named override.
/// <para>
/// Phase 1 reads <see cref="BatchSize"/> and <see cref="CoalesceWindow"/>; the
/// remaining members are reserved for later phases and are accepted but unused.
/// </para>
/// </summary>
public sealed class LatticeViewOptions
{
    /// <summary>Default <see cref="BatchSize"/> (256 entries per drain pass).</summary>
    public const int DefaultBatchSize = 256;

    /// <summary>Default <see cref="CoalesceWindow"/> (50 ms between idle poll passes).</summary>
    public static readonly TimeSpan DefaultCoalesceWindow = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Maximum number of source WAL entries the maintainer reads and applies in a
    /// single drain pass per source shard before checkpointing. Must be positive;
    /// the registered validator rejects a non-positive value at first resolve.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    /// Idle poll cadence: how long the maintainer waits before re-checking the
    /// source WAL for new entries once it has drained to the head. Also bounds how
    /// long repeated writes to the same view key are batched together for
    /// last-writer-wins coalescing. Must be greater than zero.
    /// </summary>
    public TimeSpan CoalesceWindow { get; set; } = DefaultCoalesceWindow;
}
