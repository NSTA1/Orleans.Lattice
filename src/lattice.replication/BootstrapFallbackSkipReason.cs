namespace Orleans.Lattice.Replication;

/// <summary>
/// Why a scoped bootstrap-snapshot fallback pass was skipped without re-shipping
/// any committed-projection entries to the diverged peer. Surfaced on
/// <see cref="BootstrapFallbackOutcome.SkipReason"/> and mapped to the
/// <c>reason</c> tag on
/// <see cref="LatticeReplicationMetrics.BootstrapFallbackSkipped"/>.
/// </summary>
public enum BootstrapFallbackSkipReason
{
    /// <summary>The pass was not skipped (it ran, whether or not it re-shipped any entry).</summary>
    None = 0,

    /// <summary>
    /// The fallback is disabled
    /// (<see cref="LatticeReplicationOptions.BootstrapFallbackEnabled"/> is
    /// <see langword="false"/>) even though a targeted leaf re-replay reported
    /// the local write-ahead-log had been trimmed past the divergence point.
    /// </summary>
    Disabled = 1,

    /// <summary>
    /// The localiser produced no leaf ranges to scope the snapshot to, so the
    /// fallback had no bounded scope to export.
    /// </summary>
    RangeEmpty = 2,

    /// <summary>
    /// The range-scoped snapshot export yielded no committed-projection entries
    /// in the divergent leaf range (the range is empty on the local tree).
    /// </summary>
    Empty = 3,
}
