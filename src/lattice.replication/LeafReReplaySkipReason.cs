namespace Orleans.Lattice.Replication;

/// <summary>
/// Why a targeted leaf re-replay repair pass was skipped without re-shipping
/// any write-ahead-log entries to the diverged peer. Surfaced on
/// <see cref="LeafReReplayOutcome.SkipReason"/> and mapped to the
/// <c>reason</c> tag on
/// <see cref="LatticeReplicationMetrics.LeafReReplaySkipped"/>.
/// </summary>
public enum LeafReReplaySkipReason
{
    /// <summary>The pass was not skipped (it ran, whether or not it re-shipped any entry).</summary>
    None = 0,

    /// <summary>
    /// The repair stage is disabled
    /// (<see cref="LatticeReplicationOptions.LeafReReplayEnabled"/> is
    /// <see langword="false"/>) even though localisation found a divergent leaf.
    /// </summary>
    Disabled = 1,

    /// <summary>
    /// The localised leaf range yielded no write-ahead-log entries to re-ship -
    /// either the localiser produced no ranges, or no retained entry sat in-range
    /// above the peer's high-water-mark cursor.
    /// </summary>
    RangeEmpty = 2,

    /// <summary>
    /// The local write-ahead-log has been garbage-collected past the divergence
    /// point, so the re-replay cannot source the missing entries and attempts no
    /// repair. The digest probe escalates this outcome to the scoped
    /// bootstrap-snapshot fallback, which re-derives the divergent ranges from the
    /// live tree when <see cref="LatticeReplicationOptions.BootstrapFallbackEnabled"/>
    /// is <see langword="true"/>; with the fallback off it remains an operator
    /// alert signal.
    /// </summary>
    WalTrimmed = 3,
}
