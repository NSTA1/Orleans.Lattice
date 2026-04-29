namespace Orleans.Lattice;

/// <summary>
/// Surfaced when a leaf grain''s persisted projection checkpoint is
/// stale relative to the per-shard WAL and the configured
/// <see cref="ProjectionRebuildPolicy"/> elects to surface the
/// condition rather than recover automatically. Callers respond by
/// invoking the operator surface to drive an explicit rebuild
/// (the operator rebuild API) or by
/// reconfiguring the option to <see cref="ProjectionRebuildPolicy.SnapshotThenWal"/>
/// and reactivating the leaf.
/// <para>
/// Three triggers can produce this exception: (1) the WAL has been
/// trimmed past the leaf''s persisted projection checkpoint;
/// (2) the gap between the persisted checkpoint and the WAL head
/// exceeds <see cref="LatticeOptions.MaxLeafReplayEntries"/>; or
/// (3) the persisted checkpoint is older than
/// <see cref="LatticeOptions.LeafProjectionRetention"/>.
/// </para>
/// </summary>
public sealed class LeafProjectionStaleException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LeafProjectionStaleException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LeafProjectionStaleException(string message, Exception innerException) : base(message, innerException) { }
}
