namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="ILattice.OpenKeyCursorAsync"/>,
/// <see cref="ILattice.OpenEntryCursorAsync"/> or their WherePredicate
/// variants when <see cref="LatticeCursorSpec.PointInTime"/> is <c>true</c>
/// and accepting the cursor's saga-decision snapshot would push a touched
/// saga-decision registry shard over
/// <see cref="LatticeOptions.MaxPinnedSagaDecisions"/>. The new cursor is not
/// opened, though a multi-shard snapshot can leave its pin on shards that
/// accepted their part; existing point-in-time cursors continue paging.
/// Callers either retry once a peer cursor closes, reduce the cap, or
/// fall back to a non-point-in-time cursor.
/// </summary>
public sealed class LatticeCursorRegistryPinExhaustedException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LatticeCursorRegistryPinExhaustedException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LatticeCursorRegistryPinExhaustedException(string message, Exception innerException) : base(message, innerException) { }
}