namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Per-pin tombstone-retention record persisted by
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain"/> against a point-in-time cursor's
/// saga-decision snapshot. Keeps every txid the snapshot referenced
/// queryable for the lifetime of the pin even as concurrent sagas call
/// <c>ForgetAsync</c>, so a cursor's <c>Next*Async</c> step under
/// <see cref="LatticeRegistrySnapshotContext"/> never observes a
/// tombstoned-then-pruned saga as <see cref="TxStatus.InFlight"/> when
/// the snapshot captured it as <see cref="TxStatus.Committed"/> /
/// <see cref="TxStatus.Aborted"/>.
/// <para>
/// One <see cref="SnapshotPin"/> entry lives in the registry's
/// <c>SnapshotPins</c> map per active <see cref="LatticeCursorSpec.PointInTime"/>
/// cursor, keyed by a server-assigned <c>Guid pinId</c>. The cursor
/// grain refreshes its pin on every step; the registry independently
/// expires pins past <see cref="ExpiresAt"/> via the same prune pass
/// that handles tombstone expiry.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotPin)]
internal sealed class SnapshotPin
{
    /// <summary>
    /// The set of saga txids this pin holds against the registry. Every
    /// entry was present in the cursor's captured snapshot (with a
    /// non-<see cref="TxStatus.InFlight"/> outcome) at the moment the
    /// pin was installed. Stored as <see cref="HashSet{T}"/> for fast
    /// union/intersection during prune and footprint accounting.
    /// </summary>
    [Id(0)] public HashSet<Guid> Txids { get; set; } = [];

    /// <summary>
    /// Wall-clock instant past which the registry may evict this pin
    /// from its <c>SnapshotPins</c> map even without an explicit
    /// <c>UnpinSnapshotAsync</c>. Refreshed by
    /// <c>RefreshPinAsync</c> on every cursor step. A subsequent
    /// cursor step that finds its pin missing throws
    /// <see cref="LatticeCursorSnapshotExpiredException"/>.
    /// </summary>
    [Id(1)] public DateTimeOffset ExpiresAt { get; set; }
}