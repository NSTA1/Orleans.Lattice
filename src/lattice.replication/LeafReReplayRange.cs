namespace Orleans.Lattice.Replication;

/// <summary>
/// A cluster-stable <c>[StartKey, EndKey)</c> covering range of a single leaf
/// the read-only Merkle walk localised as diverging. The targeted leaf
/// re-replay repair stage consumes these ranges to bound which keys it
/// re-ships from the local write-ahead-log to the diverged peer.
/// <para>
/// The range is half-open: <see cref="StartKey"/> is inclusive and
/// <see cref="EndKey"/> is exclusive. A <see langword="null"/> bound denotes
/// unbounded - a <see langword="null"/> <see cref="StartKey"/> is the leftmost
/// leaf of the shard and a <see langword="null"/> <see cref="EndKey"/> is the
/// rightmost. Keys are compared with ordinal (<see cref="System.StringComparison.Ordinal"/>)
/// semantics, matching the B+ tree's separator-key ordering.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LeafReReplayRange)]
[Immutable]
public readonly record struct LeafReReplayRange
{
    /// <summary>
    /// The inclusive start key of the covering range, or <see langword="null"/>
    /// when the range is unbounded on the left (the shard's leftmost leaf).
    /// </summary>
    [Id(0)] public string? StartKey { get; init; }

    /// <summary>
    /// The exclusive end key of the covering range, or <see langword="null"/>
    /// when the range is unbounded on the right (the shard's rightmost leaf).
    /// </summary>
    [Id(1)] public string? EndKey { get; init; }
}
