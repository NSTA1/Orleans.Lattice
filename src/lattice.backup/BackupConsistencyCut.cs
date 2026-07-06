namespace Orleans.Lattice.Backup;

/// <summary>
/// The consistency-cut coordinates a backup was captured at: the pinned
/// write-ahead-log sequence floor and the causal-stable hybrid-logical-clock
/// frontier that together define the point-in-time boundary of the capture, plus
/// an optional per-origin frontier for a multi-origin (replicated) tree. A restore
/// replays deterministically up to this cut so the recovered tree is a causally
/// consistent snapshot rather than a smear across concurrent writes.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupConsistencyCut)]
[Immutable]
public sealed record BackupConsistencyCut
{
    /// <summary>Initializes a new <see cref="BackupConsistencyCut"/>.</summary>
    /// <param name="walSequence">The pinned write-ahead-log sequence floor of the cut. Must not be negative.</param>
    /// <param name="hlcTimestamp">The causal-stable hybrid-logical-clock frontier of the cut. Must not be negative.</param>
    /// <param name="perOriginFrontier">
    /// Optional per-origin sequence frontier for a replicated tree, keyed by origin
    /// id. <c>null</c> or empty for a single-origin (local-only) tree.
    /// </param>
    /// <param name="walPartitionOffsets">
    /// Optional per-WAL-partition next-offset (head) frontier at the cut, keyed by
    /// partition index. Recorded so an incremental backup can resume a forward WAL
    /// read from exactly this cut on each partition. <c>null</c> for a legacy
    /// manifest captured before the field existed (an incremental resumes from the
    /// start of the WAL in that case).
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="walSequence"/> or <paramref name="hlcTimestamp"/> is negative.</exception>
    public BackupConsistencyCut(
        long walSequence,
        long hlcTimestamp,
        IReadOnlyDictionary<string, long>? perOriginFrontier = null,
        IReadOnlyDictionary<int, long>? walPartitionOffsets = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(walSequence);
        ArgumentOutOfRangeException.ThrowIfNegative(hlcTimestamp);
        WalSequence = walSequence;
        HlcTimestamp = hlcTimestamp;
        PerOriginFrontier = perOriginFrontier;
        WalPartitionOffsets = walPartitionOffsets;
    }

    /// <summary>The pinned write-ahead-log sequence floor of the cut.</summary>
    [Id(0)]
    public long WalSequence { get; init; }

    /// <summary>The causal-stable hybrid-logical-clock frontier of the cut.</summary>
    [Id(1)]
    public long HlcTimestamp { get; init; }

    /// <summary>
    /// The per-origin sequence frontier for a replicated tree, keyed by origin id;
    /// <c>null</c> for a single-origin tree.
    /// </summary>
    [Id(2)]
    public IReadOnlyDictionary<string, long>? PerOriginFrontier { get; init; }

    /// <summary>
    /// The per-WAL-partition next-offset (head) frontier at the cut, keyed by
    /// partition index; <c>null</c> for a manifest captured before this field
    /// existed. An incremental backup resumes its forward WAL read from this
    /// frontier, reading only entries at or beyond each partition's recorded
    /// offset.
    /// </summary>
    [Id(3)]
    public IReadOnlyDictionary<int, long>? WalPartitionOffsets { get; init; }
}
