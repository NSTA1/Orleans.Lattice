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
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="walSequence"/> or <paramref name="hlcTimestamp"/> is negative.</exception>
    public BackupConsistencyCut(
        long walSequence,
        long hlcTimestamp,
        IReadOnlyDictionary<string, long>? perOriginFrontier = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(walSequence);
        ArgumentOutOfRangeException.ThrowIfNegative(hlcTimestamp);
        WalSequence = walSequence;
        HlcTimestamp = hlcTimestamp;
        PerOriginFrontier = perOriginFrontier;
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
}
