namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Page of WAL entries returned by <see cref="IReplogShardGrain.ReadAsync"/>.
/// Carries the sequence number to use on the next call so consumers can
/// drive a stateful cursor without inspecting individual entries.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplogShardPage)]
[Immutable]
internal readonly record struct ReplogShardPage
{
    /// <summary>
    /// The page entries in ascending sequence order. May be shorter
    /// than the requested <c>maxEntries</c> when the cursor reaches the
    /// end of the WAL; never <c>null</c>.
    /// </summary>
    [Id(0)] public IReadOnlyList<ReplogShardEntry> Entries { get; init; }

    /// <summary>
    /// The sequence number to pass to the next
    /// <see cref="IReplogShardGrain.ReadAsync"/> call. Equal to the
    /// requested <c>fromSequence</c> when <see cref="Entries"/> is
    /// empty, and to <c>Entries[^1].Sequence + 1</c> otherwise.
    /// </summary>
    [Id(1)] public long NextSequence { get; init; }

    /// <summary>An empty page positioned at <paramref name="atSequence"/>.</summary>
    public static ReplogShardPage Empty(long atSequence) => new()
    {
        Entries = Array.Empty<ReplogShardEntry>(),
        NextSequence = atSequence,
    };
}
