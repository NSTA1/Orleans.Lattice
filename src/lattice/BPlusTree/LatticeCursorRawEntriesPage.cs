namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A page of raw last-writer-wins entries returned by
/// <see cref="ILatticeCursorGrain.NextRawEntriesAsync"/> from a snapshot
/// cursor. Unlike <see cref="LatticeCursorEntriesPage"/> - which yields only
/// the key/value projection - each element here carries the complete causal
/// envelope (hybrid-logical-clock timestamp, tombstone flag, expiry, origin
/// cluster id, and version vector) via <see cref="LwwEntry"/>. This is an
/// internal seam consumed by the backup capture engine, which must record
/// metadata the public cursor discards.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorRawEntriesPage)]
internal sealed record LatticeCursorRawEntriesPage
{
    /// <summary>
    /// The raw entries in this page, in the cursor's scan order.
    /// </summary>
    [Id(0)] public required IReadOnlyList<LwwEntry> Entries { get; init; }

    /// <summary>
    /// <c>true</c> when the cursor may have more entries to yield. <c>false</c>
    /// only after the cursor has been fully drained.
    /// </summary>
    [Id(1)] public required bool HasMore { get; init; }
}
