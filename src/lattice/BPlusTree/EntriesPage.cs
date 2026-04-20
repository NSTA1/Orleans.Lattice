using System.ComponentModel;
using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A page of key-value entries returned from a shard's B+ tree scan.
/// Used for paginated entry enumeration to avoid loading all entries into memory.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.EntriesPage)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record EntriesPage
{
    /// <summary>The key-value entries in this page, in sorted key order.</summary>
    [Id(0)] public required List<KeyValuePair<string, byte[]>> Entries { get; init; }

    /// <summary>Whether more pages are available after this one.</summary>
    [Id(1)] public required bool HasMore { get; init; }

    /// <summary>
    /// Virtual slots whose entries were filtered out of <see cref="Entries"/> by
    /// the source shard because the slot has been (or is being) moved to
    /// another physical shard by an adaptive split. <c>null</c> or
    /// empty when no such filtering occurred. Strongly-consistent scans
    /// (<c>ILattice.EntriesAsync</c>) use this signal to query the new owner
    /// for the missing slots before completing.
    /// </summary>
    [Id(2)] public int[]? MovedAwaySlots { get; init; }
}
