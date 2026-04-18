using System.ComponentModel;
using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A page of keys returned from a shard's B+ tree scan.
/// Used for paginated key enumeration to avoid loading all keys into memory.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.KeysPage)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record KeysPage
{
    /// <summary>The keys in this page, in sorted order.</summary>
    [Id(0)] public required List<string> Keys { get; init; }

    /// <summary>Whether more pages are available after this one.</summary>
    [Id(1)] public required bool HasMore { get; init; }

    /// <summary>
    /// Virtual slots whose entries were filtered out of <see cref="Keys"/> by
    /// the source shard because the slot has been (or is being) moved to
    /// another physical shard by an adaptive split (F-011). <c>null</c> or
    /// empty when no such filtering occurred. Strongly-consistent scans
    /// (<c>ILattice.KeysAsync</c>) use this signal to query the new owner
    /// for the missing slots before completing.
    /// </summary>
    [Id(2)] public int[]? MovedAwaySlots { get; init; }
}
