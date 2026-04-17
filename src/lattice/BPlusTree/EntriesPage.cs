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
}
