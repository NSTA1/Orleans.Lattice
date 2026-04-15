namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A page of keys returned from a shard's B+ tree scan.
/// Used for paginated key enumeration to avoid loading all keys into memory.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record KeysPage
{
    /// <summary>The keys in this page, in sorted order.</summary>
    [Id(0)] public required List<string> Keys { get; init; }

    /// <summary>Whether more pages are available after this one.</summary>
    [Id(1)] public required bool HasMore { get; init; }
}
