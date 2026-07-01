namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The batched metadata a leaf-split donor stamps onto a freshly created
/// sibling in a single <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.InitializeSiblingAsync"/>
/// round-trip. Collapses the five separate gated setter RPCs the donor
/// used to issue (tree id, shard index, ownership key range, and the
/// next/prev sibling pointers) into one cross-grain call backed by a
/// single gate acquire and a single state persist on the sibling.
/// <para>
/// Every field carries the same idempotent semantics as the setter it
/// replaces: <see cref="TreeId"/> and <see cref="ShardIndex"/> are
/// write-once (a re-call against an already-seeded sibling is a no-op),
/// and the key-range low bound is the seeded sentinel. A
/// <see langword="null"/> <see cref="ShardIndex"/> means the donor's
/// own shard index was unset and the sibling should leave its slot
/// unseeded.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SiblingInitialization)]
[Immutable]
internal readonly record struct SiblingInitialization
{
    /// <summary>The tree id the sibling is associated with (write-once).</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The owning chain-shard index, or <see langword="null"/> when the donor's slot was unseeded.</summary>
    [Id(1)] public int? ShardIndex { get; init; }

    /// <summary>The inclusive low bound of the sibling's ownership range (the split key).</summary>
    [Id(2)] public string? LowKeyInclusive { get; init; }

    /// <summary>The exclusive high bound of the sibling's ownership range (the donor's pre-split high).</summary>
    [Id(3)] public string? HighKeyExclusive { get; init; }

    /// <summary>The sibling's right (next) sibling pointer.</summary>
    [Id(4)] public GrainId? NextSibling { get; init; }

    /// <summary>The sibling's left (prev) sibling pointer (the donor itself).</summary>
    [Id(5)] public GrainId? PrevSibling { get; init; }
}
