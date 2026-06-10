namespace Orleans.Lattice;

/// <summary>
/// Result of a leaf-level conditional bulk write (see
/// <see cref="BPlusTree.IBPlusLeafGrain.SetManyWherePredicateAsync"/>).
/// <para>
/// A conditional bulk write evaluates a server-side predicate against each
/// key's <b>current</b> stored value (the guard) and commits only the entries
/// whose existing value satisfies the predicate. <see cref="WrittenKeys"/> is
/// the subset of input keys this leaf actually committed, so the coordinator
/// can aggregate the written set across leaves and shards and report it to the
/// caller. <see cref="Split"/> carries the <see cref="BPlusTree.SplitResult"/>
/// produced when the committed entries pushed the leaf above its key budget,
/// or <see langword="null"/> when no split occurred.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ConditionalSetManyResult)]
[Immutable]
internal readonly record struct ConditionalSetManyResult
{
    /// <summary>
    /// The <see cref="BPlusTree.SplitResult"/> produced when the committed
    /// entries overflowed the leaf, or <see langword="null"/> if no split
    /// occurred.
    /// </summary>
    [Id(0)] public BPlusTree.SplitResult? Split { get; init; }

    /// <summary>
    /// The subset of input keys whose current stored value satisfied the guard
    /// predicate and were therefore committed by this leaf. Never
    /// <see langword="null"/>; an empty list means no input key matched.
    /// </summary>
    [Id(1)] public IReadOnlyList<string> WrittenKeys { get; init; }
}
