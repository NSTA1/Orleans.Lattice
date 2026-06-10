namespace Orleans.Lattice;

/// <summary>
/// One participating tree's slice of a cross-tree atomic write: the logical
/// tree to write into, the key/value entries to commit on that tree, and an
/// optional server-side guard predicate evaluated against each key's pre-saga
/// value during the prepare phase.
/// <para>
/// Passed as a list to
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAcrossTreesAsync"/>.
/// Every <see cref="TreeId"/> in the list must be distinct - a single
/// cross-tree operation touches each tree at most once.
/// </para>
/// <para>
/// <b>Mutability / safe-copy.</b> This type carries mutable reference-typed
/// members (the <see cref="Entries"/> list, each entry's value <c>byte[]</c>,
/// and the optional mutable <see cref="LatticePredicateNode"/>
/// <see cref="Predicate"/>), so it is deliberately <b>not</b> marked
/// <c>[Immutable]</c>. Orleans elides the same-silo deep copy on any
/// <c>[Immutable]</c> type, which would alias the caller's list and buffers
/// straight into the coordinator grain's persisted state; leaving the type
/// copy-eligible forces Orleans to deep-copy it across the grain-proxy
/// boundary. The non-aliasing contract is pinned by
/// <c>ImmutableRecordCopyAliasingTests</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeBatch)]
public readonly record struct LatticeTreeBatch(
    [property: Id(0)] string TreeId,
    [property: Id(1)] List<KeyValuePair<string, byte[]>> Entries,
    [property: Id(2)] LatticePredicateNode? Predicate = null);
