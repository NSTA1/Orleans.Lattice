namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A contiguous run of leaf-snapshot segment rows - indices
/// <see cref="Start"/> through <see cref="Start"/> + <see cref="Count"/> - 1 of
/// generation <see cref="Generation"/> - that no manifest references any more and
/// whose deletion is still owed (issue #4383).
/// <para>
/// Recorded on the manifest in the same write that stops referencing the run, so
/// a retirement that fails, or a process that dies before retiring, leaves a
/// durable record the next capture or clear can finish from. A run the manifest
/// still references is never recorded: retiring it would delete a live snapshot.
/// </para>
/// </summary>
/// <param name="Generation">The segment generation the run belongs to.</param>
/// <param name="Start">The first segment index in the run.</param>
/// <param name="Count">The number of segments in the run; always positive.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LeafSnapshotSegmentRange)]
internal readonly record struct LeafSnapshotSegmentRange(
    [property: Id(0)] int Generation,
    [property: Id(1)] int Start,
    [property: Id(2)] int Count);
