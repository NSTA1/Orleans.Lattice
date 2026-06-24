using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One prepared-but-not-yet-terminal saga mutation carried in a
/// <see cref="LeafBaselineFreeze"/>. Flattens the live leaf's in-memory
/// pending-transaction buckets (keyed by transaction id, then key) into a
/// single serializable row so the frozen baseline can be shipped from the
/// leaf to the shard root and back without round-tripping nested
/// dictionaries.
/// <para>
/// Re-seeding these into the capture-time fold before replaying the
/// <c>(leaf_frontier, capturedHead]</c> tail is load-bearing: a saga whose
/// prepare landed at or before the leaf's frozen frontier (so it lives in the
/// leaf's persisted pending-tx state, not its cache) but whose terminal
/// (commit / abort) lands in the tail would otherwise drain an empty bucket
/// and silently lose the committed write. Seeding the frozen pending bucket
/// first makes the tail's terminal resolve against the real prepared
/// mutation, exactly as a live activation's two-pass replay does.
/// </para>
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LeafBaselinePendingEntry)]
internal readonly record struct LeafBaselinePendingEntry(
    [property: Id(0)] Guid TransactionId,
    [property: Id(1)] string Key,
    [property: Id(2)] LwwValue<byte[]> Value,
    [property: Id(3)] byte[]? Delta,
    [property: Id(4)] LatticeMergeMode Mode);
