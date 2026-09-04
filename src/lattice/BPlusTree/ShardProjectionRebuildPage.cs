namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One work-bounded batch of a shard projection rebuild (see
/// <see cref="IShardRootGrain.RebuildShardProjectionBoundedAsync"/>).
/// <para>
/// The shard rebuilds the projection of a bounded number of leaves and then
/// returns, releasing the non-reentrant shard so other traffic can interleave.
/// The caller drives batches until <see cref="ResumeFromInclusive"/> is
/// <see langword="null"/>, at which point every leaf in the chain has been
/// rebuilt - the same end state the single-call walk reached (issue 1972).
/// </para>
/// <para>
/// <b>Splitting this walk weakens no atomicity guarantee, because it never had
/// one.</b> The rebuild is already documented as applying leaf by leaf, with
/// each leaf independently idempotent and no tree-wide consistency lock, so a
/// caller can already observe some leaves rebuilt and others not - the existing
/// cancellation contract stops the fan-out "before the next leaf" and leaves
/// exactly that partial state behind. A batch boundary is the same partial
/// state arrived at deliberately rather than by cancellation, so the difference
/// is who is waiting, not what is observable.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardProjectionRebuildPage)]
[Immutable]
internal readonly record struct ShardProjectionRebuildPage
{
    /// <summary>Leaves rebuilt by this batch.</summary>
    [Id(0)] public int LeavesRebuilt { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when every leaf in
    /// this shard's chain has been rebuilt.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
