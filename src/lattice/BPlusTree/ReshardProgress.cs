namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The durable intent of a reshard coordinator, returned by
/// <see cref="ITreeReshardGrain.GetProgressAsync"/>. The progress itself is the
/// tree's shard map, which each split updates durably as it commits; this
/// supplies what that map is measured against.
/// </summary>
/// <param name="InProgress"><see langword="true"/> while a reshard is in flight.</param>
/// <param name="TargetShardCount">The physical shard count the reshard grows the tree to, or zero when none is in flight.</param>
/// <param name="StartShardCount">
/// The physical shard count the tree had when the reshard started, or zero when
/// none is in flight or the reshard was started by a build that did not record it.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.ReshardProgress)]
[Immutable]
internal readonly record struct ReshardProgress(
    [property: Id(0)] bool InProgress,
    [property: Id(1)] int TargetShardCount,
    [property: Id(2)] int StartShardCount);
