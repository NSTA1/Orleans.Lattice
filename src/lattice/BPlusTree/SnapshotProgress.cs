using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How far a snapshot coordinator has durably got, returned by
/// <see cref="ITreeSnapshotGrain.GetProgressAsync"/>. Every field is taken from
/// the snapshot state as last persisted, so it never runs ahead of work a
/// reactivated coordinator would resume from.
/// </summary>
/// <param name="InProgress"><see langword="true"/> while a snapshot is in flight.</param>
/// <param name="Complete"><see langword="true"/> once the most recent snapshot has finished.</param>
/// <param name="OperationId">The operation id of the current or most recent snapshot, or <see langword="null"/>.</param>
/// <param name="Phase">The phase of the shard at the head of the copy.</param>
/// <param name="CopiedShardCount">
/// The shards whose copy has durably finished: every position before the head,
/// plus every later position a concurrent online drain has already finished.
/// Zero when no snapshot is in flight.
/// </param>
/// <param name="ShardCount">The shards the snapshot copies, or zero when no snapshot is in flight.</param>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotProgress)]
[Immutable]
internal readonly record struct SnapshotProgress(
    [property: Id(0)] bool InProgress,
    [property: Id(1)] bool Complete,
    [property: Id(2)] string? OperationId,
    [property: Id(3)] SnapshotPhase Phase,
    [property: Id(4)] int CopiedShardCount,
    [property: Id(5)] int ShardCount);
