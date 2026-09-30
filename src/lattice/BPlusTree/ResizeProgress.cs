using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How far a resize coordinator has durably got, returned by
/// <see cref="ITreeResizeGrain.GetProgressAsync"/>. A resize is measured in
/// work units: one per shard its snapshot copies, then one for each of the
/// three steps after the copy (<see cref="ResizePhase.Swap"/>,
/// <see cref="ResizePhase.Reject"/> and <see cref="ResizePhase.Cleanup"/>).
/// Every unit is counted only once the coordinator has persisted it, so the
/// figures never run ahead of work a reactivated coordinator would resume from.
/// </summary>
/// <param name="InProgress"><see langword="true"/> while a resize is in flight.</param>
/// <param name="Phase">The phase the resize has durably reached.</param>
/// <param name="CompletedUnits">The work units durably finished.</param>
/// <param name="TotalUnits">
/// The work units the resize consists of, or zero when that is not known - when
/// no resize is in flight, or when the resize state predates the shard set.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.ResizeProgress)]
[Immutable]
internal readonly record struct ResizeProgress(
    [property: Id(0)] bool InProgress,
    [property: Id(1)] ResizePhase Phase,
    [property: Id(2)] int CompletedUnits,
    [property: Id(3)] int TotalUnits)
{
    /// <summary>The number of work units after the copy: the alias swap, rejecting the old shards, and retiring the old copy.</summary>
    public const int StepsAfterCopy = 3;
}
