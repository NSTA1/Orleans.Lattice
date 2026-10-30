using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Test-only facets of a substitute leaf: the separate reads that the leaf's
/// combined walk calls fold into one grain turn, but that are not grain calls
/// themselves. A fixture stubs these (and <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/>
/// and friends) to describe a leaf, and
/// <see cref="LeafSubstituteCombinedCalls.ComposeChainLink"/> answers the
/// combined calls production makes from them. Reach them through
/// <see cref="LeafSubstituteCombinedCalls.Separate"/>.
/// </summary>
internal interface ILeafSeparateCalls
{
    /// <summary>The leaf's persisted owned-key-range bounds.</summary>
    Task<LeafKeyRange> GetKeyRangeAsync();

    /// <summary>The leaf's persisted projection-checkpoint offset.</summary>
    Task<long> GetProjectionCheckpointOffsetAsync();

    /// <summary>The moved-slot delta of a split or consolidation drain.</summary>
    Task<StateDelta> GetDeltaSinceForSlotsAsync(VersionVector sinceVersion, int[] sortedMovedSlots, int virtualShardCount);

    /// <summary>The snapshot-baseline freeze of the leaf's projection.</summary>
    Task<LeafBaselineFreeze> FreezeProjectionAsync(CancellationToken cancellationToken);

    /// <summary>The projection reset of an operator rebuild.</summary>
    Task RebuildProjectionFromWalAsync();
}
