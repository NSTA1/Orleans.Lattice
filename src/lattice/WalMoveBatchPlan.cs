namespace Orleans.Lattice;

using System.Collections.Immutable;

/// <summary>
/// A read-only preview of a batch
/// <see cref="ILatticeAdmin.ExecuteWalMoveAsync(string, IEnumerable{ValueTuple{int, string}}, WalMoveOptions?, CancellationToken)"/>
/// operation, returned by
/// <see cref="ILatticeAdmin.PlanWalMoveAsync(string, IEnumerable{ValueTuple{int, string}}, CancellationToken)"/>.
/// Wraps one <see cref="WalMovePlan"/> per requested partition so an operator can
/// review the whole batch - the range each partition would copy and whether every
/// target key resolves - before committing. Computing the plan quiesces nothing
/// and changes no placement.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveBatchPlan)]
[Immutable]
public readonly record struct WalMoveBatchPlan
{
    /// <summary>The tree whose partitions would be moved.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// The placement version the batch must compare-and-swap against. Every
    /// per-partition plan in <see cref="Moves"/> shares this version; a move
    /// computed from this plan aborts if the placement changes first.
    /// </summary>
    [Id(1)] public long PlacementVersion { get; init; }

    /// <summary>
    /// One per-partition <see cref="WalMovePlan"/> per requested move, in the
    /// order supplied to the planner.
    /// </summary>
    [Id(2)] public ImmutableArray<WalMovePlan> Moves { get; init; }

    /// <summary>
    /// <see langword="true"/> only when every partition's target provider key
    /// resolves on the silo that produced this plan. A batch with an
    /// unresolvable target fails closed at execution; register the missing key
    /// on every silo first.
    /// </summary>
    [Id(3)] public bool AllTargetsResolvableOnThisSilo { get; init; }
}
