namespace Orleans.Lattice;

using System.Collections.Immutable;

/// <summary>
/// The result of a batch
/// <see cref="ILatticeAdmin.ExecuteWalMoveAsync(string, IEnumerable{ValueTuple{int, string}}, WalMoveOptions?, CancellationToken)"/>
/// operation. Wraps one <see cref="WalMoveReceipt"/> per requested partition and
/// the single placement-version transition the batch applied.
/// <para>
/// The batch is all-or-nothing: either every partition flips together under one
/// compare-and-swap (<see cref="PreviousPlacementVersion"/> -&gt;
/// <see cref="NewPlacementVersion"/>) or the whole batch aborts and the placement
/// is unchanged. As with a single move, <b>no source is trimmed</b> - each moved
/// source is retained for reclaim until an explicit
/// <see cref="ILatticeAdmin.ReclaimMovedWalSourceAsync"/> per partition.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveBatchReceipt)]
[Immutable]
public readonly record struct WalMoveBatchReceipt
{
    /// <summary>The tree whose partitions were moved.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The placement version before the batch.</summary>
    [Id(1)] public long PreviousPlacementVersion { get; init; }

    /// <summary>
    /// The placement version after the batch's single compare-and-swap. Equal to
    /// <see cref="PreviousPlacementVersion"/> when nothing was flipped (every
    /// requested partition was already at its target).
    /// </summary>
    [Id(2)] public long NewPlacementVersion { get; init; }

    /// <summary>
    /// One per-partition <see cref="WalMoveReceipt"/> per requested move, in the
    /// order supplied to the executor.
    /// </summary>
    [Id(3)] public ImmutableArray<WalMoveReceipt> Moves { get; init; }

    /// <summary>
    /// The aggregate classification: <see cref="WalMoveOutcome.Moved"/> when at
    /// least one partition's tail was copied and the pin flipped, otherwise
    /// <see cref="WalMoveOutcome.AlreadyAtTarget"/> (every partition was already
    /// pinned to its requested target).
    /// </summary>
    [Id(4)] public WalMoveOutcome Outcome { get; init; }
}
