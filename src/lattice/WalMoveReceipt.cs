namespace Orleans.Lattice;

/// <summary>
/// The result of an <see cref="ILatticeAdmin.ExecuteWalMoveAsync"/> (or
/// <see cref="ILatticeAdmin.ReclaimMovedWalSourceAsync"/>) operation. Records
/// the copied range, the source and target tails, and the new placement version
/// so an operator can audit the cutover.
/// <para>
/// <b>The source is never trimmed by a move.</b> After a successful move the
/// source partition still holds the copied entries (<see cref="SourceRetained"/>
/// is <see langword="true"/>); reclaiming that orphaned range is a separate,
/// explicit <see cref="ILatticeAdmin.ReclaimMovedWalSourceAsync"/> call so a
/// move can always be reverted by moving the partition back before the source
/// is discarded.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveReceipt)]
[Immutable]
public readonly record struct WalMoveReceipt
{
    /// <summary>The tree whose partition was moved.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The WAL partition that was moved.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>The provider key the partition was moved from.</summary>
    [Id(2)] public string FromProviderKey { get; init; }

    /// <summary>The provider key the partition was moved to.</summary>
    [Id(3)] public string ToProviderKey { get; init; }

    /// <summary>The placement version before the move.</summary>
    [Id(4)] public long PreviousPlacementVersion { get; init; }

    /// <summary>The placement version after the move's compare-and-swap.</summary>
    [Id(5)] public long NewPlacementVersion { get; init; }

    /// <summary>The first offset copied to the target, or <c>-1</c> when nothing was copied.</summary>
    [Id(6)] public long CopiedFromOffset { get; init; }

    /// <summary>The last offset copied to the target, or <c>-1</c> when nothing was copied.</summary>
    [Id(7)] public long CopiedThroughOffset { get; init; }

    /// <summary>The highest offset on the source after the move.</summary>
    [Id(8)] public long SourceHighestOffset { get; init; }

    /// <summary>The highest offset on the target after the move.</summary>
    [Id(9)] public long TargetHighestOffset { get; init; }

    /// <summary>
    /// <see langword="true"/> when the source still holds the copied entries
    /// (the move never trims the source). Reclaim explicitly with
    /// <see cref="ILatticeAdmin.ReclaimMovedWalSourceAsync"/>.
    /// </summary>
    [Id(10)] public bool SourceRetained { get; init; }

    /// <summary>The classification of the operation.</summary>
    [Id(11)] public WalMoveOutcome Outcome { get; init; }
}
