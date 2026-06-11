namespace Orleans.Lattice;

/// <summary>
/// Terminal result of a cross-tree atomic write
/// (<see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>).
/// Reported without an exception so callers can branch on a precondition miss
/// as ordinary control flow rather than catching one, mirroring the single-tree
/// <see cref="AtomicWriteOutcome"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeAtomicWriteOutcome)]
public enum CrossTreeAtomicWriteOutcome
{
    /// <summary>
    /// Every participating tree's batch satisfied its guard predicate (if any)
    /// and every prepare phase succeeded, so the whole cross-tree batch was
    /// committed atomically - readers on every participating tree flip from the
    /// pre-saga view to the post-saga view at the coordinator's single decision
    /// moment, never observing a partial cross-tree commit.
    /// </summary>
    Committed = 0,

    /// <summary>
    /// At least one participating tree's guard predicate failed (or a targeted
    /// key had no live pre-saga value), so the coordinator aborted the whole
    /// cross-tree batch before making any write visible and committed nothing.
    /// A precondition miss is reported as a value, not an exception; genuine
    /// write failures still throw and compensate.
    /// </summary>
    PreconditionFailed = 1,
}
