namespace Orleans.Lattice.Replication;

/// <summary>
/// Inspection and replay seam for the per-tree dead-letter queue.
/// Resolved from DI as a singleton; routes calls to the underlying
/// per-tree <c>IReplicationDeadLetterGrain</c> activation and (on
/// replay) into the canonical <see cref="IReplicationApplier"/>.
/// </summary>
public interface ILatticeReplicationDeadLetters
{
    /// <summary>
    /// Returns every parked entry for <paramref name="treeId"/> in
    /// ascending <see cref="DeadLetterEntry.EntryId"/> order. Empty list
    /// when nothing is parked. Pure read — no state changes.
    /// </summary>
    Task<IReadOnlyList<DeadLetterEntry>> ListAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of entries currently parked for
    /// <paramref name="treeId"/>.
    /// </summary>
    Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the parked entry with id <paramref name="entryId"/> from
    /// <paramref name="treeId"/>'s queue without attempting to apply
    /// it. Returns <c>true</c> when an entry was removed; <c>false</c>
    /// when no entry with that id existed.
    /// </summary>
    Task<bool> DiscardAsync(string treeId, long entryId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replays the parked entry with id <paramref name="entryId"/>
    /// through the canonical applier, bypassing the failure-tracking
    /// decorator so the replay is not itself counted toward
    /// <see cref="LatticeReplicationOptions.MaxApplyRetries"/>. The
    /// entry is removed from the queue when the replay returns
    /// successfully (regardless of the resulting
    /// <see cref="ApplyResult.Applied"/> flag — a re-delivery filter is
    /// considered terminal for inspection purposes); replay failures
    /// leave the entry parked so the operator can decide whether to
    /// retry or discard. Returns <c>null</c> when no entry with that id
    /// exists.
    /// </summary>
    Task<ApplyResult?> ReplayAsync(string treeId, long entryId, CancellationToken cancellationToken = default);
}
