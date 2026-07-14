namespace Orleans.Lattice.Api.State;

/// <summary>
/// The kind of state change carried by a <see cref="StateChangeNotification"/>.
/// A read-only projection of the core mutation kinds that a change observer
/// surfaces - single-key writes, single-key deletes, and bulk range deletes.
/// Saga-terminal and tombstone-reap WAL records are not observable changes and
/// are never surfaced.
/// </summary>
public enum StateChangeKind
{
    /// <summary>A single key was written (created or updated).</summary>
    Set = 0,

    /// <summary>A single key was deleted (tombstoned).</summary>
    Delete = 1,

    /// <summary>A half-open key range <c>[Key, EndExclusiveKey)</c> was bulk-deleted.</summary>
    DeleteRange = 2,
}
