namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// An immutable read-only snapshot of a tree's soft-deletion lifecycle state,
/// returned by <see cref="ITreeDeletionGrain.GetDeletionStatusAsync"/>. Projects
/// the persisted <see cref="State.TreeDeletionState"/> plus the derived recovery
/// deadline so a caller can report a tree's deletion status without any side
/// effect. A tree that has never been deleted reports every flag at its default
/// (<see cref="IsDeleted"/> <see langword="false"/>).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeDeletionSnapshot)]
[Immutable]
internal readonly record struct TreeDeletionSnapshot
{
    /// <summary>Whether the tree has been soft-deleted (whether or not the purge has completed).</summary>
    [Id(0)] public bool IsDeleted { get; init; }

    /// <summary>The UTC time the soft delete was initiated, or <see langword="null"/> when the tree is live.</summary>
    [Id(1)] public DateTimeOffset? DeletedAtUtc { get; init; }

    /// <summary>
    /// The UTC instant after which the soft-deleted tree is eligible for automatic
    /// purge (the delete time plus the configured soft-delete duration), or
    /// <see langword="null"/> when the tree is live. Recovery is only possible
    /// before this deadline and before an explicit purge.
    /// </summary>
    [Id(2)] public DateTimeOffset? RecoveryDeadlineUtc { get; init; }

    /// <summary>Whether a purge pass is currently in progress.</summary>
    [Id(3)] public bool PurgeInProgress { get; init; }

    /// <summary>Whether the purge has fully completed (all data irreversibly gone).</summary>
    [Id(4)] public bool PurgeComplete { get; init; }

    /// <summary>
    /// Whether the tree can still be recovered: it is soft-deleted, no purge has
    /// completed, and no purge is currently in progress.
    /// </summary>
    public bool CanRecover => IsDeleted && !PurgeComplete && !PurgeInProgress;
}
