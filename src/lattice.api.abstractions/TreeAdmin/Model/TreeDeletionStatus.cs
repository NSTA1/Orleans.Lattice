namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a tree's soft-deletion lifecycle, returned by the
/// tree delete / recover / purge verbs and the standalone status read. Reports
/// whether the tree is soft-deleted, when, the recovery deadline, and whether a
/// hard purge is in progress or has completed. A tree that has never been deleted
/// reports <see cref="IsDeleted"/> <see langword="false"/> with every other flag
/// at its default. A pure projection with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeDeletionStatus)]
[Immutable]
public sealed record TreeDeletionStatus
{
    /// <summary>The tree id whose deletion status this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree has been soft-deleted (whether or not
    /// the purge has completed); otherwise <see langword="false"/>.
    /// </summary>
    [Id(1)] public bool IsDeleted { get; init; }

    /// <summary>
    /// The UTC time the soft delete was initiated, or <see langword="null"/> when
    /// the tree is live.
    /// </summary>
    [Id(2)] public DateTimeOffset? DeletedAtUtc { get; init; }

    /// <summary>
    /// The UTC instant after which the soft-deleted tree becomes eligible for the
    /// automatic deferred purge (the delete time plus the configured soft-delete
    /// duration), or <see langword="null"/> when the tree is live. Recovery is
    /// only possible before this deadline and before an explicit purge.
    /// </summary>
    [Id(3)] public DateTimeOffset? RecoveryDeadlineUtc { get; init; }

    /// <summary>
    /// <see langword="true"/> when a hard purge pass is currently in progress.
    /// </summary>
    [Id(4)] public bool PurgeInProgress { get; init; }

    /// <summary>
    /// <see langword="true"/> when the hard purge has fully completed and the
    /// tree's data is irreversibly gone.
    /// </summary>
    [Id(5)] public bool PurgeComplete { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree can still be recovered: it is
    /// soft-deleted, no purge has completed, and no purge is in progress.
    /// </summary>
    [Id(6)] public bool CanRecover { get; init; }
}
