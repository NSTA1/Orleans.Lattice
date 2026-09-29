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

    /// <summary>
    /// The number of shards the hard purge has finished: while
    /// <see cref="PurgeInProgress"/> is <see langword="true"/>, how far the walk
    /// has durably got (it never runs ahead of what a resumed purge would start
    /// from); once <see cref="PurgeComplete"/> is <see langword="true"/>, every
    /// shard it walked; otherwise 0. Read it against
    /// <see cref="PurgeShardCount"/> to follow a purge that outlasted the purge
    /// verb's call.
    /// </summary>
    [Id(7)] public int PurgedShardCount { get; init; }

    /// <summary>
    /// The number of shards the hard purge walks - one past the highest physical
    /// shard the tree has ever allocated - while it is in progress or once it has
    /// completed, or 0 when no purge has started (or a purge recorded by an
    /// earlier build did not record it).
    /// </summary>
    [Id(8)] public int PurgeShardCount { get; init; }
}
