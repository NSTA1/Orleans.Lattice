namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The outcome of a tree-administration restore
/// (<see cref="ILatticeTreeAdmin.RestoreTreeAsync"/> or a single member of
/// <see cref="ILatticeTreeAdmin.RestoreTreeSetAsync"/>): the backup and target it
/// applied, the resolved idempotency key, the base-first ordered chain of backup
/// ids replayed, and how many entries were installed. Because a
/// tree-administration restore always runs as a
/// <see cref="TreeRestoreMode.ShadowCutover"/>, it also carries the shadow
/// physical tree the alias now points at and the previous physical tree retained
/// for revert, so the whole result can be handed back verbatim to
/// <see cref="ILatticeTreeAdmin.RevertTreeRestoreAsync"/> to undo the cutover.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeRestoreResult)]
[Immutable]
public sealed record TreeRestoreResult
{
    /// <summary>The backup id that was restored.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>The tree the backup was restored into.</summary>
    [Id(1)] public required string TargetTreeId { get; init; }

    /// <summary>The mode the restore applied (always <see cref="TreeRestoreMode.ShadowCutover"/> for a tree-administration restore).</summary>
    [Id(2)] public required TreeRestoreMode Mode { get; init; }

    /// <summary>The resolved idempotency key for the restore.</summary>
    [Id(3)] public required string OperationId { get; init; }

    /// <summary>The base-first ordered chain of backup ids that were replayed. Never <see langword="null"/>.</summary>
    [Id(4)] public required IReadOnlyList<string> ManifestChain { get; init; }

    /// <summary>The number of entries installed by the restore.</summary>
    [Id(5)] public required long EntriesApplied { get; init; }

    /// <summary>The physical tree id the target's alias now resolves to after the shadow cutover, or <see langword="null"/> for an in-place restore.</summary>
    [Id(6)] public string? ShadowPhysicalTreeId { get; init; }

    /// <summary>The physical tree id the target's alias resolved to before the cutover, retained so the restore can be reverted, or <see langword="null"/> for an in-place restore.</summary>
    [Id(7)] public string? PreviousPhysicalTreeId { get; init; }
}
