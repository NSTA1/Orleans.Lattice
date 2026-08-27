namespace Orleans.Lattice.Backup;

/// <summary>
/// A self-describing manifest tying together the per-tree backups captured as one
/// set. When <see cref="CrossTreeConsistent"/> is <c>true</c> the member backups
/// were all captured as of a single <see cref="BackupSetFence"/>, so a cross-tree
/// atomic write is never torn across the set boundary. When <c>false</c> the set
/// is a convenience grouping and each member carries its own cheap per-tree cut
/// with no cross-tree coordination. A set of a single scope carries a <c>null</c>
/// <see cref="SetId"/>: it is captured as, and lists as, a plain backup.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetManifest)]
[Immutable]
public sealed record BackupSetManifest
{
    /// <summary>Initializes a new <see cref="BackupSetManifest"/>.</summary>
    /// <param name="setId">
    /// The content-addressed id of the set (derived from the ordered member backup
    /// ids), or <c>null</c> for a single-member set, whose membership is
    /// deliberately never stamped onto its only member manifest and which
    /// therefore has no id that resolves. Must not be empty when non-<c>null</c>.
    /// </param>
    /// <param name="name">The human-readable set name. Must not be <c>null</c> or empty.</param>
    /// <param name="createdAtUtc">The wall-clock time the set capture completed.</param>
    /// <param name="crossTreeConsistent">Whether the members were captured at a single cross-tree causal fence.</param>
    /// <param name="fence">The causal fence the set was captured at, or <c>null</c> when <paramref name="crossTreeConsistent"/> is <c>false</c>.</param>
    /// <param name="memberBackupIds">The ordered content-addressed ids of the member backups (one per tree). Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="setId"/> is empty, <paramref name="name"/> is <c>null</c> or empty, or <paramref name="memberBackupIds"/> is empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="memberBackupIds"/> is <c>null</c>.</exception>
    public BackupSetManifest(
        string? setId,
        string name,
        DateTimeOffset createdAtUtc,
        bool crossTreeConsistent,
        BackupSetFence? fence,
        IReadOnlyList<string> memberBackupIds)
    {
        if (setId is { Length: 0 })
        {
            throw new ArgumentException("A backup set id must not be empty when present.", nameof(setId));
        }

        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(memberBackupIds);
        if (memberBackupIds.Count == 0)
        {
            throw new ArgumentException("A backup set must have at least one member.", nameof(memberBackupIds));
        }

        SetId = setId;
        Name = name;
        CreatedAtUtc = createdAtUtc;
        CrossTreeConsistent = crossTreeConsistent;
        Fence = fence;
        MemberBackupIds = memberBackupIds;
    }

    /// <summary>
    /// The content-addressed id of the set, or <c>null</c> for a single-member
    /// set. Membership is durable only as the <see cref="BackupManifest.SetId"/>
    /// stamp on each member's own manifest, and a one-member set is deliberately
    /// left unstamped, so it is given no id rather than one that resolves to
    /// nothing: a non-<c>null</c> id here always matches the
    /// <see cref="BackupManifest.SetId"/> of every catalogued member.
    /// </summary>
    [Id(0)]
    public string? SetId { get; init; }

    /// <summary>The human-readable set name.</summary>
    [Id(1)]
    public string Name { get; init; }

    /// <summary>The wall-clock time the set capture completed.</summary>
    [Id(2)]
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Whether the members were captured at a single cross-tree causal fence.</summary>
    [Id(3)]
    public bool CrossTreeConsistent { get; init; }

    /// <summary>The causal fence the set was captured at; <c>null</c> when not cross-tree-consistent.</summary>
    [Id(4)]
    public BackupSetFence? Fence { get; init; }

    /// <summary>The ordered content-addressed ids of the member backups (one per tree).</summary>
    [Id(5)]
    public IReadOnlyList<string> MemberBackupIds { get; init; }
}
