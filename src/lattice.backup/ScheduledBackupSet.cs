namespace Orleans.Lattice.Backup;

/// <summary>
/// A backup set a schedule captured and still retains, with its exact
/// membership. The membership is recorded when the set is captured and never
/// changes, so the set stays restorable whole even after a later set has
/// captured an unchanged tree into the same content-addressed backup (which moves
/// that backup's own <see cref="BackupManifest.SetId"/> stamp to the later set).
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.ScheduledBackupSet)]
[Immutable]
public sealed record ScheduledBackupSet
{
    /// <summary>Initializes a new <see cref="ScheduledBackupSet"/>.</summary>
    /// <param name="setId">
    /// The set's id: the content address of its ordered member backup ids
    /// (<see cref="BackupSetManifest.SetId"/>, or for a single-member set the id
    /// that member would have been given). Pass it to <c>RestoreTreeSetAsync</c>
    /// to restore the set. Must not be <c>null</c> or empty.
    /// </param>
    /// <param name="name">The set's name. Must not be <c>null</c> or empty.</param>
    /// <param name="createdAtUtc">When the set was first captured.</param>
    /// <param name="lastCapturedAtUtc">When the set was last captured: a later run that captured identical content re-captures the same set.</param>
    /// <param name="crossTreeConsistent">Whether the set was captured at a single cross-tree fence.</param>
    /// <param name="members">The set's members, one per tree, in tree-id order. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException">An argument is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="members"/> is <c>null</c>.</exception>
    public ScheduledBackupSet(
        string setId,
        string name,
        DateTimeOffset createdAtUtc,
        DateTimeOffset lastCapturedAtUtc,
        bool crossTreeConsistent,
        IReadOnlyList<ScheduledBackupSetMember> members)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(members);
        if (members.Count == 0)
        {
            throw new ArgumentException("A scheduled backup set has at least one member.", nameof(members));
        }

        SetId = setId;
        Name = name;
        CreatedAtUtc = createdAtUtc;
        LastCapturedAtUtc = lastCapturedAtUtc;
        CrossTreeConsistent = crossTreeConsistent;
        Members = members;
    }

    /// <summary>The set's id; pass it to <c>RestoreTreeSetAsync</c>.</summary>
    [Id(0)]
    public string SetId { get; init; }

    /// <summary>The set's name.</summary>
    [Id(1)]
    public string Name { get; init; }

    /// <summary>When the set was first captured.</summary>
    [Id(2)]
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>When the set was last captured; retention ranks a set by this time.</summary>
    [Id(3)]
    public DateTimeOffset LastCapturedAtUtc { get; init; }

    /// <summary>Whether the set was captured at a single cross-tree fence.</summary>
    [Id(4)]
    public bool CrossTreeConsistent { get; init; }

    /// <summary>The set's members, one per tree, in tree-id order.</summary>
    [Id(5)]
    public IReadOnlyList<ScheduledBackupSetMember> Members { get; init; }
}
