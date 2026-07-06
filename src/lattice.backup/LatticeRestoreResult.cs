namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome of a successful restore: the backup and target it applied, the
/// resolved <see cref="OperationId"/>, the base-first ordered chain of backup ids
/// replayed, and how many entries were installed. For a
/// <see cref="LatticeRestoreMode.ShadowCutover"/> restore, the
/// <see cref="ShadowPhysicalTreeId"/> the alias now points at and the
/// <see cref="PreviousPhysicalTreeId"/> retained for revert are also carried.
/// </summary>
public sealed record LatticeRestoreResult
{
    /// <summary>Initializes a new <see cref="LatticeRestoreResult"/>.</summary>
    /// <param name="backupId">The backup id restored. Must not be <c>null</c> or empty.</param>
    /// <param name="targetTreeId">The tree restored into. Must not be <c>null</c> or empty.</param>
    /// <param name="mode">The restore mode applied.</param>
    /// <param name="operationId">The resolved idempotency key. Must not be <c>null</c> or empty.</param>
    /// <param name="manifestChain">
    /// The base-first ordered chain of backup ids that were replayed. Must not be
    /// <c>null</c>.
    /// </param>
    /// <param name="entriesApplied">The number of entries installed. Must not be negative.</param>
    /// <param name="shadowPhysicalTreeId">
    /// For a shadow-cutover restore, the physical tree id the alias now resolves to;
    /// <c>null</c> for an in-place restore.
    /// </param>
    /// <param name="previousPhysicalTreeId">
    /// For a shadow-cutover restore, the physical tree id the alias resolved to
    /// before the cutover (retained for revert); <c>null</c> for an in-place restore.
    /// </param>
    /// <exception cref="ArgumentException">A required string argument is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="manifestChain"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="entriesApplied"/> is negative.</exception>
    public LatticeRestoreResult(
        string backupId,
        string targetTreeId,
        LatticeRestoreMode mode,
        string operationId,
        IReadOnlyList<string> manifestChain,
        long entriesApplied,
        string? shadowPhysicalTreeId = null,
        string? previousPhysicalTreeId = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(targetTreeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(manifestChain);
        ArgumentOutOfRangeException.ThrowIfNegative(entriesApplied);

        BackupId = backupId;
        TargetTreeId = targetTreeId;
        Mode = mode;
        OperationId = operationId;
        ManifestChain = manifestChain;
        EntriesApplied = entriesApplied;
        ShadowPhysicalTreeId = shadowPhysicalTreeId;
        PreviousPhysicalTreeId = previousPhysicalTreeId;
    }

    /// <summary>The backup id restored.</summary>
    public string BackupId { get; init; }

    /// <summary>The tree restored into.</summary>
    public string TargetTreeId { get; init; }

    /// <summary>The restore mode applied.</summary>
    public LatticeRestoreMode Mode { get; init; }

    /// <summary>The resolved idempotency key.</summary>
    public string OperationId { get; init; }

    /// <summary>The base-first ordered chain of backup ids that were replayed.</summary>
    public IReadOnlyList<string> ManifestChain { get; init; }

    /// <summary>The number of entries installed.</summary>
    public long EntriesApplied { get; init; }

    /// <summary>The physical tree id the alias now resolves to (shadow-cutover only).</summary>
    public string? ShadowPhysicalTreeId { get; init; }

    /// <summary>The physical tree id retained for revert (shadow-cutover only).</summary>
    public string? PreviousPhysicalTreeId { get; init; }
}
