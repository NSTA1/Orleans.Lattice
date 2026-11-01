namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to restore a backup into a target tree. The backup identified by
/// <see cref="BackupId"/> selects the point in the chain to restore to: a full
/// backup, or the tip of a base-plus-increments chain that is walked back to its
/// base and replayed base-first. An optional <see cref="TargetTreeId"/> redirects
/// the restore to a different tree than the one captured; an optional
/// <see cref="Scope"/> narrows the restore to a sub-region (prefix or single key)
/// of the captured backup. <see cref="Mode"/> selects in-place replay or an atomic
/// shadow-cutover. <see cref="OperationId"/> makes a retried restore idempotent.
/// </summary>
public sealed record LatticeRestoreRequest
{
    /// <summary>The default apply batch size (<c>1024</c>).</summary>
    public const int DefaultApplyBatchSize = 1024;

    /// <summary>Initializes a new <see cref="LatticeRestoreRequest"/>.</summary>
    /// <param name="backupId">
    /// The content-addressed id of the backup to restore to. Must not be <c>null</c>
    /// or empty.
    /// </param>
    /// <param name="targetTreeId">
    /// The tree to restore into, or <c>null</c> to restore into the tree the backup
    /// was captured from. Must not be empty when supplied.
    /// </param>
    /// <param name="scope">
    /// The sub-region of the backup to restore, or <c>null</c> to restore the whole
    /// captured scope. When supplied it must fall within the captured scope; its
    /// tree id is ignored (the restore targets <paramref name="targetTreeId"/>).
    /// </param>
    /// <param name="mode">The restore mode. Defaults to <see cref="LatticeRestoreMode.InPlace"/>.</param>
    /// <param name="operationId">
    /// An idempotency key that makes a retried restore a no-op, or <c>null</c> to
    /// derive a deterministic id from the request. Must not be empty when supplied.
    /// </param>
    /// <param name="applyBatchSize">
    /// The maximum number of entries applied to a single shard per round-trip. Must
    /// be positive. Defaults to <see cref="DefaultApplyBatchSize"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="backupId"/> is <c>null</c> or empty, or
    /// <paramref name="targetTreeId"/> / <paramref name="operationId"/> is empty.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="applyBatchSize"/> is not positive.</exception>
    public LatticeRestoreRequest(
        string backupId,
        string? targetTreeId = null,
        BackupScopeSelector? scope = null,
        LatticeRestoreMode mode = LatticeRestoreMode.InPlace,
        string? operationId = null,
        int applyBatchSize = DefaultApplyBatchSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (targetTreeId is not null)
        {
            ArgumentException.ThrowIfNullOrEmpty(targetTreeId);
        }

        if (operationId is not null)
        {
            ArgumentException.ThrowIfNullOrEmpty(operationId);
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applyBatchSize);

        BackupId = backupId;
        TargetTreeId = targetTreeId;
        Scope = scope;
        Mode = mode;
        OperationId = operationId;
        ApplyBatchSize = applyBatchSize;
    }

    /// <summary>The content-addressed id of the backup to restore to.</summary>
    public string BackupId { get; init; }

    /// <summary>The tree to restore into, or <c>null</c> to restore into the captured tree.</summary>
    public string? TargetTreeId { get; init; }

    /// <summary>The sub-region of the backup to restore, or <c>null</c> for the whole captured scope.</summary>
    public BackupScopeSelector? Scope { get; init; }

    /// <summary>The restore mode.</summary>
    public LatticeRestoreMode Mode { get; init; }

    /// <summary>The idempotency key, or <c>null</c> to derive one from the request.</summary>
    public string? OperationId { get; init; }

    /// <summary>The maximum number of entries applied to a single shard per round-trip.</summary>
    public int ApplyBatchSize { get; init; }
}
