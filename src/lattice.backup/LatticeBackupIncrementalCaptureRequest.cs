namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to capture one incremental backup: the same shape as
/// <see cref="LatticeBackupCaptureRequest"/> plus the
/// <see cref="BaseBackupId"/> of the backup this increment is layered on. The
/// captured manifest records <see cref="BaseBackupId"/> as its
/// <see cref="BackupManifest.BaseBackupId"/> so a restore can walk the chain back
/// to a full base.
/// </summary>
public sealed record LatticeBackupIncrementalCaptureRequest
{
    /// <summary>Initializes a new <see cref="LatticeBackupIncrementalCaptureRequest"/>.</summary>
    /// <param name="name">The human-readable backup name recorded on the manifest. Must not be <c>null</c> or empty.</param>
    /// <param name="scope">The region of the tree to capture. Must not be <c>null</c>.</param>
    /// <param name="baseBackupId">The id of the base backup this increment is layered on. Must not be <c>null</c> or empty.</param>
    /// <param name="pageSize">
    /// The number of raw entries to drain from the snapshot cursor per round-trip.
    /// Must be positive. Defaults to <see cref="LatticeBackupCaptureRequest.DefaultPageSize"/>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="name"/> or <paramref name="baseBackupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    public LatticeBackupIncrementalCaptureRequest(
        string name,
        BackupScopeSelector scope,
        string baseBackupId,
        int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentException.ThrowIfNullOrEmpty(baseBackupId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        Name = name;
        Scope = scope;
        BaseBackupId = baseBackupId;
        PageSize = pageSize;
    }

    /// <summary>The human-readable backup name recorded on the manifest.</summary>
    public string Name { get; init; }

    /// <summary>The region of the tree the backup captures.</summary>
    public BackupScopeSelector Scope { get; init; }

    /// <summary>The id of the base backup this increment is layered on.</summary>
    public string BaseBackupId { get; init; }

    /// <summary>The raw-entry drain page size.</summary>
    public int PageSize { get; init; }
}
