namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome of a successful capture: the content-addressed
/// <see cref="BackupId"/> (also the manifest id and the sole catalog key) and
/// the self-describing <see cref="Manifest"/> that was written to the sink and
/// registered in the hidden catalog.
/// </summary>
public sealed record LatticeBackupCaptureResult
{
    /// <summary>Initializes a new <see cref="LatticeBackupCaptureResult"/>.</summary>
    /// <param name="backupId">The content-addressed backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="manifest">The manifest written for the backup. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is <c>null</c>.</exception>
    public LatticeBackupCaptureResult(string backupId, BackupManifest manifest)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(manifest);
        BackupId = backupId;
        Manifest = manifest;
    }

    /// <summary>The content-addressed backup id.</summary>
    public string BackupId { get; init; }

    /// <summary>The manifest written for the backup.</summary>
    public BackupManifest Manifest { get; init; }
}
