namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to capture one full backup: the human-readable
/// <see cref="Name"/> recorded on the manifest, the <see cref="Scope"/> that
/// selects the region of the tree to capture (whole-tree, per-prefix, or
/// per-key), and an optional <see cref="PageSize"/> that bounds how many raw
/// entries the capture drains from the snapshot cursor per round-trip. A larger
/// page trades memory for fewer round-trips; the values still stream to the sink
/// one page at a time so the whole scope is never buffered.
/// </summary>
public sealed record LatticeBackupCaptureRequest
{
    /// <summary>The default raw-entry drain page size (<c>1024</c>).</summary>
    public const int DefaultPageSize = 1024;

    /// <summary>Initializes a new <see cref="LatticeBackupCaptureRequest"/>.</summary>
    /// <param name="name">The human-readable backup name recorded on the manifest. Must not be <c>null</c> or empty.</param>
    /// <param name="scope">The region of the tree to capture. Must not be <c>null</c>.</param>
    /// <param name="pageSize">
    /// The number of raw entries to drain from the snapshot cursor per round-trip.
    /// Must be positive. Defaults to <see cref="DefaultPageSize"/>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    public LatticeBackupCaptureRequest(string name, BackupScopeSelector scope, int pageSize = DefaultPageSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        Name = name;
        Scope = scope;
        PageSize = pageSize;
    }

    /// <summary>The human-readable backup name recorded on the manifest.</summary>
    public string Name { get; init; }

    /// <summary>The region of the tree the backup captures.</summary>
    public BackupScopeSelector Scope { get; init; }

    /// <summary>The raw-entry drain page size.</summary>
    public int PageSize { get; init; }
}
