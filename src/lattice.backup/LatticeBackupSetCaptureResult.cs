namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome of a successful backup-set capture: the self-describing
/// <see cref="SetManifest"/> tying the members together, and the per-tree
/// <see cref="Members"/> results in scope order (each the same value a direct
/// <see cref="ILatticeBackupCaptureService.CaptureAsync"/> would return).
/// </summary>
public sealed record LatticeBackupSetCaptureResult
{
    /// <summary>Initializes a new <see cref="LatticeBackupSetCaptureResult"/>.</summary>
    /// <param name="setManifest">The set manifest tying the members together. Must not be <c>null</c>.</param>
    /// <param name="members">The per-tree member results, in scope order. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="setManifest"/> or <paramref name="members"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="members"/> is empty.</exception>
    public LatticeBackupSetCaptureResult(
        BackupSetManifest setManifest,
        IReadOnlyList<LatticeBackupCaptureResult> members)
    {
        ArgumentNullException.ThrowIfNull(setManifest);
        ArgumentNullException.ThrowIfNull(members);
        if (members.Count == 0)
        {
            throw new ArgumentException("A backup set result must have at least one member.", nameof(members));
        }

        SetManifest = setManifest;
        Members = members;
    }

    /// <summary>The set manifest tying the members together.</summary>
    public BackupSetManifest SetManifest { get; init; }

    /// <summary>The per-tree member results, in scope order.</summary>
    public IReadOnlyList<LatticeBackupCaptureResult> Members { get; init; }
}
