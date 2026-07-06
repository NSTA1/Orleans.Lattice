namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The advisory set of backup operations the connected user may perform over a
/// single scope, as reported by the backend capability probe. Every flag is
/// default-deny: a value of <see langword="false"/> means "not known to be
/// permitted", and the flags are a UX affordance only - the server remains the
/// fail-closed enforcement point, so a real action must still handle a denial
/// even when the matching flag was <see langword="true"/>.
/// </summary>
/// <remarks>
/// This is the explorer-owned mirror of the backup control-API capability
/// result. It is intentionally free of any backup-API dependency so the pure
/// navigation layer can compute area availability from it without dragging the
/// cluster libraries into the explorer core.
/// </remarks>
public sealed record BackupScopeCapabilitySnapshot
{
    /// <summary>A snapshot with every capability denied. The safe default.</summary>
    public static BackupScopeCapabilitySnapshot None { get; } = new();

    /// <summary>Whether the caller may list / read backups in the scope.</summary>
    public bool CanList { get; init; }

    /// <summary>Whether the caller may capture a full backup of the scope.</summary>
    public bool CanCapture { get; init; }

    /// <summary>Whether the caller may capture an incremental backup of the scope.</summary>
    public bool CanCaptureIncremental { get; init; }

    /// <summary>Whether the caller may restore a backup into the scope.</summary>
    public bool CanRestore { get; init; }

    /// <summary>Whether the caller may delete a backup in the scope.</summary>
    public bool CanDelete { get; init; }
}
