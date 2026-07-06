namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The outcome classification of a backup management action (list, describe,
/// capture, incremental, restore, delete). Lets the UI render a clean affordance
/// for a permission denial or a transport failure instead of surfacing an
/// unhandled exception.
/// </summary>
public enum BackupOperationStatus
{
    /// <summary>The action completed successfully.</summary>
    Succeeded,

    /// <summary>The server denied the caller. The advisory capability map was over-optimistic; the server is the enforcement point.</summary>
    Denied,

    /// <summary>The action failed for a transport or server reason other than a permission denial.</summary>
    Failed,
}
