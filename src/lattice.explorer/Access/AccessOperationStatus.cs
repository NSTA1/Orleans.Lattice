namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The outcome category of an Access-area operation. Mirrors the Backups area's
/// operation-status vocabulary: a clean, three-way split so the UI can style a
/// success, a permission denial (the fail-closed server verdict), and a
/// transport / server failure distinctly, and never surface an unhandled error.
/// </summary>
public enum AccessOperationStatus
{
    /// <summary>The operation completed successfully.</summary>
    Succeeded,

    /// <summary>The server denied the operation (the caller is not an authorized administrator).</summary>
    Denied,

    /// <summary>The operation failed for a transport or server-side reason other than a denial.</summary>
    Failed,
}
