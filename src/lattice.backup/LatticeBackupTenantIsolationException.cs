namespace Orleans.Lattice.Backup;

/// <summary>
/// Thrown when a capture or restore would cross the active tenant's isolation
/// boundary: capturing or restoring into a tree owned by a different tenant (or a
/// platform tree the active tenant does not own). The operation is refused before
/// any data is read or written, so a tenant-scoped backup can never span another
/// tenant's namespace and a restore can never author into one.
/// </summary>
/// <remarks>
/// Mirrors <see cref="LatticeRestoreValidationException"/>: it derives directly
/// from <see cref="InvalidOperationException"/> and is not
/// <c>[GenerateSerializer]</c>, because it is thrown and handled within the silo
/// that runs the backup engine rather than being returned across a grain
/// boundary.
/// </remarks>
public sealed class LatticeBackupTenantIsolationException : InvalidOperationException
{
    /// <summary>Initializes a new instance with the specified message.</summary>
    /// <param name="message">The isolation-violation description.</param>
    public LatticeBackupTenantIsolationException(string message) : base(message)
    {
    }

    /// <summary>Initializes a new instance with the specified message and inner exception.</summary>
    /// <param name="message">The isolation-violation description.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeBackupTenantIsolationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
