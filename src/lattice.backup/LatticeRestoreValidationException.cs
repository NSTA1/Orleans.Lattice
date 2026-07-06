namespace Orleans.Lattice.Backup;

/// <summary>
/// Thrown by <see cref="ILatticeBackupRestoreService.RestoreAsync"/> when a
/// backup fails the pre-apply trust-boundary validation: the manifest (or a
/// manifest in its base chain) is missing, an artifact it references is absent
/// from the sink, a streamed artifact's recomputed content digest does not match
/// the digest recorded on the manifest, or the requested sub-scope falls outside
/// the captured scope. The restore aborts before installing anything, so the
/// target tree is left untouched.
/// </summary>
public sealed class LatticeRestoreValidationException : InvalidOperationException
{
    /// <summary>Initializes a new instance with the specified message.</summary>
    /// <param name="message">The validation failure description.</param>
    public LatticeRestoreValidationException(string message) : base(message)
    {
    }

    /// <summary>Initializes a new instance with the specified message and inner exception.</summary>
    /// <param name="message">The validation failure description.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeRestoreValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
