namespace Orleans.Lattice.Backup;

/// <summary>
/// Thrown by <see cref="ILatticeBackupCaptureService.CaptureSetAsync"/> when a
/// stable cross-tree-consistent fence could not be established: either in-flight
/// cross-tree atomic sagas touching the set did not drain within
/// <see cref="LatticeBackupOptions.CrossTreeFenceDrainTimeout"/>, or a cross-tree
/// saga kept registering on the set during the capture window across every one of
/// <see cref="LatticeBackupOptions.MaxCrossTreeFenceAttempts"/> attempts. The
/// capture wrote no member manifests. Retry when the set is quieter, or raise the
/// drain timeout / attempt budget.
/// </summary>
[GenerateSerializer]
public sealed class LatticeBackupCrossTreeFenceException : Exception
{
    /// <summary>Initializes a new <see cref="LatticeBackupCrossTreeFenceException"/>.</summary>
    /// <param name="message">The error message.</param>
    public LatticeBackupCrossTreeFenceException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new <see cref="LatticeBackupCrossTreeFenceException"/> with an inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public LatticeBackupCrossTreeFenceException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
