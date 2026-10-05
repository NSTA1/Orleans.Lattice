using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Converts between the facade's <see cref="LatticeRestoreResult"/> and its
/// serializable wire mirror <see cref="RestoreResponse"/>. Both ends of the
/// restore and revert-restore RPCs convert in both directions, so the
/// field-by-field copy lives here once and a field added to the result cannot
/// be carried by one direction or one end and silently dropped by another.
/// </summary>
internal static class RestoreResponseMapping
{
    /// <summary>Projects a facade restore result onto its wire mirror.</summary>
    /// <param name="result">The facade result.</param>
    /// <returns>The wire response.</returns>
    public static RestoreResponse ToRestoreResponse(LatticeRestoreResult result) =>
        new()
        {
            BackupId = result.BackupId,
            TargetTreeId = result.TargetTreeId,
            Mode = result.Mode,
            OperationId = result.OperationId,
            ManifestChain = result.ManifestChain,
            EntriesApplied = result.EntriesApplied,
            ShadowPhysicalTreeId = result.ShadowPhysicalTreeId,
            PreviousPhysicalTreeId = result.PreviousPhysicalTreeId,
        };

    /// <summary>Reconstructs the facade restore result from its wire mirror.</summary>
    /// <param name="response">The wire response.</param>
    /// <returns>The facade result.</returns>
    public static LatticeRestoreResult ToRestoreResult(RestoreResponse response) =>
        new(
            response.BackupId,
            response.TargetTreeId,
            response.Mode,
            response.OperationId,
            response.ManifestChain,
            response.EntriesApplied,
            response.ShadowPhysicalTreeId,
            response.PreviousPhysicalTreeId);
}
