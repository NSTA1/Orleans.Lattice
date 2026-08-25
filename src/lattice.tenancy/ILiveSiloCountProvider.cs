namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Reports the number of currently-live silos in the cluster, so the budget
/// coordinator can divide a tenant's cluster-wide rate into per-silo shares.
/// Consulted at lease cadence only (O(silos)), never on the per-op hot path.
/// </summary>
internal interface ILiveSiloCountProvider
{
    /// <summary>
    /// Returns the count of active silos in the cluster, or <c>1</c> when the
    /// cluster view is unavailable (so a single-silo or client-hosted deployment
    /// receives the whole cluster rate rather than a zero share).
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The live silo count, at least <c>1</c>.</returns>
    ValueTask<int> GetLiveSiloCountAsync(CancellationToken cancellationToken = default);
}
