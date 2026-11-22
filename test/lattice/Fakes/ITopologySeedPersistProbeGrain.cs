namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Drives <c>TopologySeedPersist.WriteRecoveringLandedConflictAsync</c> through a
/// real Orleans persistent-state bridge, so the landed-conflict recovery of issue
/// #5095 can be exercised without a B+ tree workload around it.
/// </summary>
internal interface ITopologySeedPersistProbeGrain : IGrainWithStringKey
{
    /// <summary>Sets the probe value and persists it through the shared write seam.</summary>
    /// <param name="value">The value to persist.</param>
    /// <returns>The state's etag after the write.</returns>
    Task<string?> WriteAsync(long value);

    /// <summary>Re-reads the stored row and returns its probe value.</summary>
    /// <returns>The stored value.</returns>
    Task<long> ReadStoredAsync();
}
