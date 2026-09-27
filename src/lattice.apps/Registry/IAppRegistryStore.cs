using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The storage seam the app registry reads and writes records through. The production
/// implementation is <see cref="LatticeAppRegistryStore"/> over the reserved
/// <c>sys-app-registry</c> tree; the seam exists so the registry's lifecycle and
/// optimistic-concurrency logic can be driven deterministically in unit tests.
/// </summary>
internal interface IAppRegistryStore
{
    /// <summary>Reads a record and the version to write it back conditionally on.</summary>
    /// <param name="key">The registry key.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The record (or <c>null</c>) and its version (<see cref="HybridLogicalClock.Zero"/> when absent).</returns>
    Task<AppRegistryStoreRead> GetAsync(string key, CancellationToken cancellationToken);

    /// <summary>
    /// Writes <paramref name="record"/> only if the stored version still equals
    /// <paramref name="expectedVersion"/> (<see cref="HybridLogicalClock.Zero"/> meaning
    /// "create only if still absent").
    /// </summary>
    /// <param name="key">The registry key.</param>
    /// <param name="record">The record to write.</param>
    /// <param name="expectedVersion">The version read before deciding the write.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><c>true</c> when the write applied; <c>false</c> when a competing writer won.</returns>
    Task<bool> TrySetAsync(string key, AppRegistryRecord record, HybridLogicalClock expectedVersion, CancellationToken cancellationToken);

    /// <summary>Enumerates records in ascending key order within an optional key range.</summary>
    /// <param name="startInclusive">The inclusive lower bound, or <c>null</c>.</param>
    /// <param name="endExclusive">The exclusive upper bound, or <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The records in range.</returns>
    IAsyncEnumerable<AppRegistryRecord> ScanAsync(string? startInclusive, string? endExclusive, CancellationToken cancellationToken);
}
