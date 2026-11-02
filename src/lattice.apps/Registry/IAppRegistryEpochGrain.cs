using Orleans.Concurrency;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The cluster-wide record of how far the reserved <c>sys-app-registry</c> tree has moved (#5595). A
/// single activation, addressed by <see cref="GrainKey"/>: the silo that commits a registry write
/// advances it, and every silo is leased by it and pushed every advance (#5713), reading it before
/// trusting its compiled snapshot only while it holds no live lease. The core change feed fires only on
/// the silo hosting the leaf that committed a write, so without it every other silo would keep
/// answering from the registry revision it first warmed on.
/// </summary>
[Alias(AppRegistryTypeAliases.IAppRegistryEpochGrain)]
internal interface IAppRegistryEpochGrain : IGrainWithStringKey
{
    /// <summary>The fixed key of the single cluster-wide activation.</summary>
    const string GrainKey = "sys-app-registry-epoch";

    /// <summary>Returns the current epoch.</summary>
    /// <returns>The current epoch.</returns>
    [ReadOnly]
    Task<AppRegistryEpoch> GetAsync();

    /// <summary>
    /// Grants or renews <paramref name="observer"/>'s lease and returns the current epoch with the granted
    /// duration (#5713). The observer is pushed every advance for as long as its lease is live, so the silo
    /// can trust its snapshot without reading the epoch on every evaluation.
    /// </summary>
    /// <param name="observer">The requesting silo's epoch observer.</param>
    /// <param name="silo">The requesting silo's address, used to tell when every live silo has leased from this activation.</param>
    /// <returns>The current epoch and the granted lease duration.</returns>
    Task<AppRegistryEpochLease> LeaseAsync(IAppRegistryEpochObserver observer, SiloAddress silo);

    /// <summary>
    /// Advances the epoch after a committed registry write, returning only once every silo holding a live
    /// lease has acknowledged the new epoch or had its lease expire, so no silo can go on trusting a
    /// snapshot that predates the write.
    /// </summary>
    /// <returns>The advanced epoch.</returns>
    Task<AppRegistryEpoch> AdvanceAsync();
}
