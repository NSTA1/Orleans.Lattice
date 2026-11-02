using Orleans.Concurrency;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The cluster-wide record of how far the reserved <c>sys-app-registry</c> tree has moved (#5595). A
/// single activation, addressed by <see cref="GrainKey"/>: the silo that commits a registry write
/// advances it, and every silo reads it before trusting its compiled snapshot. The core change feed
/// fires only on the silo hosting the leaf that committed a write, so without it every other silo
/// would keep answering from the registry revision it first warmed on.
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

    /// <summary>Advances the epoch after a committed registry write.</summary>
    /// <returns>The advanced epoch.</returns>
    Task<AppRegistryEpoch> AdvanceAsync();
}
