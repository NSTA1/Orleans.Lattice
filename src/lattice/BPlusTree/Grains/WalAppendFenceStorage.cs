using Microsoft.Extensions.DependencyInjection;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Resolves the tree's grain storage provider for the WAL append fence's rows
/// (epic #5605) without throwing when an activation's service provider does not
/// support keyed services, as a hand-built test harness's may not; the fence then
/// stays off for that activation, which appends unstamped.
/// </summary>
internal static class WalAppendFenceStorage
{
    /// <summary>
    /// The grain storage registered under <see cref="LatticeOptions.StorageProviderName"/>,
    /// or <see langword="null"/> when there is none or <paramref name="services"/>
    /// cannot resolve keyed services.
    /// </summary>
    internal static IGrainStorage? Resolve(IServiceProvider? services) =>
        services is IKeyedServiceProvider keyed
            ? keyed.GetKeyedService(typeof(IGrainStorage), LatticeOptions.StorageProviderName) as IGrainStorage
            : null;
}
