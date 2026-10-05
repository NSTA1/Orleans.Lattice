using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Whether every active silo honours a saga decision-purge hold (issue
/// #4533): it hosts <see cref="IWalPurgeHoldGrain"/>, which shipped with the
/// transaction registry that reads the holds, after the decision-purge guard
/// of #4508. A silo that predates it purges decisions on retention alone.
/// </summary>
internal static class PurgeHoldSupport
{
    /// <summary>
    /// <see langword="true"/> when every silo in the current cluster manifest
    /// hosts <see cref="IWalPurgeHoldGrain"/>. A host without the Orleans
    /// runtime services (a bare unit-test activation) has no other silo and
    /// answers <see langword="true"/>.
    /// </summary>
    public static bool AllSilosHonour(IServiceProvider? services)
    {
        var manifests = services?.GetService<IClusterManifestProvider>();
        var interfaces = services?.GetService<GrainInterfaceTypeResolver>();
        if (manifests is null || interfaces is null)
        {
            return true;
        }

        var holdInterface = interfaces.GetGrainInterfaceType(typeof(IWalPurgeHoldGrain));
        var silos = manifests.Current.Silos;
        if (silos.Count == 0)
        {
            return false;
        }

        foreach (var silo in silos.Values)
        {
            if (!silo.Interfaces.ContainsKey(holdInterface))
            {
                return false;
            }
        }

        return true;
    }
}
