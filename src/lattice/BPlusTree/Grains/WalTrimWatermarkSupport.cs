using Microsoft.Extensions.DependencyInjection;
using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Whether a WAL reader may trust the trim watermark (issue #4621): every silo in
/// the current cluster manifest hosts <see cref="IWalTrimWatermarkSupportGrain"/>,
/// so every silo that can run a WAL garbage-collection pass trims through a
/// provider that persists the watermark first. Until then a reader treats every
/// jump in offsets as a trim, which can over-trigger recovery during a rolling
/// upgrade but never skips a trim silently.
/// </summary>
internal static class WalTrimWatermarkSupport
{
    /// <summary>
    /// <see langword="true"/> when every silo in the current cluster manifest hosts
    /// <see cref="IWalTrimWatermarkSupportGrain"/>. A host without the Orleans
    /// runtime services (a bare unit-test activation) has no other silo and
    /// answers <see langword="true"/>.
    /// </summary>
    public static bool AllSilosMaintain(IServiceProvider? services)
    {
        var manifests = services?.GetService<IClusterManifestProvider>();
        var interfaces = services?.GetService<GrainInterfaceTypeResolver>();
        if (manifests is null || interfaces is null)
        {
            return true;
        }

        var marker = interfaces.GetGrainInterfaceType(typeof(IWalTrimWatermarkSupportGrain));
        var silos = manifests.Current.Silos;
        if (silos.Count == 0)
        {
            return false;
        }

        foreach (var silo in silos.Values)
        {
            if (!silo.Interfaces.ContainsKey(marker))
            {
                return false;
            }
        }

        return true;
    }
}
