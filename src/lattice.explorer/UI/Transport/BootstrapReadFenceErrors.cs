using Grpc.Core;
using Orleans.Lattice;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>Recognizes the fixed state-API fault used for reads refused during a legacy in-place bootstrap.</summary>
internal static class BootstrapReadFenceErrors
{
    private const string BootstrapStatusDetail = "being bootstrapped from a snapshot";

    public static bool IsBootstrapReadFence(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is LatticeTreeBootstrappingException
                || current is RpcException rpc
                && rpc.StatusCode == StatusCode.Unavailable
                && rpc.Status.Detail.Contains(BootstrapStatusDetail, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
