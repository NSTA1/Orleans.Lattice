using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the per-circuit transport channel and one scoped adapter per
    /// facade the Shell consumes (T1, issue #3830), and the app bridge client
    /// (B2, issue #3822) over the same circuit invoker.
    /// </summary>
    /// <remarks>
    /// The bridge client is registered directly rather than wrapped in an adapter:
    /// it already maps every <c>RpcException</c> to an <see cref="AppBridgeException"/>
    /// carrying the sanitised <see cref="AppBridgeFailure"/>, and a cancelled call to an
    /// <see cref="OperationCanceledException"/>, which is exactly what the frame broker
    /// consumes. It is registered before the adapters, so a host's own registration
    /// still wins the <c>TryAdd</c>.
    /// </remarks>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTransport(IServiceCollection services)
    {
        services.AddShellTransportClient<ILatticeAppBridge>(LatticeAppBridgeApiGrpcClient.Create);
        services.AddShellTransport();
    }
}
