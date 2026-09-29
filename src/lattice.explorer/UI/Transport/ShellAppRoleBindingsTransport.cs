using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeAppRoleBindings"/> over gRPC. The
/// <see cref="LatticeAppsApiGrpcClient"/> already implements the facade; this
/// per-circuit adapter delegates to it only so its transport faults map through
/// <see cref="ShellTransportFaults"/> like every other facade the Shell consumes. A
/// cluster that does not serve role re-binding answers Unimplemented, which surfaces
/// as <see cref="NotSupportedException"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellAppRoleBindingsTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeAppsApiGrpcClient>(channel, LatticeAppsApiGrpcClient.Create), ILatticeAppRoleBindings
{
    /// <inheritdoc />
    public Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.UpdateRoleBindingsAsync(state, ct), null, cancellationToken);
    }
}
