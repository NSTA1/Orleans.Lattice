using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeAppWorkspace"/> over gRPC. The
/// <see cref="LatticeAppWorkspaceApiGrpcClient"/> already implements the facade;
/// this per-circuit adapter delegates to it only so its transport faults map
/// through <see cref="ShellTransportFaults"/> like every other facade the Shell
/// consumes. The workspace is read on the circuit's own sign-in, which is what
/// scopes it to the apps that user may open.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellAppWorkspaceTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeAppWorkspaceApiGrpcClient>(channel, LatticeAppWorkspaceApiGrpcClient.Create), ILatticeAppWorkspace
{
    /// <inheritdoc />
    public Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.ListMyAppsAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.DescribeMyAppAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.GetIconAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        ArgumentException.ThrowIfNullOrEmpty(path);
        return CallAsync(
            (Slug: appSlug, Path: path),
            static (client, state, ct) => client.GetUiAssetAsync(state.Slug, state.Path, ct),
            null,
            cancellationToken);
    }
}
