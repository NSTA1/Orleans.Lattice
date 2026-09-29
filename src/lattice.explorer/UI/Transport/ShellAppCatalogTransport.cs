using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeAppCatalog"/> over gRPC. The
/// <see cref="LatticeAppCatalogApiGrpcClient"/> already implements the facade;
/// this per-circuit adapter delegates to it only so its transport faults map
/// through <see cref="ShellTransportFaults"/> like every other facade the Shell
/// consumes.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellAppCatalogTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeAppCatalogApiGrpcClient>(channel, LatticeAppCatalogApiGrpcClient.Create), ILatticeAppCatalog
{
    /// <inheritdoc />
    public Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.ListSourcesAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return CallAsync(query, static (client, state, ct) => client.ListAvailableAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeFromSourceAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceKey);
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(
            (SourceKey: sourceKey, Slug: appSlug, Version: version),
            static (client, state, ct) => client.DescribeFromSourceAsync(state.SourceKey, state.Slug, state.Version, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppIconAsset?> GetIconAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceKey);
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(
            (SourceKey: sourceKey, Slug: appSlug, Version: version),
            static (client, state, ct) => client.GetIconAsync(state.SourceKey, state.Slug, state.Version, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.GetCapabilitiesAsync(ct), null, cancellationToken);
}
