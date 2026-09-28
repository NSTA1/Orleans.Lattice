using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeAppsControl"/> over gRPC. The
/// <see cref="LatticeAppsApiGrpcClient"/> already implements the facade; this
/// per-circuit adapter delegates to it only so its transport faults map through
/// <see cref="ShellTransportFaults"/> like every other facade the Shell consumes.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellAppsControlTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeAppsApiGrpcClient>(channel, LatticeAppsApiGrpcClient.Create), ILatticeAppsControl
{
    /// <inheritdoc />
    public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.InstallAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.EnableAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.DisableAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.UninstallAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.ListAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(
            (Slug: appSlug, Version: version),
            static (client, state, ct) => client.DescribeAsync(state.Slug, state.Version, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSlug);
        return CallAsync(appSlug, static (client, state, ct) => client.GetConsentAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.UpdateConsentAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.GetCapabilitiesAsync(ct), null, cancellationToken);
}
