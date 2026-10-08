using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantRegionAdmin"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s region-residency
/// RPCs, ported from the Tenancy plugin's <c>GrpcTenantAdminClient</c>. Faults map
/// through <see cref="ShellTenantFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantRegionAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantRegionAdmin
{
    /// <inheritdoc />
    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(allowedRegions);
        return CallAsync(
            (TenantId: tenantId, Regions: allowedRegions),
            static (client, state, ct) => client.AuthorizeAllowedRegionsAsync(state.TenantId, state.Regions, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantResidencyChangeResult> SetResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(residencyRegions);
        return CallAsync(
            (TenantId: tenantId, Regions: residencyRegions),
            static (client, state, ct) => client.SetTenantResidencyAsync(state.TenantId, state.Regions, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.GetTenantRegionStatusAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantRegionStatusReport> AdvanceRegionAsync(
        string tenantId, string regionId, bool acknowledgeDataInPlace, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        return CallAsync(
            (TenantId: tenantId, RegionId: regionId, AcknowledgeDataInPlace: acknowledgeDataInPlace),
            static (client, state, ct) => client.AdvanceTenantRegionAsync(
                state.TenantId, state.RegionId, state.AcknowledgeDataInPlace, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);
}
