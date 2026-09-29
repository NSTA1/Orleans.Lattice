using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantGrantAdmin"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s cross-tenant grant
/// RPCs, ported from the Tenancy plugin's <c>GrpcTenantAdminClient</c>.
/// </summary>
/// <remarks>
/// Listing and offering map faults through <see cref="ShellTenantFaults"/>. The
/// approve, reject and revoke transitions name an existing grant, and the facade
/// reports an unknown grant - and, deliberately, an unregistered granting tenant -
/// as <see cref="TenantGrantNotFoundException"/>, so a <c>NotFound</c> on those
/// three is rebuilt into that type with the grant's key.
/// </remarks>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantGrantAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantGrantAdmin
{
    /// <inheritdoc />
    public Task<TenantGrantReport> ListGrantsAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return CallAsync(tenantId, static (client, state, ct) => client.ListCrossTenantGrantsAsync(state, ct), tenantId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return CallAsync(
            (Granter: granterTenantId, Grantee: granteeTenantId, Scope: scope, Operations: operations),
            static (client, state, ct) => client.OfferCrossTenantGrantAsync(state.Granter, state.Grantee, state.Scope, state.Operations, ct),
            granterTenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return TransitionAsync(
            new GrantKey(granterTenantId, granteeTenantId, scope),
            static (client, key, ct) => client.ApproveCrossTenantGrantAsync(key.Granter, key.Grantee, key.Scope, ct),
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return TransitionAsync(
            new GrantKey(granterTenantId, granteeTenantId, scope),
            static (client, key, ct) => client.RejectCrossTenantGrantAsync(key.Granter, key.Grantee, key.Scope, ct),
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return TransitionAsync(
            new GrantKey(granterTenantId, granteeTenantId, scope),
            static (client, key, ct) => client.RevokeCrossTenantGrantAsync(key.Granter, key.Grantee, key.Scope, ct),
            cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantFaults.Map(exception, subject, cancellationToken);

    private static void ValidateGrantKey(string granterTenantId, string granteeTenantId, string scope)
    {
        ArgumentException.ThrowIfNullOrEmpty(granterTenantId);
        ArgumentException.ThrowIfNullOrEmpty(granteeTenantId);
        ArgumentException.ThrowIfNullOrEmpty(scope);
    }

    private async Task<TenantGrantChangeResult> TransitionAsync(
        GrantKey key,
        Func<LatticeTenantAdminApiGrpcClient, GrantKey, CancellationToken, Task<TenantGrantChangeResult>> call,
        CancellationToken cancellationToken)
    {
        try
        {
            return await call(Client, key, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound)
        {
            throw new TenantGrantNotFoundException(key.Granter, key.Grantee, key.Scope);
        }
        catch (RpcException exception)
        {
            throw ShellTenantFaults.Map(exception, key.Granter, cancellationToken);
        }
    }

    /// <summary>The key naming one cross-tenant grant.</summary>
    /// <param name="Granter">The granting tenant.</param>
    /// <param name="Grantee">The receiving tenant.</param>
    /// <param name="Scope">The granted scope.</param>
    private readonly record struct GrantKey(string Granter, string Grantee, string Scope);
}
