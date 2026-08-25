using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Strongly-typed client for the read-only tenant <b>self-service</b> RPCs on the
/// tenant-administration gRPC surface. Wraps a gRPC <see cref="CallInvoker"/> and
/// the code-first method definitions, re-exposing the transport-agnostic
/// <see cref="ILatticeTenantSelfService"/> facade surface over the wire - the
/// caller's current tenant, the tenants it may access, and the read-only status of
/// one such tenant. A management surface (dashboard, CLI) or a split MCP head
/// consumes tenant self-awareness through this client rather than hand-rolling
/// channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service provider
/// that has Orleans serialization registered (<c>AddSerializer()</c>) so the wire
/// marshallers match the server exactly. The self-service RPCs share the
/// tenant-administration gRPC service but are exempt from its default-deny
/// authorizer: the server stamps the forwarded caller credential and defers to the
/// facade's own fail-closed per-caller scoping, so an anonymous or non-privileged
/// caller sees only its own default context, an empty accessible list, and a
/// fail-closed not-found on inspect. Every operation flows through the single
/// <see cref="CallInvoker"/> seam, so the client can adopt region-aware call routing
/// without restructuring.
/// </remarks>
public sealed class LatticeTenantSelfServiceApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeTenantAdminGrpcMethods _methods;

    internal LatticeTenantSelfServiceApiGrpcClient(CallInvoker invoker, LatticeTenantAdminGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    /// <returns>A ready-to-use client.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static LatticeTenantSelfServiceApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTenantSelfServiceApiGrpcClient(
            callInvoker,
            LatticeTenantAdminGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Resolves the tenant the caller's own credential is operating as. Requires no
    /// special authorization; a caller with no tenant in context resolves to the
    /// reserved default tenant.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A descriptor for the caller's current tenant.</returns>
    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetCurrentTenant, new TenantSelfCurrentRequest(), cancellationToken);

    /// <summary>
    /// Lists the tenants the caller is authorized to access, ascending by id. Scoped
    /// fail-closed to the caller's resolved subject: an anonymous or non-privileged
    /// caller under the default tenant gets an empty list.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The accessible tenants, ascending by id; empty when none.</returns>
    public async Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.ListAccessibleTenants,
            new TenantSelfListRequest(),
            cancellationToken).ConfigureAwait(false);
        return response.Tenants;
    }

    /// <summary>
    /// Reads the read-only lifecycle status and per-region residency of one tenant
    /// the caller is authorized to see. Fails closed with a
    /// <see cref="TenantNotFoundException"/> when the tenant does not exist or the
    /// caller is not authorized to see it - the two cases are deliberately
    /// indistinguishable.
    /// </summary>
    /// <param name="tenantId">The tenant id to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The read-only status report for the tenant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(_methods.GetTenant, new TenantAdminTenantRequest { TenantId = tenantId }, cancellationToken);
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
