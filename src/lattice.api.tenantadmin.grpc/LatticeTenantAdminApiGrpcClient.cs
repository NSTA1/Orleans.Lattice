using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Strongly-typed client for the tenant-administration control-API gRPC surface.
/// Wraps a gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the full transport-agnostic <see cref="ILatticeTenantAdmin"/> facade
/// surface over the wire - the four tenant lifecycle operations (create, suspend,
/// resume, and delete with tree cascade) alongside the unauthenticated auth-scheme
/// discovery RPC. A management surface (dashboard, CLI) consumes the API through
/// this client rather than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service provider
/// that has Orleans serialization registered (<c>AddSerializer()</c>) so the wire
/// marshallers match the server exactly. Every operation flows through the single
/// <see cref="CallInvoker"/> seam, so the client can adopt region-aware call
/// routing without restructuring.
/// </remarks>
public sealed class LatticeTenantAdminApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeTenantAdminGrpcMethods _methods;

    internal LatticeTenantAdminApiGrpcClient(CallInvoker invoker, LatticeTenantAdminGrpcMethods methods)
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
    public static LatticeTenantAdminApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTenantAdminApiGrpcClient(
            callInvoker,
            LatticeTenantAdminGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Reads the endpoint's advertised auth schemes. Unauthenticated: this RPC is
    /// exempt from the server's authorization interceptor, so a client can learn
    /// how to sign in before it holds any credential.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The advertised auth schemes, in the server's preference order.</returns>
    public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.GetAuthScheme,
            new AuthSchemeAdvertisementRequest(),
            cancellationToken).ConfigureAwait(false);
        return response.Schemes;
    }

    /// <summary>
    /// Creates a new active tenant with the given id, seeding its tenant-admin
    /// subjects. Requires the caller to be authorized to administer tenants; fails
    /// closed otherwise.
    /// </summary>
    /// <remarks>
    /// Tenant visibility on the read-only self-service surface resolves from
    /// admin-subject membership, so a tenant created with no admin subjects is
    /// invisible to list and get. Passing <c>null</c> or an empty
    /// <paramref name="adminSubjects"/> asks the server to seed the calling
    /// subject, so a create followed by a read-back works out of the box; a
    /// non-empty collection overrides that default outright.
    /// </remarks>
    /// <param name="tenantId">The tenant id to create. Must not be <c>null</c> or empty.</param>
    /// <param name="adminSubjects">The tenant-admin subject ids to seed, or <c>null</c> / empty to seed the calling subject.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the creation, carrying the created tenant's id, status, and seeded admin subjects.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.CreateTenant,
            new TenantAdminCreateRequest
            {
                TenantId = tenantId,
                AdminSubjects = adminSubjects is null ? [] : [.. adminSubjects],
            },
            cancellationToken);
    }

    /// <summary>
    /// Suspends an existing tenant. A suspended tenant's trees remain intact but
    /// its data-plane operations are refused until it is resumed. Requires the
    /// caller to be authorized to administer tenants; fails closed otherwise.
    /// </summary>
    /// <param name="tenantId">The tenant id to suspend. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the transition, indicating whether the status changed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(_methods.SuspendTenant, new TenantAdminTenantRequest { TenantId = tenantId }, cancellationToken);
    }

    /// <summary>
    /// Resumes a suspended tenant, returning it to the active state. Requires the
    /// caller to be authorized to administer tenants; fails closed otherwise.
    /// </summary>
    /// <param name="tenantId">The tenant id to resume. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the transition, indicating whether the status changed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(_methods.ResumeTenant, new TenantAdminTenantRequest { TenantId = tenantId }, cancellationToken);
    }

    /// <summary>
    /// Deletes a tenant, cascading the removal of all trees the tenant owns. This
    /// is destructive and irreversible. Requires the caller to be authorized to
    /// administer tenants; fails closed otherwise.
    /// </summary>
    /// <param name="tenantId">The tenant id to delete. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the deletion, carrying the number of trees removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(_methods.DeleteTenant, new TenantAdminTenantRequest { TenantId = tenantId }, cancellationToken);
    }

    /// <summary>
    /// Authors a tenant's resource quotas and burst allowance, replacing whatever
    /// quotas the tenant currently carries. This is the control-plane surface for
    /// per-tenant capacity governance. Requires the caller to be authorized to
    /// administer tenants; fails closed otherwise.
    /// </summary>
    /// <param name="tenantId">The tenant id whose quotas to author. Must not be <c>null</c> or empty.</param>
    /// <param name="quotas">The quotas to apply. <see cref="TenantQuotasDescriptor.BurstPercent"/> must be non-negative.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The update result, carrying the quotas now in effect for the tenant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.SetTenantQuotas,
            new TenantAdminSetQuotasRequest { TenantId = tenantId, Quotas = quotas },
            cancellationToken);
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
