using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Strongly-typed client for the tenant-administration control-API gRPC surface.
/// Wraps a gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the full transport-agnostic <see cref="ILatticeTenantAdmin"/> facade
/// surface over the wire - the four tenant lifecycle operations (create, suspend,
/// resume, and delete with tree cascade) alongside the unauthenticated auth-scheme
/// discovery RPC - plus the three <see cref="ILatticeTenantRegionAdmin"/>
/// region-residency operations (authorize allowed regions, set residency, read
/// per-region status) and the three <see cref="ILatticeTenantAccessAdmin"/>
/// admin-subject operations (list, add, remove). A management surface (dashboard,
/// CLI) consumes the API through this client rather than hand-rolling channel
/// calls.
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

    /// <summary>
    /// Authorizes a tenant's allowed region set, replacing it with exactly
    /// <paramref name="allowedRegions"/>: absent regions are revoked. An
    /// <b>operator</b> action - the server authorizes it as cluster-wide admin on
    /// the reserved auth policy tree and denies every non-operator caller
    /// regardless of the data-plane default effect. Revoking a region the tenant is
    /// still resident in is refused with <c>FailedPrecondition</c>.
    /// </summary>
    /// <param name="tenantId">The tenant id whose allowed set to author. Must not be <c>null</c> or empty.</param>
    /// <param name="allowedRegions">The complete desired allowed region set. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The authorization result with the resulting allowed region ids.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="allowedRegions"/> is <c>null</c>.</exception>
    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(allowedRegions);

        return UnaryAsync(
            _methods.AuthorizeAllowedRegions,
            new TenantAdminRegionSetRequest { TenantId = tenantId, Regions = [.. allowedRegions] },
            cancellationToken);
    }

    /// <summary>
    /// Sets a tenant's residency set within its allowed regions, replacing it with
    /// exactly <paramref name="residencyRegions"/>: newly-listed regions begin
    /// adding, and currently-resident regions absent from the set begin draining. A
    /// <b>tenant-admin</b> action - the server authorizes the caller as the platform
    /// operator <b>or</b> a live admin subject on the tenant record. A region
    /// outside the allowed set, and a change that would remove the last resident
    /// region, are both refused with <c>FailedPrecondition</c>.
    /// </summary>
    /// <param name="tenantId">The tenant id whose residency to author. Must not be <c>null</c> or empty.</param>
    /// <param name="residencyRegions">The complete desired residency set. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The residency-change result with the added, removed, and resulting regions.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="residencyRegions"/> is <c>null</c>.</exception>
    public Task<TenantResidencyChangeResult> SetTenantResidencyAsync(
        string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(residencyRegions);

        return UnaryAsync(
            _methods.SetTenantResidency,
            new TenantAdminRegionSetRequest { TenantId = tenantId, Regions = [.. residencyRegions] },
            cancellationToken);
    }

    /// <summary>
    /// Reads a tenant's per-region residency status: one row per region that is
    /// either allowed or carries a non-<c>None</c> status, ordered by region id.
    /// Read-only, and a <b>tenant-admin</b> action - the server authorizes the caller
    /// as the platform operator <b>or</b> a live admin subject on the tenant record.
    /// </summary>
    /// <param name="tenantId">The tenant id to report on. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The per-region status report.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.GetTenantRegionStatus,
            new TenantAdminTenantRequest { TenantId = tenantId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a tenant's current usage against its quota ceilings: per dimension the
    /// consumption, the steady-state ceiling, the burst-adjusted admission ceiling,
    /// and the live and accrued overage, qualified by the enforcement scope the
    /// figures were read under. Read-only, and a <b>tenant-admin</b> action - the
    /// server authorizes the caller as the platform operator <b>or</b> a live admin
    /// subject on the tenant record, and answers <c>NotFound</c> both when the
    /// tenant does not exist and when the caller may not read it, so the call can
    /// never be used to probe for tenant existence.
    /// </summary>
    /// <param name="tenantId">The tenant id to report on. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tenant's usage-against-quota report.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.GetTenantQuotaUsage,
            new TenantAdminTenantRequest { TenantId = tenantId },
            cancellationToken);
    }

    /// <summary>
    /// Lists the subjects that hold tenant-admin authority over a tenant, in
    /// ordinal order. Read-only, and a <b>tenant-admin</b> action - the server
    /// authorizes the caller as the platform operator <b>or</b> a live admin
    /// subject on the tenant record, and answers a non-operator caller naming a
    /// tenant it does not administer with <c>PermissionDenied</c> rather than
    /// <c>NotFound</c>, so tenant existence cannot be probed.
    /// </summary>
    /// <param name="tenantId">The tenant id whose admin subjects to list. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tenant's live admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c> or empty.</exception>
    public Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.ListTenantAdminSubjects,
            new TenantAdminTenantRequest { TenantId = tenantId },
            cancellationToken);
    }

    /// <summary>
    /// Grants <paramref name="subjectId"/> tenant-admin authority over
    /// <paramref name="tenantId"/>. A <b>tenant-admin</b> action - the server
    /// authorizes the caller as the platform operator <b>or</b> a live admin
    /// subject on the tenant record. Idempotent: granting an existing member
    /// reports <see cref="TenantAdminSubjectChangeResult.Changed"/>
    /// <see langword="false"/>. The reserved default tenant is refused with
    /// <c>FailedPrecondition</c>.
    /// </summary>
    /// <param name="tenantId">The tenant id to grant authority over. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectId">The subject id to grant tenant-admin authority to. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> or <paramref name="subjectId"/> is <c>null</c> or empty.</exception>
    public Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return UnaryAsync(
            _methods.AddTenantAdminSubject,
            new TenantAdminSubjectRequest { TenantId = tenantId, SubjectId = subjectId },
            cancellationToken);
    }

    /// <summary>
    /// Revokes <paramref name="subjectId"/>'s tenant-admin authority over
    /// <paramref name="tenantId"/>. A <b>tenant-admin</b> action - the server
    /// authorizes the caller as the platform operator <b>or</b> a live admin
    /// subject on the tenant record. Idempotent: revoking a non-member reports
    /// <see cref="TenantAdminSubjectChangeResult.Changed"/> <see langword="false"/>.
    /// Removing the tenant's last admin subject, and any mutation of the reserved
    /// default tenant, are refused with <c>FailedPrecondition</c>.
    /// </summary>
    /// <param name="tenantId">The tenant id to revoke authority over. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectId">The subject id to revoke tenant-admin authority from. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> or <paramref name="subjectId"/> is <c>null</c> or empty.</exception>
    public Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return UnaryAsync(
            _methods.RemoveTenantAdminSubject,
            new TenantAdminSubjectRequest { TenantId = tenantId, SubjectId = subjectId },
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
