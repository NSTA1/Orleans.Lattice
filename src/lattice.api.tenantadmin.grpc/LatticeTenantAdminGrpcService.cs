using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Abstract base for the tenant-administration control-API gRPC service. Carries
/// the <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the unary RPCs (the four tenant
/// lifecycle operations and <c>GetAuthScheme</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation resolved
/// from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeTenantAdminGrpcServiceBase.BindService"/> once at startup
/// with a <see langword="null"/> instance to record method metadata, then
/// resolves the actual instance per request.
/// </remarks>
[BindServiceMethod(typeof(LatticeTenantAdminGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeTenantAdminGrpcServiceBase
{
    /// <summary>Creates a new active tenant. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantCreationResult> CreateTenant(TenantAdminCreateRequest request, ServerCallContext context);

    /// <summary>Suspends an existing tenant. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantStatusChangeResult> SuspendTenant(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>Resumes a suspended tenant. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantStatusChangeResult> ResumeTenant(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>Deletes a tenant, cascading its trees. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantDeletionResult> DeleteTenant(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>Authors a tenant's resource quotas. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantQuotasUpdateResult> SetTenantQuotas(TenantAdminSetQuotasRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>
    /// Reports the caller's current tenant. Read-only self-service: this RPC is
    /// exempt from the tenant-admin authorization interceptor and defers to the
    /// facade's own fail-closed per-caller scoping. Implemented in
    /// <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantDescriptor> GetCurrentTenant(TenantSelfCurrentRequest request, ServerCallContext context);

    /// <summary>
    /// Lists the tenants the caller is authorized to see. Read-only self-service:
    /// this RPC is exempt from the tenant-admin authorization interceptor and defers
    /// to the facade's own fail-closed per-caller scoping. Implemented in
    /// <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantSelfDescriptorList> ListAccessibleTenants(TenantSelfListRequest request, ServerCallContext context);

    /// <summary>
    /// Reads the read-only status of one tenant the caller may see. Read-only
    /// self-service: this RPC is exempt from the tenant-admin authorization
    /// interceptor and defers to the facade's own fail-closed per-caller scoping
    /// (an unseeable tenant is indistinguishable from an absent one). Implemented in
    /// <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantStatusReport> GetTenant(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>
    /// Authorizes a tenant's allowed region set. An <b>operator-only</b> action:
    /// the facade authorizes it as cluster-wide admin on the reserved auth policy
    /// tree, independent of the data-plane default effect. Implemented in
    /// <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegions(TenantAdminRegionSetRequest request, ServerCallContext context);

    /// <summary>
    /// Sets a tenant's residency set within its allowed regions. An
    /// <b>operator-or-tenant-admin</b> action, enforced fail-closed by the facade.
    /// Implemented in <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantResidencyChangeResult> SetTenantResidency(TenantAdminRegionSetRequest request, ServerCallContext context);

    /// <summary>
    /// Reads a tenant's per-region residency status. Read-only, and an
    /// <b>operator-or-tenant-admin</b> action enforced fail-closed by the facade.
    /// Implemented in <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantRegionStatusReport> GetTenantRegionStatus(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>
    /// Reads a tenant's current usage against its quota ceilings. Read-only, and an
    /// <b>operator-or-tenant-admin</b> action enforced fail-closed by the facade
    /// (an unauthorized tenant is indistinguishable from an absent one).
    /// Implemented in <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantQuotaUsageReport> GetTenantQuotaUsage(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at startup
    /// with <paramref name="serviceImpl"/> set to <see langword="null"/> to record
    /// method metadata; the actual service instance is resolved per request from
    /// DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeTenantAdminGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeTenantAdminGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeTenantAdminGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeTenantAdminApiGrpcServiceCollectionExtensions.AddLatticeTenantAdminApiGrpc)} ran and that "
                + $"{nameof(LatticeTenantAdminApiGrpcServiceCollectionExtensions.MapLatticeTenantAdminApiGrpc)} pre-resolved "
                + "LatticeTenantAdminGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.CreateTenant, (UnaryServerMethod<TenantAdminCreateRequest, TenantCreationResult>?)null);
            binder.AddMethod(methods.SuspendTenant, (UnaryServerMethod<TenantAdminTenantRequest, TenantStatusChangeResult>?)null);
            binder.AddMethod(methods.ResumeTenant, (UnaryServerMethod<TenantAdminTenantRequest, TenantStatusChangeResult>?)null);
            binder.AddMethod(methods.DeleteTenant, (UnaryServerMethod<TenantAdminTenantRequest, TenantDeletionResult>?)null);
            binder.AddMethod(methods.SetTenantQuotas, (UnaryServerMethod<TenantAdminSetQuotasRequest, TenantQuotasUpdateResult>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            binder.AddMethod(methods.GetCurrentTenant, (UnaryServerMethod<TenantSelfCurrentRequest, TenantDescriptor>?)null);
            binder.AddMethod(methods.ListAccessibleTenants, (UnaryServerMethod<TenantSelfListRequest, TenantSelfDescriptorList>?)null);
            binder.AddMethod(methods.GetTenant, (UnaryServerMethod<TenantAdminTenantRequest, TenantStatusReport>?)null);
            binder.AddMethod(methods.AuthorizeAllowedRegions, (UnaryServerMethod<TenantAdminRegionSetRequest, TenantRegionAuthorizationResult>?)null);
            binder.AddMethod(methods.SetTenantResidency, (UnaryServerMethod<TenantAdminRegionSetRequest, TenantResidencyChangeResult>?)null);
            binder.AddMethod(methods.GetTenantRegionStatus, (UnaryServerMethod<TenantAdminTenantRequest, TenantRegionStatusReport>?)null);
            binder.AddMethod(methods.GetTenantQuotaUsage, (UnaryServerMethod<TenantAdminTenantRequest, TenantQuotaUsageReport>?)null);
            return;
        }

        binder.AddMethod(methods.CreateTenant, new UnaryServerMethod<TenantAdminCreateRequest, TenantCreationResult>(serviceImpl.CreateTenant));
        binder.AddMethod(methods.SuspendTenant, new UnaryServerMethod<TenantAdminTenantRequest, TenantStatusChangeResult>(serviceImpl.SuspendTenant));
        binder.AddMethod(methods.ResumeTenant, new UnaryServerMethod<TenantAdminTenantRequest, TenantStatusChangeResult>(serviceImpl.ResumeTenant));
        binder.AddMethod(methods.DeleteTenant, new UnaryServerMethod<TenantAdminTenantRequest, TenantDeletionResult>(serviceImpl.DeleteTenant));
        binder.AddMethod(methods.SetTenantQuotas, new UnaryServerMethod<TenantAdminSetQuotasRequest, TenantQuotasUpdateResult>(serviceImpl.SetTenantQuotas));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
        binder.AddMethod(methods.GetCurrentTenant, new UnaryServerMethod<TenantSelfCurrentRequest, TenantDescriptor>(serviceImpl.GetCurrentTenant));
        binder.AddMethod(methods.ListAccessibleTenants, new UnaryServerMethod<TenantSelfListRequest, TenantSelfDescriptorList>(serviceImpl.ListAccessibleTenants));
        binder.AddMethod(methods.GetTenant, new UnaryServerMethod<TenantAdminTenantRequest, TenantStatusReport>(serviceImpl.GetTenant));
        binder.AddMethod(methods.AuthorizeAllowedRegions, new UnaryServerMethod<TenantAdminRegionSetRequest, TenantRegionAuthorizationResult>(serviceImpl.AuthorizeAllowedRegions));
        binder.AddMethod(methods.SetTenantResidency, new UnaryServerMethod<TenantAdminRegionSetRequest, TenantResidencyChangeResult>(serviceImpl.SetTenantResidency));
        binder.AddMethod(methods.GetTenantRegionStatus, new UnaryServerMethod<TenantAdminTenantRequest, TenantRegionStatusReport>(serviceImpl.GetTenantRegionStatus));
        binder.AddMethod(methods.GetTenantQuotaUsage, new UnaryServerMethod<TenantAdminTenantRequest, TenantQuotaUsageReport>(serviceImpl.GetTenantQuotaUsage));
    }
}

/// <summary>
/// Server-side gRPC service for the tenant-administration control API. Adapts each
/// RPC onto the transport-agnostic <see cref="ILatticeTenantAdmin"/> facade,
/// mapping the facade's results onto the serializable wire responses and
/// translating argument failures, precondition failures, and authorization
/// denials onto gRPC status codes.
/// </summary>
internal sealed class LatticeTenantAdminGrpcService : LatticeTenantAdminGrpcServiceBase
{
    private readonly ILatticeTenantAdmin _control;
    private readonly ILatticeTenantSelfService _selfService;
    private readonly ILatticeTenantRegionAdmin? _regionAdmin;
    private readonly ILatticeTenantQuotaUsage? _quotaUsage;
    private readonly ILatticeTenantAdminApiCredentialBridge _credentialBridge;
    private readonly ILatticeTenantAdminApiAuthSchemeSource _authSchemeSource;
    private readonly IOptions<LatticeTenantAdminApiGrpcOptions> _options;
    private readonly ILogger<LatticeTenantAdminGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is unused
    /// in the body but load-bearing on the constructor: resolving it forces the DI
    /// container to build the <see cref="LatticeTenantAdminGrpcMethods"/> singleton
    /// (whose factory populates
    /// <see cref="LatticeTenantAdminGrpcMethodsHolder.Current"/>) before this
    /// service resolves, so the static
    /// <see cref="LatticeTenantAdminGrpcServiceBase.BindService"/> hook always
    /// observes a populated holder.
    /// </summary>
    /// <remarks>
    /// <paramref name="regionAdmin"/> and <paramref name="quotaUsage"/> are
    /// <b>optional</b>. The region-residency facade is a separate opt-in
    /// registration and the usage facade may be absent on a host running an older
    /// facade package, so a host that binds tenant administration without either
    /// must keep working exactly as it did before those RPCs existed: the binding
    /// still serves every lifetime and self-service RPC, and the missing RPCs
    /// answer <see cref="StatusCode.Unimplemented"/> rather than failing container
    /// construction at startup.
    /// </remarks>
    public LatticeTenantAdminGrpcService(
        LatticeTenantAdminGrpcMethods methods,
        ILatticeTenantAdmin control,
        ILatticeTenantSelfService selfService,
        ILatticeTenantAdminApiCredentialBridge credentialBridge,
        ILatticeTenantAdminApiAuthSchemeSource authSchemeSource,
        IOptions<LatticeTenantAdminApiGrpcOptions> options,
        ILogger<LatticeTenantAdminGrpcService> logger,
        ILatticeTenantRegionAdmin? regionAdmin = null,
        ILatticeTenantQuotaUsage? quotaUsage = null)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(selfService);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _control = control;
        _selfService = selfService;
        _regionAdmin = regionAdmin;
        _quotaUsage = quotaUsage;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Lifts the caller's asserted active tenant onto the ambient
    /// <see cref="LatticeActiveTenantContext"/> for the duration of the call, so
    /// the self-service surface reports the tenant the caller is actually acting
    /// as, and the admin surface sees that scope rather than the reserved default.
    /// Returns <see langword="null"/> (no scope, no allocation) when no tenant is
    /// asserted, so a tenancy-off cluster is unchanged. The assertion is
    /// re-validated against the caller's own membership downstream; this seam only
    /// carries it.
    /// </summary>
    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context,
            static (ctx, name) => ctx.RequestHeaders?.GetValue(name),
            _options.Value.ActiveTenantHeaderName);

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the facade's own fail-closed access gate resolves the caller's
    /// subject. Returns <see langword="null"/> (no scope) when the call carries no
    /// credential, leaving the caller anonymous. This is orthogonal to, and runs
    /// after, the transport-level <see cref="ILatticeTenantAdminApiAuthorizer"/>
    /// gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<TenantCreationResult> CreateTenant(TenantAdminCreateRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.CreateTenantAsync(req.TenantId, req.AdminSubjects, ct));

    /// <inheritdoc />
    public override Task<TenantStatusChangeResult> SuspendTenant(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.SuspendTenantAsync(req.TenantId, ct));

    /// <inheritdoc />
    public override Task<TenantStatusChangeResult> ResumeTenant(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.ResumeTenantAsync(req.TenantId, ct));

    /// <inheritdoc />
    public override Task<TenantDeletionResult> DeleteTenant(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.DeleteTenantAsync(req.TenantId, ct));

    /// <inheritdoc />
    public override Task<TenantQuotasUpdateResult> SetTenantQuotas(TenantAdminSetQuotasRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.SetTenantQuotasAsync(req.TenantId, req.Quotas, ct));

    /// <inheritdoc />
    public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // Unauthenticated by design (the interceptor exempts this method), so no
        // credential is bridged and only the public advertisement is returned.
        return Task.FromResult(_authSchemeSource.GetAdvertisement());
    }

    /// <inheritdoc />
    public override Task<TenantDescriptor> GetCurrentTenant(TenantSelfCurrentRequest request, ServerCallContext context)
        => InvokeSelfServiceAsync(request, context, static (service, _, ct) => service.GetCurrentTenantAsync(ct));

    /// <inheritdoc />
    public override async Task<TenantSelfDescriptorList> ListAccessibleTenants(TenantSelfListRequest request, ServerCallContext context)
    {
        var tenants = await InvokeSelfServiceAsync(
            request,
            context,
            static (service, _, ct) => service.ListAccessibleTenantsAsync(ct)).ConfigureAwait(false);
        return new TenantSelfDescriptorList { Tenants = tenants };
    }

    /// <inheritdoc />
    public override Task<TenantStatusReport> GetTenant(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeSelfServiceAsync(request, context, static (service, req, ct) => service.GetTenantAsync(req.TenantId, ct));

    /// <inheritdoc />
    public override Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegions(TenantAdminRegionSetRequest request, ServerCallContext context)
        => InvokeRegionAdminAsync(request, context, static (admin, req, ct) => admin.AuthorizeAllowedRegionsAsync(req.TenantId, req.Regions, ct));

    /// <inheritdoc />
    public override Task<TenantResidencyChangeResult> SetTenantResidency(TenantAdminRegionSetRequest request, ServerCallContext context)
        => InvokeRegionAdminAsync(request, context, static (admin, req, ct) => admin.SetResidencyAsync(req.TenantId, req.Regions, ct));

    /// <inheritdoc />
    public override Task<TenantRegionStatusReport> GetTenantRegionStatus(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeRegionAdminAsync(request, context, static (admin, req, ct) => admin.GetTenantRegionStatusAsync(req.TenantId, ct));

    /// <inheritdoc />
    public override async Task<TenantQuotaUsageReport> GetTenantQuotaUsage(TenantAdminTenantRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // The usage facade ships with the tenant-administration facade package, so
        // its absence means the host is running an older one. Answer honestly
        // rather than faulting container construction at startup, exactly as the
        // opt-in region-residency RPCs do.
        if (_quotaUsage is not { } quotaUsage)
        {
            throw new RpcException(new Status(
                StatusCode.Unimplemented,
                "This cluster does not serve tenant usage against quota. Register the "
                + "tenant usage facade to enable it."));
        }

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await quotaUsage
                .GetQuotaUsageAsync(request.TenantId, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The tenant usage request was cancelled."));
        }
        // The facade unifies an unauthorized tenant with an absent one into a
        // single not-found, so this is the only refusal a caller can observe and
        // the transport must not widen it back into a distinguishable denial.
        catch (TenantNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        // A fail-closed tenant resolution: the caller has no valid active tenant, or
        // may not act as the one it asserted. An authorization outcome, not a server
        // fault, so it must not fall through to Internal below.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TenantAdmin: gRPC tenant-usage call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tenant usage request failed."));
        }
    }

    /// <summary>
    /// Runs a per-tenant region-residency call under the caller-credential and
    /// asserted-tenant scopes, mapping the facade's outcomes onto gRPC status
    /// codes. Authorization stays entirely at the facade's own two-tier,
    /// fail-closed gate - operator-only for the allowed set, operator-or-tenant-admin
    /// for residency and status - which is independent of the data-plane default
    /// effect; the transport interceptor only applies the coarse per-operation host
    /// policy on top.
    /// </summary>
    /// <remarks>
    /// Every domain refusal this surface can raise has its own typed arm below and
    /// a deliberate status. In particular
    /// <see cref="TenantRegionNotAllowedException"/> and
    /// <see cref="TenantLastRegionException"/> derive directly from
    /// <see cref="Exception"/>, so without an explicit arm they would fall to the
    /// catch-all and reach the caller as an opaque <c>Internal</c> - the exact
    /// defect the tenant lifecycle surface already had to fix. Both are
    /// precondition breaches on a well-formed request, so both map to
    /// <c>FailedPrecondition</c>, which is also what their own contract documents.
    /// </remarks>
    private async Task<TResponse> InvokeRegionAdminAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeTenantRegionAdmin, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // The region-residency facade is a separate opt-in. A host that binds
        // tenant administration without it serves every other RPC unchanged and
        // answers these three honestly rather than faulting at startup.
        if (_regionAdmin is not { } regionAdmin)
        {
            throw new RpcException(new Status(
                StatusCode.Unimplemented,
                "This cluster does not serve tenant region residency. Register the "
                + "region-residency facade to enable it."));
        }

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(regionAdmin, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The tenant region-residency request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (TenantNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        // A region outside the tenant's allowed set, or a revoke of a region the
        // tenant is still resident in. The request is well-formed; the cluster
        // state refuses it, so it is a precondition breach, not a bad argument.
        catch (TenantRegionNotAllowedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        // The unbypassable last-resident-region guard. Also a precondition breach:
        // the caller must add a region before it can remove this one.
        catch (TenantLastRegionException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ReservedTenantOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        // A fail-closed tenant resolution: the caller has no valid active tenant, or
        // may not act as the one it asserted. An authorization outcome, not a server
        // fault, so it must not fall through to Internal below.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TenantAdmin: gRPC region-residency call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tenant region-residency request failed."));
        }
    }

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeTenantAdmin, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_control, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The tenant-administration control-API request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (TenantAlreadyExistsException ex)
        {
            // A create against an already-registered tenant: a distinct
            // precondition breach, surfaced as AlreadyExists so a client can tell
            // it apart from a bad argument.
            throw new RpcException(new Status(StatusCode.AlreadyExists, ex.Message));
        }
        catch (TenantNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ReservedTenantOperationException ex)
        {
            // An operation was attempted against the reserved default tenant that
            // can never be its target (for example delete or suspend). It is a
            // precondition failure, not a bad argument.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        // A fail-closed tenant resolution: the caller has no valid active tenant,
        // or may not act as the one it asserted. That is an authorization outcome,
        // not a server fault, so it must not fall through to Internal below - which
        // would replace the actionable reason with a generic message and invite a
        // client to retry a decision that will never change.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TenantAdmin: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tenant-administration control-API request failed."));
        }
    }

    /// <summary>
    /// Runs a read-only self-service call under the caller-credential scope, mapping
    /// the facade's fail-closed outcomes onto gRPC status codes. The self-service
    /// facade never mutates and unifies "no such tenant" with "not authorized to see
    /// it" into a single <see cref="TenantNotFoundException"/> (NotFound), so a
    /// caller can never probe for a tenant outside its authority; an invalid tenant
    /// id surfaces as InvalidArgument. The credential is stamped exactly as for the
    /// admin path, but the interceptor exempts these methods from the admin
    /// authorizer, so scoping is enforced solely at the facade - the single narrowest
    /// seam.
    /// </summary>
    private async Task<TResponse> InvokeSelfServiceAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeTenantSelfService, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_selfService, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The tenant self-service request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (TenantNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        // A fail-closed tenant resolution: the caller has no valid active tenant,
        // or may not act as the one it asserted. That is an authorization outcome,
        // not a server fault, so it must not fall through to Internal below - which
        // would replace the actionable reason with a generic message and invite a
        // client to retry a decision that will never change.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TenantAdmin: gRPC self-service call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tenant self-service request failed."));
        }
    }
}
