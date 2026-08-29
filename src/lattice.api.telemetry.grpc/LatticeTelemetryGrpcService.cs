using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Abstract base for the telemetry gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the unary RPCs (catalogue discovery, query
/// evaluation, and the unauthenticated auth-scheme advertisement).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation resolved
/// from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeTelemetryGrpcServiceBase.BindService"/> once at startup with
/// a <see langword="null"/> instance to record method metadata, then resolves the
/// actual instance per request.
/// </remarks>
[BindServiceMethod(typeof(LatticeTelemetryGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeTelemetryGrpcServiceBase
{
    /// <summary>
    /// Reads the curated named-query catalogue the caller may select from. The
    /// facade scopes it to the caller's entitlement, so an entry the caller may not
    /// run is absent rather than present-and-denied. Implemented in
    /// <see cref="LatticeTelemetryGrpcService"/>.
    /// </summary>
    /// <param name="request">The (field-less) catalogue request.</param>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>The caller's catalogue.</returns>
    public abstract Task<TelemetryQueryCatalog> GetCatalog(TelemetryCatalogRequest request, ServerCallContext context);

    /// <summary>
    /// Evaluates one curated query selected by id, under the tenant scope the
    /// facade derives from the authenticated caller. Implemented in
    /// <see cref="LatticeTelemetryGrpcService"/>.
    /// </summary>
    /// <param name="request">The query selection and its bounded parameters.</param>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>The evaluated series and the scope actually applied.</returns>
    public abstract Task<TelemetryQueryResponse> Query(TelemetryQueryRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeTelemetryGrpcService"/>.
    /// </summary>
    /// <param name="request">The (field-less) advertisement probe.</param>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>The advertised auth schemes.</returns>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at startup
    /// with <paramref name="serviceImpl"/> set to <see langword="null"/> to record
    /// method metadata; the actual service instance is resolved per request from
    /// DI.
    /// </summary>
    /// <param name="binder">The gRPC service binder.</param>
    /// <param name="serviceImpl">The service instance, or <see langword="null"/> during metadata discovery.</param>
    /// <exception cref="ArgumentNullException"><paramref name="binder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The method holder was not initialised before binding.</exception>
    public static void BindService(ServiceBinderBase binder, LatticeTelemetryGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeTelemetryGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeTelemetryGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeTelemetryApiGrpcServiceCollectionExtensions.AddLatticeTelemetryApiGrpc)} ran and that "
                + $"{nameof(LatticeTelemetryApiGrpcServiceCollectionExtensions.MapLatticeTelemetryApiGrpc)} pre-resolved "
                + "LatticeTelemetryGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.GetCatalog, (UnaryServerMethod<TelemetryCatalogRequest, TelemetryQueryCatalog>?)null);
            binder.AddMethod(methods.Query, (UnaryServerMethod<TelemetryQueryRequest, TelemetryQueryResponse>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            return;
        }

        binder.AddMethod(methods.GetCatalog, new UnaryServerMethod<TelemetryCatalogRequest, TelemetryQueryCatalog>(serviceImpl.GetCatalog));
        binder.AddMethod(methods.Query, new UnaryServerMethod<TelemetryQueryRequest, TelemetryQueryResponse>(serviceImpl.Query));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
    }
}

/// <summary>
/// Server-side gRPC service for the telemetry API. Adapts each RPC onto the
/// transport-agnostic <see cref="ILatticeTelemetry"/> facade and translates the
/// facade's typed refusals onto gRPC status codes.
/// </summary>
/// <remarks>
/// <para>
/// <b>Transport only.</b> The service derives no tenant, applies no visibility of
/// its own, and never rewrites the scope on a response. It bridges the caller's
/// credential and asserted active tenant onto the ambient context, forwards the
/// request the caller made, and returns whatever the facade decided - which is why
/// <see cref="TelemetryQueryResponse.Scope"/> reaches the client unmodified. The
/// facade is the single enforcement point.
/// </para>
/// <para>
/// A desktop client head therefore cannot widen its own scope by editing a
/// request: the widened visibility travels as a request, the facade validates it
/// server-side, and a refused widening comes back degraded and flagged rather than
/// honoured.
/// </para>
/// </remarks>
internal sealed class LatticeTelemetryGrpcService : LatticeTelemetryGrpcServiceBase
{
    private readonly ILatticeTelemetry _telemetry;
    private readonly ILatticeTelemetryApiCredentialBridge _credentialBridge;
    private readonly ILatticeTelemetryApiAuthSchemeSource _authSchemeSource;
    private readonly IOptions<LatticeTelemetryApiGrpcOptions> _options;
    private readonly ILogger<LatticeTelemetryGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is unused
    /// in the body but load-bearing on the constructor: resolving it forces the DI
    /// container to build the <see cref="LatticeTelemetryGrpcMethods"/> singleton
    /// (whose factory populates
    /// <see cref="LatticeTelemetryGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static
    /// <see cref="LatticeTelemetryGrpcServiceBase.BindService"/> hook always
    /// observes a populated holder.
    /// </summary>
    /// <param name="methods">The resolved method definitions.</param>
    /// <param name="telemetry">The transport-agnostic telemetry facade.</param>
    /// <param name="credentialBridge">The inbound credential bridge.</param>
    /// <param name="authSchemeSource">The auth-scheme advertisement source.</param>
    /// <param name="options">The binding options.</param>
    /// <param name="logger">The logger.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public LatticeTelemetryGrpcService(
        LatticeTelemetryGrpcMethods methods,
        ILatticeTelemetry telemetry,
        ILatticeTelemetryApiCredentialBridge credentialBridge,
        ILatticeTelemetryApiAuthSchemeSource authSchemeSource,
        IOptions<LatticeTelemetryApiGrpcOptions> options,
        ILogger<LatticeTelemetryGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(telemetry);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _telemetry = telemetry;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public override Task<TelemetryQueryCatalog> GetCatalog(TelemetryCatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (facade, _, ct) => facade.GetCatalogAsync(ct));

    /// <inheritdoc />
    public override Task<TelemetryQueryResponse> Query(TelemetryQueryRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (facade, req, ct) => facade.QueryAsync(req, ct));

    /// <inheritdoc />
    public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // Unauthenticated by design (the interceptor exempts this method), so no
        // credential is bridged and only the public advertisement is returned.
        return Task.FromResult(_authSchemeSource.GetAdvertisement());
    }

    /// <summary>
    /// Lifts the caller's asserted active tenant onto the ambient
    /// <see cref="LatticeActiveTenantContext"/> for the duration of the call, so
    /// the facade derives the tenant the caller is actually acting as rather than
    /// the reserved default. Returns <see langword="null"/> (no scope, no
    /// allocation) when no tenant is asserted, so a tenancy-off cluster is
    /// unchanged. The assertion is re-validated against the caller's own membership
    /// downstream; this seam only carries it and never treats it as an effective
    /// tenant.
    /// </summary>
    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context,
            static (ctx, name) => ctx.RequestHeaders?.GetValue(name),
            _options.Value.ActiveTenantHeaderName);

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the facade's own fail-closed gate resolves the caller's subject.
    /// Returns <see langword="null"/> (no scope) when the call carries no
    /// credential, leaving the caller anonymous. This is orthogonal to, and runs
    /// after, the transport-level <see cref="ILatticeTelemetryApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <summary>
    /// Runs a telemetry call under the caller-credential and asserted-tenant
    /// scopes, mapping the facade's typed outcomes onto gRPC status codes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every typed refusal the facade can raise derives directly from
    /// <see cref="Exception"/>, so without an explicit arm each would fall to the
    /// catch-all and reach the caller as an opaque <c>Internal</c>. The three are
    /// mapped apart deliberately, because they are different in kind:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <see cref="TelemetryQueryNotFoundException"/> - a caller error, and one that
    /// deliberately unifies "no such query" with "not offered to you", so the
    /// transport must keep them indistinguishable (<c>NotFound</c>).
    /// </item>
    /// <item>
    /// <see cref="TelemetryQueryBoundsException"/> - a caller error: a well-formed
    /// request whose window the entry's declared bounds refuse, hence
    /// <c>OutOfRange</c> rather than a bad argument.
    /// </item>
    /// <item>
    /// <see cref="TelemetryBackendException"/> - <b>not</b> the caller's fault: the
    /// metrics backend was unreachable, timed out, or answered unusably. It maps to
    /// <c>Unavailable</c>, the retryable-with-backoff code. Presenting it as a
    /// caller error would make a client abandon a transient outage as though its
    /// query were permanently invalid; presenting a caller error as this would make
    /// a client retry a bad query forever.
    /// </item>
    /// </list>
    /// <para>
    /// The backend fault's message embeds the underlying transport fault, which
    /// routinely carries the backend host or address. This facade is routable and
    /// its callers are untrusted, so the reason is logged server-side and the caller
    /// receives a fixed detail naming only the query id it already supplied.
    /// </para>
    /// </remarks>
    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeTelemetry, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_telemetry, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The telemetry request was cancelled."));
        }
        // Unknown and unoffered are deliberately indistinguishable at the facade;
        // keep them so on the wire. In particular an unconfigured backend is
        // reported here as not-found, which must not be "helpfully" re-mapped to an
        // availability fault: discovery and execution agree, because a catalogue
        // that offers nothing cannot then be told a query is unavailable for a
        // different reason.
        catch (TelemetryQueryNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        // A well-formed request whose window the entry's declared bounds refuse.
        catch (TelemetryQueryBoundsException ex)
        {
            throw new RpcException(new Status(StatusCode.OutOfRange, ex.Message));
        }
        // The backend failed us, not the caller. Retryable, and reported without the
        // underlying transport message, which can name the backend host.
        catch (TelemetryBackendException ex)
        {
            _logger.LogError(
                ex,
                "Api.Telemetry: the metrics backend failed query {QueryId} for {Method}.",
                ex.QueryId,
                context.Method);
            throw new RpcException(new Status(
                StatusCode.Unavailable,
                $"The telemetry backend could not answer query '{ex.QueryId}'. This is a backend "
                + "fault rather than a problem with the request; retry with backoff."));
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
        // fault, so it must not fall through to Internal below - which would replace
        // the actionable reason with a generic message and invite a client to retry
        // a decision that will never change.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Telemetry: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The telemetry request failed."));
        }
    }
}
