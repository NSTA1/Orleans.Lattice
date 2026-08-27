using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Abstract base for the replication control-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the unary RPCs (<c>EnableReplication</c>,
/// <c>DisableReplication</c>, <c>GetReplicationConfig</c>, <c>GetAuthScheme</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation
/// resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="BindService"/> once at startup with a <see langword="null"/>
/// instance to record method metadata, then resolves the actual instance per
/// request.
/// </remarks>
[BindServiceMethod(typeof(LatticeReplicationGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeReplicationGrpcServiceBase
{
    /// <summary>Enables replication for a tree. Implemented in <see cref="LatticeReplicationGrpcService"/>.</summary>
    public abstract Task<ReplicationEnableResponse> EnableReplication(ReplicationEnableRequestMessage request, ServerCallContext context);

    /// <summary>Disables replication for a tree. Implemented in <see cref="LatticeReplicationGrpcService"/>.</summary>
    public abstract Task<ReplicationDisableResponse> DisableReplication(ReplicationDisableRequestMessage request, ServerCallContext context);

    /// <summary>Reports the runtime replicated-tree set. Implemented in <see cref="LatticeReplicationGrpcService"/>.</summary>
    public abstract Task<ReplicationConfigResponse> GetReplicationConfig(ReplicationGetConfigRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual service
    /// instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeReplicationGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeReplicationGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeReplicationGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationApiGrpcServiceCollectionExtensions.AddLatticeReplicationApiGrpc)} ran and that "
                + $"{nameof(LatticeReplicationApiGrpcServiceCollectionExtensions.MapLatticeReplicationApiGrpc)} pre-resolved "
                + "LatticeReplicationGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.EnableReplication, (UnaryServerMethod<ReplicationEnableRequestMessage, ReplicationEnableResponse>?)null);
            binder.AddMethod(methods.DisableReplication, (UnaryServerMethod<ReplicationDisableRequestMessage, ReplicationDisableResponse>?)null);
            binder.AddMethod(methods.GetReplicationConfig, (UnaryServerMethod<ReplicationGetConfigRequest, ReplicationConfigResponse>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            return;
        }

        binder.AddMethod(methods.EnableReplication, new UnaryServerMethod<ReplicationEnableRequestMessage, ReplicationEnableResponse>(serviceImpl.EnableReplication));
        binder.AddMethod(methods.DisableReplication, new UnaryServerMethod<ReplicationDisableRequestMessage, ReplicationDisableResponse>(serviceImpl.DisableReplication));
        binder.AddMethod(methods.GetReplicationConfig, new UnaryServerMethod<ReplicationGetConfigRequest, ReplicationConfigResponse>(serviceImpl.GetReplicationConfig));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
    }
}

/// <summary>
/// Server-side gRPC service for the replication control API. Adapts each RPC
/// onto the transport-agnostic <see cref="ILatticeReplicationControl"/> facade,
/// mapping the facade's plain result records onto the serializable wire
/// responses and translating the engine's precondition / mode-change failures,
/// argument failures, and authorization denials onto gRPC status codes.
/// </summary>
internal sealed class LatticeReplicationGrpcService : LatticeReplicationGrpcServiceBase
{
    private readonly ILatticeReplicationControl _control;
    private readonly ILatticeReplicationApiCredentialBridge _credentialBridge;
    private readonly ILatticeReplicationApiAuthSchemeSource _authSchemeSource;
    private readonly IOptions<LatticeReplicationApiGrpcOptions> _options;
    private readonly ILogger<LatticeReplicationGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the
    /// <see cref="LatticeReplicationGrpcMethods"/> singleton (whose factory
    /// populates <see cref="LatticeReplicationGrpcMethodsHolder.Current"/>) before
    /// this service resolves, so the static
    /// <see cref="LatticeReplicationGrpcServiceBase.BindService"/> hook always
    /// observes a populated holder.
    /// </summary>
    public LatticeReplicationGrpcService(
        LatticeReplicationGrpcMethods methods,
        ILatticeReplicationControl control,
        ILatticeReplicationApiCredentialBridge credentialBridge,
        ILatticeReplicationApiAuthSchemeSource authSchemeSource,
        IOptions<LatticeReplicationApiGrpcOptions> options,
        ILogger<LatticeReplicationGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _control = control;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the replication engine's own fail-closed access gate resolves the
    /// caller's subject. Returns <see langword="null"/> (no scope) when the call
    /// carries no credential, leaving the caller anonymous. This is orthogonal
    /// to, and runs after, the transport-level
    /// <see cref="ILatticeReplicationApiAuthorizer"/> gate.
    /// </summary>
    /// <summary>
    /// Lifts the caller's asserted active tenant onto the ambient
    /// <see cref="LatticeActiveTenantContext"/> for the duration of the call, so
    /// this facade's tenant-scoped name resolution sees the caller's tenant rather
    /// than the reserved default. Returns <see langword="null"/> (no scope, no
    /// allocation) when no tenant is asserted, so a tenancy-off cluster is
    /// unchanged. The assertion is re-validated against the caller's own
    /// membership downstream; this seam only carries it.
    /// </summary>
    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context,
            static (ctx, name) => ctx.RequestHeaders?.GetValue(name),
            _options.Value.ActiveTenantHeaderName);

    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<ReplicationEnableResponse> EnableReplication(ReplicationEnableRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control
                .EnableReplicationAsync(req.TreeId, req.Mode, NullIfEmpty(req.BootstrapSourceClusterId), ct)
                .ConfigureAwait(false);
            return new ReplicationEnableResponse
            {
                TreeId = result.TreeId,
                Mode = result.Mode,
                AlreadyEnabled = result.AlreadyEnabled,
                BootstrapRequested = result.BootstrapRequested,
            };
        });

    /// <inheritdoc />
    public override Task<ReplicationDisableResponse> DisableReplication(ReplicationDisableRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control.DisableReplicationAsync(req.TreeId, ct).ConfigureAwait(false);
            return new ReplicationDisableResponse
            {
                TreeId = result.TreeId,
                AlreadyDisabled = result.AlreadyDisabled,
            };
        });

    /// <inheritdoc />
    public override Task<ReplicationConfigResponse> GetReplicationConfig(ReplicationGetConfigRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, _, ct) =>
        {
            var report = await control.GetReplicationConfigAsync(ct).ConfigureAwait(false);
            var trees = new List<ReplicationTreeConfigMessage>(report.Trees.Count);
            foreach (var entry in report.Trees)
            {
                trees.Add(new ReplicationTreeConfigMessage
                {
                    TreeId = entry.TreeId,
                    Enabled = entry.Enabled,
                    HasMode = entry.Mode.HasValue,
                    Mode = entry.Mode ?? default,
                    Ambiguous = entry.Ambiguous,
                });
            }

            return new ReplicationConfigResponse { Trees = trees };
        });

    /// <inheritdoc />
    public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // Unauthenticated by design (the interceptor exempts this method), so no
        // credential is bridged and only the public advertisement is returned.
        return Task.FromResult(_authSchemeSource.GetAdvertisement());
    }

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeReplicationControl, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);
        using var activeTenantScope = StampActiveTenant(context);

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
            throw new RpcException(new Status(StatusCode.Cancelled, "The replication control-API request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (LatticeReplicationPreconditionFailedException ex)
        {
            // A runtime precondition for enabling replication was not met (for
            // example a flag-based merge mode without a configured local replica
            // id). It is a precondition failure, not an internal fault, and its
            // message is safe and actionable.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (LatticeReplicationModeChangeRejectedException ex)
        {
            // An enable would change the merge mode of an already-enabled tree;
            // the sanctioned path is disable-then-re-enable. A precondition
            // failure the operator can act on, not an internal fault.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Replication: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The replication control-API request failed."));
        }
    }

    private static string? NullIfEmpty(string? value) =>
        string.IsNullOrEmpty(value) ? null : value;
}
