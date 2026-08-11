using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Abstract base for the tree-administration control-API gRPC service. Carries
/// the <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the unary RPCs
/// (<c>ProbeCapabilities</c>, <c>GetAuthScheme</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation resolved
/// from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeTreeAdminGrpcServiceBase.BindService"/> once at startup with
/// a <see langword="null"/> instance to record method metadata, then resolves the
/// actual instance per request.
/// </remarks>
[BindServiceMethod(typeof(LatticeTreeAdminGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeTreeAdminGrpcServiceBase
{
    /// <summary>Probes the caller's tree-administration capabilities for a tree. Implemented in <see cref="LatticeTreeAdminGrpcService"/>.</summary>
    public abstract Task<LatticeTreeAdminCapabilities> ProbeCapabilities(TreeAdminTreeRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeTreeAdminGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at startup
    /// with <paramref name="serviceImpl"/> set to <see langword="null"/> to record
    /// method metadata; the actual service instance is resolved per request from
    /// DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeTreeAdminGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeTreeAdminGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeTreeAdminGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeTreeAdminApiGrpcServiceCollectionExtensions.AddLatticeTreeAdminApiGrpc)} ran and that "
                + $"{nameof(LatticeTreeAdminApiGrpcServiceCollectionExtensions.MapLatticeTreeAdminApiGrpc)} pre-resolved "
                + "LatticeTreeAdminGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.ProbeCapabilities, (UnaryServerMethod<TreeAdminTreeRequest, LatticeTreeAdminCapabilities>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            return;
        }

        binder.AddMethod(methods.ProbeCapabilities, new UnaryServerMethod<TreeAdminTreeRequest, LatticeTreeAdminCapabilities>(serviceImpl.ProbeCapabilities));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
    }
}

/// <summary>
/// Server-side gRPC service for the tree-administration control API. Adapts each
/// RPC onto the transport-agnostic <see cref="ILatticeTreeAdmin"/> facade, mapping
/// the facade's results onto the serializable wire responses and translating
/// argument failures, precondition failures, and authorization denials onto gRPC
/// status codes.
/// </summary>
internal sealed class LatticeTreeAdminGrpcService : LatticeTreeAdminGrpcServiceBase
{
    private readonly ILatticeTreeAdmin _control;
    private readonly ILatticeTreeAdminApiCredentialBridge _credentialBridge;
    private readonly ILatticeTreeAdminApiAuthSchemeSource _authSchemeSource;
    private readonly ILogger<LatticeTreeAdminGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is unused
    /// in the body but load-bearing on the constructor: resolving it forces the DI
    /// container to build the <see cref="LatticeTreeAdminGrpcMethods"/> singleton
    /// (whose factory populates
    /// <see cref="LatticeTreeAdminGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static
    /// <see cref="LatticeTreeAdminGrpcServiceBase.BindService"/> hook always
    /// observes a populated holder.
    /// </summary>
    public LatticeTreeAdminGrpcService(
        LatticeTreeAdminGrpcMethods methods,
        ILatticeTreeAdmin control,
        ILatticeTreeAdminApiCredentialBridge credentialBridge,
        ILatticeTreeAdminApiAuthSchemeSource authSchemeSource,
        ILogger<LatticeTreeAdminGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(logger);

        _control = control;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the composed facade's own fail-closed access gate resolves the
    /// caller's subject. Returns <see langword="null"/> (no scope) when the call
    /// carries no credential, leaving the caller anonymous. This is orthogonal to,
    /// and runs after, the transport-level
    /// <see cref="ILatticeTreeAdminApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<LatticeTreeAdminCapabilities> ProbeCapabilities(TreeAdminTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.ProbeCapabilitiesAsync(req.TreeId, ct));

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
        Func<ILatticeTreeAdmin, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

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
            throw new RpcException(new Status(StatusCode.Cancelled, "The tree-administration control-API request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            // A precondition failure surfaced by the composed facade. The message
            // is safe and actionable (no secrets), so surface it as
            // FailedPrecondition instead of the opaque Internal below.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TreeAdmin: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tree-administration control-API request failed."));
        }
    }
}
