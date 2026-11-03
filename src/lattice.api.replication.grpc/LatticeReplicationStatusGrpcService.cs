using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Server-side gRPC service for the replication peer-status API. Adapts the
/// <c>GetPeerStatus</c> RPC onto the transport-agnostic
/// <see cref="ILatticeReplicationStatus"/> facade after bridging the caller's
/// credential and asserted tenant, and translates argument failures and
/// authorization or tenant denials onto gRPC status codes. The transport-level
/// <see cref="ILatticeReplicationApiAuthorizer"/> gate runs before this, in the
/// shared authorization interceptor.
/// </summary>
internal sealed class LatticeReplicationStatusGrpcService : LatticeReplicationStatusGrpcServiceBase
{
    private readonly ILatticeReplicationStatus _status;
    private readonly ILatticeReplicationApiCredentialBridge _credentialBridge;
    private readonly IOptions<LatticeReplicationApiGrpcOptions> _options;
    private readonly ILogger<LatticeReplicationStatusGrpcService> _logger;

    /// <summary>
    /// Initialises the service. <paramref name="methods"/> is unused in the body but
    /// resolving it forces the DI container to build the method singleton (whose
    /// factory populates <see cref="LatticeReplicationStatusGrpcMethodsHolder.Current"/>)
    /// before this service resolves.
    /// </summary>
    /// <param name="methods">The resolved method definitions. Must not be <see langword="null"/>.</param>
    /// <param name="status">The status facade. Must not be <see langword="null"/>.</param>
    /// <param name="credentialBridge">The credential bridge. Must not be <see langword="null"/>.</param>
    /// <param name="options">The binding options. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A dependency is <see langword="null"/>.</exception>
    public LatticeReplicationStatusGrpcService(
        LatticeReplicationStatusGrpcMethods methods,
        ILatticeReplicationStatus status,
        ILatticeReplicationApiCredentialBridge credentialBridge,
        IOptions<LatticeReplicationApiGrpcOptions> options,
        ILogger<LatticeReplicationStatusGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(status);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _status = status;
        _credentialBridge = credentialBridge;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Lifts the caller's asserted active tenant onto the ambient
    /// <see cref="LatticeActiveTenantContext"/> for the duration of the call, so the
    /// facade renders and scopes tree ids for the caller's tenant rather than the
    /// reserved default. Returns <see langword="null"/> when no tenant is asserted.
    /// The assertion is re-validated against the caller's own membership
    /// downstream; this seam only carries it.
    /// </summary>
    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context,
            static (ctx, name) => ctx.RequestHeaders?.GetValue(name),
            _options.Value.ActiveTenantHeaderName);

    /// <inheritdoc />
    public override async Task<ReplicationPeerStatusPage> GetPeerStatus(ReplicationPeerStatusQuery request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        var credential = _credentialBridge.Resolve(context);
        using var credentialScope = credential is null ? null : LatticeCredentialContext.With(credential);
        using var activeTenantScope = StampActiveTenant(context);

        try
        {
            return await _status.GetPeerStatusAsync(request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The replication peer-status request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (LatticeSaturatedException)
        {
            // Storage back-pressure is a deliberate "busy, retry later" refusal (#5723):
            // report the retryable ResourceExhausted, not a precondition or server fault.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, "The store is busy (storage back-pressure) and the request was refused. Retry after a short backoff."));
        }
        catch (TimeoutException ex)
        {
            // A grain call timed out under load (#5742): the outcome is unknown, so report
            // DeadlineExceeded - never Internal, and never a failure the client may assume.
            _logger.LogWarning(ex, "Api.Replication: gRPC call to {Method} timed out inside the cluster; outcome unknown.", context.Method);
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "The request did not complete in time inside the cluster, so its outcome is unknown. Confirm the result before retrying a write."));
        }
        catch (OrleansMessageRejectionException ex)
        {
            // A silo was unavailable or rejected the message (#5742): a transient cluster
            // fault, reported as the retryable Unavailable rather than a server error.
            _logger.LogWarning(ex, "Api.Replication: gRPC call to {Method} hit an unavailable cluster node.", context.Method);
            throw new RpcException(new Status(StatusCode.Unavailable, "A cluster node was temporarily unavailable. Retry after a short backoff."));
        }
        catch (EnumerationAbortedException ex)
        {
            // A remote enumerator was lost to a grain deactivation mid-read (#5742): transient,
            // cleared by reading again, so the retryable Unavailable rather than a server error.
            _logger.LogWarning(ex, "Api.Replication: gRPC call to {Method} lost a remote enumeration.", context.Method);
            throw new RpcException(new Status(StatusCode.Unavailable, "A cluster node lost the read in progress. Retry after a short backoff."));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Replication: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The replication peer-status request failed."));
        }
    }
}
