using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Abstract base class for the gRPC saga control RPCs
/// (<c>Prepare</c>, <c>Commit</c>, <c>Abort</c>, <c>GetStatus</c>).
/// Carries the <see cref="BindServiceMethodAttribute"/> that
/// <c>Grpc.AspNetCore</c> reflects against to discover and register the
/// four saga routes. Mirrors the metadata/derived split
/// <see cref="LatticeRemoteSnapshotGrpcServiceBase"/> uses so the three
/// sibling services share a single registration shape.
/// </summary>
[BindServiceMethod(typeof(LatticeSagaGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeSagaGrpcServiceBase
{
    /// <summary>Handles the unary <c>Prepare</c> RPC.</summary>
    public abstract Task<SagaControlResponseBox> Prepare(SagaControlRequestBox request, ServerCallContext context);

    /// <summary>Handles the unary <c>Commit</c> RPC.</summary>
    public abstract Task<SagaControlResponseBox> Commit(SagaControlRequestBox request, ServerCallContext context);

    /// <summary>Handles the unary <c>Abort</c> RPC.</summary>
    public abstract Task<SagaControlResponseBox> Abort(SagaControlRequestBox request, ServerCallContext context);

    /// <summary>Handles the unary <c>GetStatus</c> RPC.</summary>
    public abstract Task<SagaControlResponseBox> GetStatus(SagaControlRequestBox request, ServerCallContext context);

    /// <summary>Handles the unary <c>GetDecision</c> RPC (issue #4637).</summary>
    public abstract Task<SagaControlResponseBox> GetDecision(SagaControlRequestBox request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once
    /// at startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual
    /// service instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeSagaGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeSagaGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeSagaGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc)} "
                + "ran and that "
                + $"{nameof(LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc)} "
                + "pre-resolved LatticeSagaGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.Prepare, (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>?)null);
            binder.AddMethod(methods.Commit, (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>?)null);
            binder.AddMethod(methods.Abort, (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>?)null);
            binder.AddMethod(methods.GetStatus, (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>?)null);
            binder.AddMethod(methods.GetDecision, (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>?)null);
            return;
        }

        binder.AddMethod(methods.Prepare,
            new UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>(serviceImpl.Prepare));
        binder.AddMethod(methods.Commit,
            new UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>(serviceImpl.Commit));
        binder.AddMethod(methods.Abort,
            new UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>(serviceImpl.Abort));
        binder.AddMethod(methods.GetStatus,
            new UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>(serviceImpl.GetStatus));
        binder.AddMethod(methods.GetDecision,
            new UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>(serviceImpl.GetDecision));
    }
}

/// <summary>
/// Process-wide holder for the resolved
/// <see cref="LatticeSagaGrpcMethods"/> singleton. Populated by
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc"/>;
/// consumed by the static
/// <see cref="LatticeSagaGrpcServiceBase.BindService"/> callback that
/// gRPC's reflection invokes at startup. Mirrors
/// <see cref="LatticeRemoteSnapshotGrpcMethodsHolder"/> for the same
/// static-bridge reason.
/// </summary>
internal static class LatticeSagaGrpcMethodsHolder
{
    /// <summary>
    /// The current resolved <see cref="LatticeSagaGrpcMethods"/>, or
    /// <see langword="null"/> if registration has not yet occurred.
    /// </summary>
    public static LatticeSagaGrpcMethods? Current { get; set; }
}

/// <summary>
/// Server-side gRPC service that exposes the participant-side
/// <see cref="ILatticeSagaControlHandler"/> to remote coordinators over
/// the <c>orleans.lattice.replication.LatticeSaga</c> service. Unlike
/// the additive replication data plane, these imperative calls mutate
/// participant state, so every method enforces an explicit
/// peer-authorization gate (via <see cref="ISagaPeerAuthorizer"/>)
/// before delegating to the handler: an unauthorized origin cluster is
/// rejected with <see cref="StatusCode.PermissionDenied"/> before any
/// state change.
/// </summary>
internal sealed class LatticeSagaGrpcService : LatticeSagaGrpcServiceBase
{
    private readonly ILatticeSagaControlHandler _handler;
    private readonly ISagaPeerAuthorizer _authorizer;
    private readonly ILogger<LatticeSagaGrpcService> _logger;
    private readonly Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> _prepare;
    private readonly Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> _commit;
    private readonly Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> _abort;
    private readonly Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> _getStatus;
    private readonly Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> _getDecision;

    /// <summary>
    /// Initialises the service with its dependencies. The
    /// <paramref name="methods"/> parameter is unused inside the service
    /// body but its presence on the constructor is load-bearing: it
    /// forces the DI container to resolve the
    /// <see cref="LatticeSagaGrpcMethods"/> singleton (whose factory
    /// populates <see cref="LatticeSagaGrpcMethodsHolder.Current"/>)
    /// before this service resolves, so the static
    /// <see cref="LatticeSagaGrpcServiceBase.BindService"/> hook always
    /// observes a populated holder.
    /// </summary>
    public LatticeSagaGrpcService(
        LatticeSagaGrpcMethods methods,
        ILatticeSagaControlHandler handler,
        ISagaPeerAuthorizer authorizer,
        ILogger<LatticeSagaGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(logger);

        _handler = handler;
        _authorizer = authorizer;
        _logger = logger;

        // Cache the per-operation handler delegates once (this service is
        // a singleton) so each RPC does not allocate a capturing closure.
        _prepare = _handler.PrepareAsync;
        _commit = _handler.CommitAsync;
        _abort = _handler.AbortAsync;
        _getStatus = _handler.GetStatusAsync;
        _getDecision = _handler.GetDecisionAsync;
    }

    /// <inheritdoc />
    public override Task<SagaControlResponseBox> Prepare(SagaControlRequestBox request, ServerCallContext context)
        => HandleAsync(LatticeSagaGrpcMethods.PrepareMethodName, request, context, _prepare);

    /// <inheritdoc />
    public override Task<SagaControlResponseBox> Commit(SagaControlRequestBox request, ServerCallContext context)
        => HandleAsync(LatticeSagaGrpcMethods.CommitMethodName, request, context, _commit);

    /// <inheritdoc />
    public override Task<SagaControlResponseBox> Abort(SagaControlRequestBox request, ServerCallContext context)
        => HandleAsync(LatticeSagaGrpcMethods.AbortMethodName, request, context, _abort);

    /// <inheritdoc />
    public override Task<SagaControlResponseBox> GetStatus(SagaControlRequestBox request, ServerCallContext context)
        => HandleAsync(LatticeSagaGrpcMethods.GetStatusMethodName, request, context, _getStatus);

    /// <inheritdoc />
    /// <remarks>
    /// The caller is a participant asking the saga's coordinator, not a
    /// coordinator driving a participant, so the body's coordinator cluster is
    /// this cluster rather than the caller. The authorization input is still
    /// only the transport-stamped origin: it must be an authorized peer, and it
    /// overwrites <see cref="SagaControlRequest.RequesterClusterId"/>, which the
    /// coordinator checks against the saga's recorded participants. The answer
    /// is read-only.
    /// </remarks>
    public override Task<SagaControlResponseBox> GetDecision(SagaControlRequestBox request, ServerCallContext context)
        => HandleAsync(LatticeSagaGrpcMethods.GetDecisionMethodName, request, context, _getDecision, requesterQuery: true);

    private async Task<SagaControlResponseBox> HandleAsync(
        string operation,
        SagaControlRequestBox requestBox,
        ServerCallContext context,
        Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> handle,
        bool requesterQuery = false)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrWhiteSpace(request.SagaId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "SagaControlRequest.SagaId must be non-empty."));
        }

        if (string.IsNullOrWhiteSpace(request.TargetTree))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "SagaControlRequest.TargetTree must be non-empty."));
        }

        // Peer authorization gate. The imperative saga calls mutate
        // participant state, so the caller's origin cluster must be a
        // known/authorized peer before the handler runs.
        //
        // The authorization input is the transport-stamped origin header and
        // never the request body. A body field is chosen by the caller, so
        // authorizing on it authorizes the caller against a name the caller
        // picked: any party that clears the shared-secret interceptor could
        // name an authorized peer and drive saga state on a participant. The
        // interceptor's accepted-set match cannot compensate on its own,
        // because the set is flat and cluster-agnostic - "holds an accepted
        // secret" and "is cluster X" are unrelated facts. With
        // BindCredentialToOriginCluster on (the default) the interceptor also
        // binds the presented secret to the stamped origin, which is what makes
        // the header below an authenticated input rather than a self-assertion.
        //
        // Absent header is refused rather than falling back to the body:
        // GrpcChannelHardening stamps the header unconditionally on every
        // peer channel, so a conforming sender always carries it and only a
        // hand-rolled caller omits it. Mirrors the sibling binding in
        // LatticeReplicationGrpcService.EnsureOriginMatchesCaller - a gate
        // applied to one verb of a family and not its siblings is not a gate,
        // because the caller picks the verb.
        var origin = GrpcRequestHeaders.Read(context, LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader);
        if (string.IsNullOrWhiteSpace(origin))
        {
            _logger.LogWarning(
                "Saga control {Operation} rejected for saga {SagaId} - the call carries no stamped origin cluster.",
                operation, request.SagaId);
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "Saga control calls must carry the transport-stamped origin cluster header. "
                + "The coordinator cluster id declared in the request body is not an authorization input."));
        }

        // A participant's decision query names the coordinator it is asking - this
        // cluster - in the body, never itself, so the coordinator-attribution
        // check below does not apply to it. Its requester is the stamped origin,
        // never a body value (a caller-supplied one is overwritten).
        if (requesterQuery)
        {
            request = request with { RequesterClusterId = origin };
        }

        // A present body origin must agree with the stamped one, so a caller
        // cannot act on its own credential while attributing the saga to a
        // third cluster. The body value remains the handler's attribution.
        else if (!string.IsNullOrWhiteSpace(request.CoordinatorClusterId)
            && !string.Equals(origin, request.CoordinatorClusterId, StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "Saga control {Operation} rejected for saga {SagaId} - the request declares coordinator "
                + "'{Declared}' but the transport stamped origin '{Stamped}'.",
                operation, request.SagaId, request.CoordinatorClusterId, origin);
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "The coordinator cluster declared by the saga control call does not match the origin "
                + "stamped on the call; a peer may only drive sagas it coordinates."));
        }

        var authorized = await _authorizer.IsAuthorizedAsync(origin, context.CancellationToken).ConfigureAwait(false);
        if (!authorized)
        {
            _logger.LogWarning(
                "Saga control {Operation} rejected for saga {SagaId} - origin cluster '{Origin}' is not an authorized peer.",
                operation, request.SagaId, origin);
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "Saga control call originates from a cluster that is not an authorized replication peer. "
                + "Only clusters configured in the replication peer map may drive saga control RPCs."));
        }

        try
        {
            var response = await handle(request, context.CancellationToken).ConfigureAwait(false);
            return new SagaControlResponseBox { Value = response };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Saga control {Operation} failed for saga {SagaId} on tree {Tree}.",
                operation, request.SagaId, request.TargetTree);
            throw new RpcException(
                new Status(StatusCode.Internal,
                    $"Saga control '{operation}' failed for saga '{request.SagaId}'; "
                    + "see server logs for the underlying exception."),
                ex.Message);
        }
    }
}
