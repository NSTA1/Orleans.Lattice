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

    private async Task<SagaControlResponseBox> HandleAsync(
        string operation,
        SagaControlRequestBox requestBox,
        ServerCallContext context,
        Func<SagaControlRequest, CancellationToken, Task<SagaControlResponse>> handle)
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
        // known/authorized peer before the handler runs. The transport
        // stamps the origin header; fall back to the request's
        // coordinator cluster id when the header is absent.
        var origin = ReadHeader(context, LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader);
        if (string.IsNullOrWhiteSpace(origin))
        {
            origin = request.CoordinatorClusterId;
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

    private static string? ReadHeader(ServerCallContext context, string key)
    {
        foreach (var entry in context.RequestHeaders)
        {
            if (string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return entry.Value;
            }
        }
        return null;
    }
}
