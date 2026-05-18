using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Abstract base class for the gRPC <c>GetMetadata</c> /
/// <c>RequestSnapshot</c> RPCs. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the two snapshot
/// transport routes. Mirrors the metadata/derived split
/// <see cref="LatticeReplicationGrpcServiceBase"/> uses for the live
/// push transport so the two services share a single registration
/// shape.
/// </summary>
[BindServiceMethod(typeof(LatticeRemoteSnapshotGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeRemoteSnapshotGrpcServiceBase
{
    /// <summary>
    /// Handles the unary <c>GetMetadata</c> RPC. Implemented in
    /// <see cref="LatticeRemoteSnapshotGrpcService"/>.
    /// </summary>
    public abstract Task<RemoteSnapshotMetadataBox> GetMetadata(
        RemoteSnapshotMetadataRequestBox request,
        ServerCallContext context);

    /// <summary>
    /// Handles the server-streaming <c>RequestSnapshot</c> RPC.
    /// Implemented in <see cref="LatticeRemoteSnapshotGrpcService"/>.
    /// </summary>
    public abstract Task RequestSnapshot(
        RemoteSnapshotMetadataRequestBox request,
        IServerStreamWriter<RemoteSnapshotStreamItemBox> responseStream,
        ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called
    /// once at startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual
    /// service instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeRemoteSnapshotGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeRemoteSnapshotGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeRemoteSnapshotGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc)} "
                + "ran and that "
                + $"{nameof(LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc)} "
                + "pre-resolved LatticeRemoteSnapshotGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(
                methods.GetMetadata,
                (UnaryServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox>?)null);
            binder.AddMethod(
                methods.RequestSnapshot,
                (ServerStreamingServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox>?)null);
            return;
        }

        binder.AddMethod(
            methods.GetMetadata,
            new UnaryServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox>(serviceImpl.GetMetadata));
        binder.AddMethod(
            methods.RequestSnapshot,
            new ServerStreamingServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox>(serviceImpl.RequestSnapshot));
    }
}

/// <summary>
/// Process-wide holder for the resolved
/// <see cref="LatticeRemoteSnapshotGrpcMethods"/> singleton. Populated
/// by
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc"/>;
/// consumed by the static
/// <see cref="LatticeRemoteSnapshotGrpcServiceBase.BindService"/>
/// callback that gRPC's reflection invokes at startup.
/// </summary>
/// <remarks>
/// Mirrors <see cref="LatticeReplicationGrpcMethodHolder"/> for the
/// live push transport - the same static-bridge pattern is the only
/// way to feed DI-resolved method singletons into the gRPC binder
/// callback, which is invoked through reflection and cannot accept
/// constructor arguments.
/// </remarks>
internal static class LatticeRemoteSnapshotGrpcMethodsHolder
{
    /// <summary>
    /// The current resolved
    /// <see cref="LatticeRemoteSnapshotGrpcMethods"/>, or
    /// <see langword="null"/> if registration has not yet occurred.
    /// </summary>
    public static LatticeRemoteSnapshotGrpcMethods? Current { get; set; }
}

/// <summary>
/// Server-side gRPC service that exposes the sender-side
/// <see cref="LatticeRemoteSnapshotService"/> to remote receivers.
/// Validates the receiver-supplied request, delegates to the local
/// handler, and either returns a captured
/// <see cref="RemoteSnapshotMetadata"/> response (for the unary
/// <c>GetMetadata</c> RPC) or pumps the resulting
/// <see cref="SnapshotEntry"/> async-enumerable into the gRPC
/// server-streaming response (for the <c>RequestSnapshot</c> RPC).
/// </summary>
internal sealed class LatticeRemoteSnapshotGrpcService : LatticeRemoteSnapshotGrpcServiceBase
{
    private readonly LatticeRemoteSnapshotService _service;
    private readonly ILogger<LatticeRemoteSnapshotGrpcService> _logger;

    /// <summary>
    /// Initialises the service with its dependencies. The
    /// <paramref name="methods"/> parameter is unused inside the
    /// service body but its presence on the constructor is
    /// load-bearing: it forces the DI container to resolve the
    /// <see cref="LatticeRemoteSnapshotGrpcMethods"/> singleton
    /// (whose factory populates
    /// <see cref="LatticeRemoteSnapshotGrpcMethodsHolder.Current"/>)
    /// before this service resolves, so the static
    /// <see cref="LatticeRemoteSnapshotGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeRemoteSnapshotGrpcService(
        LatticeRemoteSnapshotGrpcMethods methods,
        LatticeRemoteSnapshotService service,
        ILogger<LatticeRemoteSnapshotGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(service);
        ArgumentNullException.ThrowIfNull(logger);

        _service = service;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task<RemoteSnapshotMetadataBox> GetMetadata(
        RemoteSnapshotMetadataRequestBox requestBox,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrWhiteSpace(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "RemoteSnapshotMetadataRequest.TreeName must be non-empty."));
        }

        if (string.IsNullOrWhiteSpace(request.SourceClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "RemoteSnapshotMetadataRequest.SourceClusterId must be non-empty."));
        }

        try
        {
            var metadata = await _service.GetMetadataAsync(
                request.TreeName,
                request.SourceClusterId,
                request.FromAsOfHlc,
                context.CancellationToken).ConfigureAwait(false);

            return new RemoteSnapshotMetadataBox { Value = metadata };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "GetMetadata failed for tree {Tree} requested by receiver pinning sender {Source}.",
                request.TreeName, request.SourceClusterId);
            throw new RpcException(
                new Status(StatusCode.Internal,
                    $"Snapshot metadata capture failed on tree '{request.TreeName}'; "
                    + "see server logs for the underlying exception."),
                ex.Message);
        }
    }

    /// <inheritdoc />
    public override async Task RequestSnapshot(
        RemoteSnapshotMetadataRequestBox requestBox,
        IServerStreamWriter<RemoteSnapshotStreamItemBox> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrWhiteSpace(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "RemoteSnapshotMetadataRequest.TreeName must be non-empty."));
        }

        if (string.IsNullOrWhiteSpace(request.SourceClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "RemoteSnapshotMetadataRequest.SourceClusterId must be non-empty."));
        }

        try
        {
            await foreach (var entry in _service.RequestSnapshotAsync(
                request.TreeName,
                request.SourceClusterId,
                request.FromAsOfHlc,
                context.CancellationToken).ConfigureAwait(false))
            {
                await responseStream
                    .WriteAsync(new RemoteSnapshotStreamItemBox { Value = new RemoteSnapshotStreamItem { Entry = entry } })
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "RequestSnapshot stream failed for tree {Tree} requested by receiver pinning sender {Source}.",
                request.TreeName, request.SourceClusterId);
            throw new RpcException(
                new Status(StatusCode.Internal,
                    $"Snapshot stream failed on tree '{request.TreeName}'; "
                    + "see server logs for the underlying exception."),
                ex.Message);
        }
    }
}