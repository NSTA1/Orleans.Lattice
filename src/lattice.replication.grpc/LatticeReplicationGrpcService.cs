using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Abstract base class for the gRPC <c>Push</c> RPC. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the unary
/// <c>Push(ReplicationBatchEnvelopeBox) -&gt; ReplicationAckBox</c>
/// route.
/// </summary>
/// <remarks>
/// The base/derived split mirrors the pattern <c>Grpc.Tools</c>'s
/// codegen produces for a <c>.proto</c>-defined service: the base
/// class is the metadata-bearing type the binder discovers, and the
/// derived class is the concrete implementation resolved from DI per
/// request. <c>Grpc.AspNetCore</c> calls <see cref="BindService"/>
/// once at startup with a <see langword="null"/> service instance to
/// record method metadata, then resolves the actual instance from DI
/// at request time.
/// </remarks>
[BindServiceMethod(typeof(LatticeReplicationGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeReplicationGrpcServiceBase
{
    /// <summary>
    /// Handles a single push batch. Implemented in
    /// <see cref="LatticeReplicationGrpcService"/>.
    /// </summary>
    public abstract Task<ReplicationAckBox> Push(ReplicationBatchEnvelopeBox request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called
    /// once at startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual
    /// service instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeReplicationGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var method = LatticeReplicationGrpcMethodHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeReplicationGrpcMethodHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcServer)} "
                + "ran and that "
                + $"{nameof(LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpcService)} "
                + "pre-resolved LatticeReplicationGrpcMethod before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            // Startup metadata pass - register the method shape with a
            // null handler. Grpc.AspNetCore replaces the handler with
            // the real per-request invoker resolved through DI.
            binder.AddMethod(method.Push, (UnaryServerMethod<ReplicationBatchEnvelopeBox, ReplicationAckBox>?)null);
            return;
        }

        binder.AddMethod(method.Push, new UnaryServerMethod<ReplicationBatchEnvelopeBox, ReplicationAckBox>(serviceImpl.Push));
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeReplicationGrpcMethod"/>.
/// Populated by <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcServer"/>.
/// </summary>
/// <remarks>
/// gRPC's static <c>BindService</c> binding hook cannot accept DI
/// dependencies directly; this holder is the bridge from the DI graph
/// to the static binding callback. Setting it more than once is
/// allowed - subsequent registrations replace the prior method
/// instance, which matches the "last-host-wins" semantics most
/// integration test fixtures rely on when standing up an in-process
/// receiver.
/// </remarks>
internal static class LatticeReplicationGrpcMethodHolder
{
    /// <summary>
    /// The current resolved <see cref="LatticeReplicationGrpcMethod"/>,
    /// or <see langword="null"/> if registration has not yet occurred.
    /// </summary>
    public static LatticeReplicationGrpcMethod? Current { get; set; }
}

/// <summary>
/// Server-side gRPC service that receives <see cref="ReplicationBatchEnvelopeBox"/>
/// payloads pushed by remote <see cref="GrpcPushTransport"/> instances,
/// decodes the contained <see cref="ReplogEntry"/> records, and routes
/// each entry through the receiver-side <see cref="IReplicationApplier"/>
/// seam. Returns a single <see cref="ReplicationAck"/> per batch whose
/// <see cref="ReplicationAck.HighestAppliedHlc"/> is the maximum
/// high-water-mark advanced by any apply call.
/// </summary>
internal sealed class LatticeReplicationGrpcService : LatticeReplicationGrpcServiceBase
{
    private readonly IReplicationApplier _applier;
    private readonly ILatticeReplicationCursorRegistry _cursorRegistry;
    private readonly ILogger<LatticeReplicationGrpcService> _logger;

    /// <summary>
    /// Initialises the service with its dependencies. The
    /// <paramref name="method"/> parameter is unused inside the
    /// service body but its presence on the constructor is
    /// load-bearing: it forces the DI container to resolve the
    /// <see cref="LatticeReplicationGrpcMethod"/> singleton (whose
    /// factory populates
    /// <see cref="LatticeReplicationGrpcMethodHolder.Current"/>)
    /// before this service resolves, so the static
    /// <see cref="LatticeReplicationGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeReplicationGrpcService(
        LatticeReplicationGrpcMethod method,
        IReplicationApplier applier,
        ILatticeReplicationCursorRegistry cursorRegistry,
        ILogger<LatticeReplicationGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(applier);
        ArgumentNullException.ThrowIfNull(cursorRegistry);
        ArgumentNullException.ThrowIfNull(logger);

        _applier = applier;
        _cursorRegistry = cursorRegistry;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task<ReplicationAckBox> Push(ReplicationBatchEnvelopeBox requestBox, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestBox);
        ArgumentNullException.ThrowIfNull(context);

        var request = requestBox.Value;

        if (string.IsNullOrEmpty(request.TreeName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ReplicationBatchEnvelope.TreeName must be non-empty."));
        }

        if (string.IsNullOrEmpty(request.OriginClusterId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "ReplicationBatchEnvelope.OriginClusterId must be non-empty."));
        }

        var entries = request.Entries ?? Array.Empty<ReplogEntry>();

        ApplyResult result;
        try
        {
            result = await _applier.ApplyBatchAsync(entries, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Best-effort context for the failure: surface the tree id
            // and batch size in the structured log so operators can
            // correlate the gRPC exception to a specific inbound push.
            // Per-entry detail is owned by the applier (it logs / parks
            // / records-metrics inside the batch loop), so we do not
            // re-inflate it here.
            _logger.LogError(ex,
                "Replication apply failed for tree {Tree} on a {EntryCount}-entry batch from origin {Origin}.",
                request.TreeName, entries.Count, request.OriginClusterId);

            throw new RpcException(
                new Status(StatusCode.Internal,
                    $"Replication apply failed on tree '{request.TreeName}' "
                    + $"({entries.Count} entries from origin '{request.OriginClusterId}'); "
                    + "see server logs for the underlying exception."),
                ex.Message);
        }

        // Stamp the receiver-side blocked-floor pin (the lowest
        // staged HLC across every partially-buffered atomic batch on
        // this tree) onto the ack so the producer-side WAL GC AND-s
        // a strict-less entry.Timestamp < blockedFloor clause into
        // its trim predicate. Failure is swallowed: the receiver
        // already applied / buffered the batch, the WAL still holds
        // the canonical mutation, and a subsequent batch's ack will
        // re-stamp the pin. Surfacing a registry-side exception out
        // of a successful apply path would convert a diagnostic
        // outage into a transport failure, which is the wrong
        // trade-off.
        HybridLogicalClock? blockedAtHlc = null;
        try
        {
            blockedAtHlc = await _cursorRegistry
                .GetBlockedFloorAsync(request.TreeName, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Reading receiver-side blocked-floor pin failed for tree {Tree}; ack will omit the slot.",
                request.TreeName);
        }

        return new ReplicationAckBox
        {
            Value = new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = result.HighWaterMark,
                BlockedAtHlc = blockedAtHlc,
            },
        };
    }
}

