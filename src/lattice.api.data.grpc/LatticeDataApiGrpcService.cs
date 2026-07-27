using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Abstract base for the write-capable data-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the nine unary RPCs (<c>Set</c>,
/// <c>Delete</c>, <c>SetManyAtomic</c>, <c>SetManyAtomicCrossTree</c>,
/// <c>Get</c>, <c>ReadRange</c>, <c>SetMany</c>, <c>CrdtWrite</c>,
/// <c>CrdtRead</c>).
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
[BindServiceMethod(typeof(LatticeDataApiGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeDataApiGrpcServiceBase
{
    /// <summary>Writes a value at a key. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataSetResponse> Set(DataSetRequest request, ServerCallContext context);

    /// <summary>Deletes a key. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataDeleteResponse> Delete(DataDeleteRequest request, ServerCallContext context);

    /// <summary>Commits a single-tree atomic batch. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataAtomicResponse> SetManyAtomic(DataAtomicRequest request, ServerCallContext context);

    /// <summary>Commits a cross-tree atomic batch. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataCrossTreeResponse> SetManyAtomicCrossTree(DataCrossTreeRequest request, ServerCallContext context);

    /// <summary>Reads a value at a key. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataReadResult> Get(DataGetRequest request, ServerCallContext context);

    /// <summary>Reads one page of a bounded range. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataRangePage> ReadRange(DataRangeRequest request, ServerCallContext context);

    /// <summary>Commits a non-atomic bulk write. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<DataSetManyResponse> SetMany(DataSetManyRequest request, ServerCallContext context);

    /// <summary>Applies a typed CRDT write. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<CrdtWriteResponse> CrdtWrite(CrdtWriteRequest request, ServerCallContext context);

    /// <summary>Reads a typed CRDT logical value. Implemented in <see cref="LatticeDataApiGrpcService"/>.</summary>
    public abstract Task<CrdtReadResponse> CrdtRead(CrdtReadRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to <see langword="null"/>
    /// to record method metadata; the actual service instance is resolved per
    /// request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeDataApiGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeDataApiGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeDataApiGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeDataApiGrpcServiceCollectionExtensions.AddLatticeDataApiGrpc)} ran and that "
                + $"{nameof(LatticeDataApiGrpcServiceCollectionExtensions.MapLatticeDataApiGrpc)} pre-resolved "
                + "LatticeDataApiGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.Set, (UnaryServerMethod<DataSetRequest, DataSetResponse>?)null);
            binder.AddMethod(methods.Delete, (UnaryServerMethod<DataDeleteRequest, DataDeleteResponse>?)null);
            binder.AddMethod(methods.SetManyAtomic, (UnaryServerMethod<DataAtomicRequest, DataAtomicResponse>?)null);
            binder.AddMethod(methods.SetManyAtomicCrossTree, (UnaryServerMethod<DataCrossTreeRequest, DataCrossTreeResponse>?)null);
            binder.AddMethod(methods.Get, (UnaryServerMethod<DataGetRequest, DataReadResult>?)null);
            binder.AddMethod(methods.ReadRange, (UnaryServerMethod<DataRangeRequest, DataRangePage>?)null);
            binder.AddMethod(methods.SetMany, (UnaryServerMethod<DataSetManyRequest, DataSetManyResponse>?)null);
            binder.AddMethod(methods.CrdtWrite, (UnaryServerMethod<CrdtWriteRequest, CrdtWriteResponse>?)null);
            binder.AddMethod(methods.CrdtRead, (UnaryServerMethod<CrdtReadRequest, CrdtReadResponse>?)null);
            return;
        }

        binder.AddMethod(methods.Set, new UnaryServerMethod<DataSetRequest, DataSetResponse>(serviceImpl.Set));
        binder.AddMethod(methods.Delete, new UnaryServerMethod<DataDeleteRequest, DataDeleteResponse>(serviceImpl.Delete));
        binder.AddMethod(methods.SetManyAtomic, new UnaryServerMethod<DataAtomicRequest, DataAtomicResponse>(serviceImpl.SetManyAtomic));
        binder.AddMethod(methods.SetManyAtomicCrossTree, new UnaryServerMethod<DataCrossTreeRequest, DataCrossTreeResponse>(serviceImpl.SetManyAtomicCrossTree));
        binder.AddMethod(methods.Get, new UnaryServerMethod<DataGetRequest, DataReadResult>(serviceImpl.Get));
        binder.AddMethod(methods.ReadRange, new UnaryServerMethod<DataRangeRequest, DataRangePage>(serviceImpl.ReadRange));
        binder.AddMethod(methods.SetMany, new UnaryServerMethod<DataSetManyRequest, DataSetManyResponse>(serviceImpl.SetMany));
        binder.AddMethod(methods.CrdtWrite, new UnaryServerMethod<CrdtWriteRequest, CrdtWriteResponse>(serviceImpl.CrdtWrite));
        binder.AddMethod(methods.CrdtRead, new UnaryServerMethod<CrdtReadRequest, CrdtReadResponse>(serviceImpl.CrdtRead));
    }
}

/// <summary>
/// Server-side implementation of the data-API gRPC service. Adapts each unary
/// RPC onto the transport-agnostic <see cref="ILatticeDataApi"/> facade, stamps
/// the caller identity onto the ambient credential context so the gated
/// <see cref="ILattice"/> surface enforces per-tree / per-key authorization, and
/// maps a gate denial onto <see cref="StatusCode.PermissionDenied"/> carrying
/// only the non-sensitive tree / operation / subject / reason fields (never a
/// value) as response trailers.
/// </summary>
internal sealed class LatticeDataApiGrpcService : LatticeDataApiGrpcServiceBase
{
    /// <summary>Trailer key carrying the denied tree id.</summary>
    internal const string DeniedTreeTrailer = "lattice-denied-tree";

    /// <summary>Trailer key carrying the denied operation.</summary>
    internal const string DeniedOperationTrailer = "lattice-denied-operation";

    /// <summary>Trailer key carrying the denied caller's subject id.</summary>
    internal const string DeniedSubjectTrailer = "lattice-denied-subject";

    /// <summary>Trailer key carrying the gate's denial reason.</summary>
    internal const string DeniedReasonTrailer = "lattice-denied-reason";

    private readonly ILatticeDataApi _dataApi;
    private readonly ILatticeDataApiCredentialBridge _credentialBridge;
    private readonly ILogger<LatticeDataApiGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeDataApiGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeDataApiGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="LatticeDataApiGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeDataApiGrpcService(
        LatticeDataApiGrpcMethods methods,
        ILatticeDataApi dataApi,
        ILatticeDataApiCredentialBridge credentialBridge,
        ILogger<LatticeDataApiGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(dataApi);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(logger);

        _dataApi = dataApi;
        _credentialBridge = credentialBridge;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the gated data-plane surface resolves the caller's subject and
    /// authorizes the mutation or read. Returns <see langword="null"/> (no scope)
    /// when the call carries no credential, leaving the caller anonymous -
    /// fail-closed on every mutation and read. This is orthogonal to, and runs
    /// after, the transport-level <see cref="ILatticeDataApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<DataSetResponse> Set(DataSetRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            await api.SetAsync(req.TreeId, req.Key, req.Value, ct).ConfigureAwait(false);
            return new DataSetResponse();
        });

    /// <inheritdoc />
    public override Task<DataDeleteResponse> Delete(DataDeleteRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            var removed = await api.DeleteAsync(req.TreeId, req.Key, ct).ConfigureAwait(false);
            return new DataDeleteResponse { Removed = removed };
        });

    /// <inheritdoc />
    public override Task<DataAtomicResponse> SetManyAtomic(DataAtomicRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            await api.SetManyAtomicAsync(req.TreeId, req.Batch, req.OperationId, ct).ConfigureAwait(false);
            return new DataAtomicResponse();
        });

    /// <inheritdoc />
    public override Task<DataCrossTreeResponse> SetManyAtomicCrossTree(DataCrossTreeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            var outcome = await api.SetManyAtomicCrossTreeAsync(req.Batches, req.OperationId, ct).ConfigureAwait(false);
            return new DataCrossTreeResponse { Outcome = outcome };
        });

    /// <inheritdoc />
    public override Task<DataReadResult> Get(DataGetRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (api, req, ct) => api.GetAsync(req.TreeId, req.Key, ct));

    /// <inheritdoc />
    public override Task<DataRangePage> ReadRange(DataRangeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (api, req, ct) => api.ReadRangeAsync(req, ct));

    /// <inheritdoc />
    public override Task<DataSetManyResponse> SetMany(DataSetManyRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            await api.SetManyAsync(req.TreeId, req.Upserts, ct).ConfigureAwait(false);
            return new DataSetManyResponse();
        });

    /// <inheritdoc />
    public override Task<CrdtWriteResponse> CrdtWrite(CrdtWriteRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (api, req, ct) =>
        {
            await DispatchCrdtWriteAsync(api, req, ct).ConfigureAwait(false);
            return new CrdtWriteResponse();
        });

    /// <inheritdoc />
    public override Task<CrdtReadResponse> CrdtRead(CrdtReadRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (api, req, ct) => DispatchCrdtReadAsync(api, req, ct));

    private static Task DispatchCrdtWriteAsync(ILatticeDataApi api, CrdtWriteRequest req, CancellationToken ct)
        => req.Op switch
        {
            CrdtWriteOp.CounterIncrement => api.CounterIncrementAsync(req.TreeId, req.Key, req.ReplicaId, req.Amount, ct),
            CrdtWriteOp.CounterDecrement => api.CounterDecrementAsync(req.TreeId, req.Key, req.ReplicaId, req.Amount, ct),
            CrdtWriteOp.SetAdd => api.SetAddAsync(req.TreeId, req.Key, req.Element, req.ReplicaId, ct),
            CrdtWriteOp.SetRemove => api.SetRemoveAsync(req.TreeId, req.Key, req.Element, ct),
            CrdtWriteOp.OrFlagEnable => api.OrFlagEnableAsync(req.TreeId, req.Key, req.ReplicaId, ct),
            CrdtWriteOp.OrFlagDisable => api.OrFlagDisableAsync(req.TreeId, req.Key, ct),
            CrdtWriteOp.RwFlagEnable => api.RwFlagEnableAsync(req.TreeId, req.Key, req.ReplicaId, ct),
            CrdtWriteOp.RwFlagDisable => api.RwFlagDisableAsync(req.TreeId, req.Key, req.ReplicaId, ct),
            CrdtWriteOp.VersionVectorTick => api.VersionVectorTickAsync(req.TreeId, req.Key, req.ReplicaId, ct),
            CrdtWriteOp.RegisterSet => api.RegisterSetAsync(req.TreeId, req.Key, req.ReplicaId, req.Element, ct),
            CrdtWriteOp.SequenceInsertAt => api.SequenceInsertAtAsync(req.TreeId, req.Key, req.Index, req.ReplicaId, req.Element, ct),
            CrdtWriteOp.SequenceRemoveAt => api.SequenceRemoveAtAsync(req.TreeId, req.Key, req.Index, ct),
            CrdtWriteOp.MapSet => api.MapSetAsync(req.TreeId, req.Key, req.Field, req.ReplicaId, req.Element, ct),
            CrdtWriteOp.MapRemove => api.MapRemoveAsync(req.TreeId, req.Key, req.Field, ct),
            _ => throw new ArgumentException($"Unknown CRDT write op '{req.Op}'.", nameof(req)),
        };

    private static async Task<CrdtReadResponse> DispatchCrdtReadAsync(ILatticeDataApi api, CrdtReadRequest req, CancellationToken ct)
    {
        switch (req.Kind)
        {
            case CrdtKind.PnCounter:
                return new CrdtReadResponse { CounterValue = await api.CounterGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false) };
            case CrdtKind.OrSet:
                return new CrdtReadResponse { Elements = ToList(await api.SetGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false)) };
            case CrdtKind.OrFlag:
                return new CrdtReadResponse { FlagValue = await api.OrFlagGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false) };
            case CrdtKind.RwFlag:
                return new CrdtReadResponse { FlagValue = await api.RwFlagGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false) };
            case CrdtKind.MvRegister:
                return new CrdtReadResponse { Elements = ToList(await api.RegisterGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false)) };
            case CrdtKind.Sequence:
                return new CrdtReadResponse { Elements = ToList(await api.SequenceGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false)) };
            case CrdtKind.VersionVector:
            {
                var vector = await api.VersionVectorGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false);
                var entries = new List<CrdtVectorEntry>(vector.Count);
                foreach (var (replicaId, clock) in vector)
                {
                    entries.Add(new CrdtVectorEntry { ReplicaId = replicaId, Clock = clock });
                }

                return new CrdtReadResponse { Vector = entries };
            }

            case CrdtKind.OrMap:
            {
                var map = await api.MapGetAsync(req.TreeId, req.Key, ct).ConfigureAwait(false);
                var fields = new List<CrdtMapField>(map.Count);
                foreach (var (field, values) in map)
                {
                    fields.Add(new CrdtMapField { Field = field, Values = ToList(values) });
                }

                return new CrdtReadResponse { Map = fields };
            }

            default:
                throw new ArgumentException($"Unknown CRDT read kind '{req.Kind}'.", nameof(req));
        }
    }

    private static List<byte[]> ToList(IReadOnlyList<byte[]> values)
        => values as List<byte[]> ?? [.. values];

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeDataApi, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_dataApi, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            // A gate denial is expected control flow on a write-capable surface.
            // Map to PermissionDenied carrying only the non-sensitive tree /
            // operation / subject / reason fields as trailers - never a value.
            throw ToPermissionDenied(ex);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The data-API request was cancelled."));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (LatticeCrdtShapeNotRegisteredException ex)
        {
            // A typed OR-Map verb targeted a tree whose host never registered the
            // map shape. This is a deterministic host-configuration precondition,
            // not a server fault: map to FailedPrecondition carrying the
            // self-contained remediation message (register via AddOrMapShape) so
            // the caller is not misdirected to the cluster logs. Placed with the
            // other typed InvalidOperationException-derived arms, ahead of the
            // generic server-fault catch that would otherwise mask it as Internal.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (LatticeIdempotencyKeyMismatchException ex)
        {
            // Reusing a caller-supplied operationId with a different key (or tree)
            // set is a client-side misuse of the idempotency key, not a server
            // fault. Map to FailedPrecondition carrying the self-contained
            // caller-facing message so no cluster-log spelunking is implied; the
            // guard already fired with nothing partially applied.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (LatticeReplicationModeMismatchException ex)
        {
            // A CRDT (or plain LWW) write used a shape that differs from the
            // single mode the replicated tree is declared with. This is a
            // deterministic caller/configuration precondition - the write shape
            // is wrong for the tree - not a server fault. Map to FailedPrecondition
            // carrying the self-contained caller-facing message, ahead of the
            // generic server-fault catch that would otherwise mask it as Internal.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (LatticeSaturatedException)
        {
            // The tree is WAL-saturated and shed the operation. Map to the
            // canonical "busy, retry later" code so the client can back off.
            throw new RpcException(new Status(
                StatusCode.ResourceExhausted,
                "The requested tree is busy (storage back-pressure) and the operation was refused. Retry after a short backoff."));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Data: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The data-API request failed."));
        }
    }

    private static RpcException ToPermissionDenied(LatticeAuthorizationDeniedException ex)
    {
        var trailers = new global::Grpc.Core.Metadata
        {
            { DeniedTreeTrailer, ex.TreeId },
            { DeniedOperationTrailer, ex.Operation.ToString() },
            { DeniedSubjectTrailer, ex.SubjectId },
            { DeniedReasonTrailer, ex.Reason },
        };

        return new RpcException(
            new Status(StatusCode.PermissionDenied, ex.Message),
            trailers);
    }
}
