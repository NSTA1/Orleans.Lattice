using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Abstract base for the read-only state-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the five unary RPCs
/// (<c>ListTrees</c>, <c>ListViews</c>, <c>GetTreeStructure</c>,
/// <c>ScanEntries</c>, <c>GetEntry</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c>
/// produces for a <c>.proto</c> service: the base type bears the binding
/// metadata the binder discovers, and the derived type is the concrete
/// implementation resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="BindService"/> once at startup with a <see langword="null"/>
/// instance to record method metadata, then resolves the actual instance per
/// request.
/// </remarks>
[BindServiceMethod(typeof(LatticeStateGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeStateGrpcServiceBase
{
    /// <summary>Enumerates the registered trees. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TreeCatalogPage> ListTrees(CatalogRequest request, ServerCallContext context);

    /// <summary>Enumerates the materialised views. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<ViewCatalogPage> ListViews(CatalogRequest request, ServerCallContext context);

    /// <summary>Returns the structural node graph of a tree. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<StructureResponse> GetTreeStructure(StructureRequest request, ServerCallContext context);

    /// <summary>Scans a key-ordered page of entries. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryScanResponse> ScanEntries(EntryScanRequest request, ServerCallContext context);

    /// <summary>Returns the full record for a single key. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryGetResponse> GetEntry(EntryGetRequest request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual service
    /// instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeStateGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeStateGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeStateGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeStateApiGrpcServiceCollectionExtensions.AddLatticeStateApiGrpc)} ran and that "
                + $"{nameof(LatticeStateApiGrpcServiceCollectionExtensions.MapLatticeStateApiGrpc)} pre-resolved "
                + "LatticeStateGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.ListTrees, (UnaryServerMethod<CatalogRequest, TreeCatalogPage>?)null);
            binder.AddMethod(methods.ListViews, (UnaryServerMethod<CatalogRequest, ViewCatalogPage>?)null);
            binder.AddMethod(methods.GetTreeStructure, (UnaryServerMethod<StructureRequest, StructureResponse>?)null);
            binder.AddMethod(methods.ScanEntries, (UnaryServerMethod<EntryScanRequest, EntryScanResponse>?)null);
            binder.AddMethod(methods.GetEntry, (UnaryServerMethod<EntryGetRequest, EntryGetResponse>?)null);
            return;
        }

        binder.AddMethod(methods.ListTrees, new UnaryServerMethod<CatalogRequest, TreeCatalogPage>(serviceImpl.ListTrees));
        binder.AddMethod(methods.ListViews, new UnaryServerMethod<CatalogRequest, ViewCatalogPage>(serviceImpl.ListViews));
        binder.AddMethod(methods.GetTreeStructure, new UnaryServerMethod<StructureRequest, StructureResponse>(serviceImpl.GetTreeStructure));
        binder.AddMethod(methods.ScanEntries, new UnaryServerMethod<EntryScanRequest, EntryScanResponse>(serviceImpl.ScanEntries));
        binder.AddMethod(methods.GetEntry, new UnaryServerMethod<EntryGetRequest, EntryGetResponse>(serviceImpl.GetEntry));
    }
}

/// <summary>
/// Server-side gRPC service for the read-only state API. Adapts each unary
/// RPC onto the transport-agnostic <see cref="ILatticeStateQuery"/> facade,
/// mapping the facade's plain result records onto the serializable wire
/// responses and translating typed not-founds and argument failures onto gRPC
/// status codes.
/// </summary>
internal sealed class LatticeStateGrpcService : LatticeStateGrpcServiceBase
{
    private readonly ILatticeStateQuery _query;
    private readonly ILogger<LatticeStateGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeStateGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeStateGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="BindService"/> hook always observes a
    /// populated holder.
    /// </summary>
    public LatticeStateGrpcService(
        LatticeStateGrpcMethods methods,
        ILatticeStateQuery query,
        ILogger<LatticeStateGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(logger);

        _query = query;
        _logger = logger;
    }

    /// <inheritdoc />
    public override Task<TreeCatalogPage> ListTrees(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListTreesAsync(req, ct));

    /// <inheritdoc />
    public override Task<ViewCatalogPage> ListViews(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListViewsAsync(req, ct));

    /// <inheritdoc />
    public override Task<StructureResponse> GetTreeStructure(StructureRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            var result = await q.GetTreeStructureAsync(req, ct).ConfigureAwait(false);
            if (result.Status == StateQueryStatus.TreeNotFound)
            {
                throw NotFound($"Tree '{result.TreeId}' was not found.");
            }

            return new StructureResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Roots = result.Roots,
                Truncated = result.Truncated,
            };
        });

    /// <inheritdoc />
    public override Task<EntryScanResponse> ScanEntries(EntryScanRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            var result = await q.ScanEntriesAsync(req, ct).ConfigureAwait(false);
            if (result.Status == StateQueryStatus.TreeNotFound)
            {
                throw NotFound($"Tree '{result.TreeId}' was not found.");
            }

            return new EntryScanResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Entries = result.Entries,
                ContinuationToken = result.ContinuationToken,
            };
        });

    /// <inheritdoc />
    public override Task<EntryGetResponse> GetEntry(EntryGetRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            var result = await q.GetEntryAsync(req.TreeId, req.Key, ct).ConfigureAwait(false);
            if (result.Status == StateQueryStatus.TreeNotFound)
            {
                throw NotFound($"Tree '{result.TreeId}' was not found.");
            }

            if (result.Status == StateQueryStatus.KeyNotFound)
            {
                throw NotFound($"Key '{result.Key}' was not found in tree '{result.TreeId}'.");
            }

            return new EntryGetResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Key = result.Key,
                Entry = result.Entry,
            };
        });

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeStateQuery, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        try
        {
            return await handler(_query, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The state-API request was cancelled."));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.State: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The state-API request failed."));
        }
    }

    private static RpcException NotFound(string message)
        => new(new Status(StatusCode.NotFound, message));
}
