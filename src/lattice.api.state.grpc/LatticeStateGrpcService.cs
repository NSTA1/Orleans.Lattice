using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Abstract base for the read-only state-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c>
/// reflects against to discover and register the five unary RPCs
/// (<c>ListTrees</c>, <c>ListViews</c>, <c>GetTreeStructure</c>,
/// <c>ScanEntries</c>, <c>GetEntry</c>) and the server-streaming
/// <c>ObserveChanges</c> subscription RPC.
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c>
/// produces for a <c>.proto</c> service: the base type bears the binding
/// metadata the binder discovers, and the derived type is the concrete
/// implementation resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeStateGrpcServiceBase.BindService"/> once at startup with a <see langword="null"/>
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

    /// <summary>Enumerates the tag-index membership trees. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TagIndexCatalogPage> ListTagIndexes(CatalogRequest request, ServerCallContext context);

    /// <summary>Enumerates the distinct tag values of one tag index. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TagValueCatalogPage> ListTagValues(CatalogRequest request, ServerCallContext context);

    /// <summary>Enumerates the subject trees a tag index covers. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<CoveredTreeCatalogPage> ListCoveredTrees(CatalogRequest request, ServerCallContext context);

    /// <summary>Enumerates a tag index's distinct tags across every covered tree. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TagValueCatalogPage> ListIndexTags(CatalogRequest request, ServerCallContext context);

    /// <summary>Enumerates the live members of a tag across a tag index. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TagMemberScanPage> ScanTagMembers(TagMemberScanRequest request, ServerCallContext context);

    /// <summary>Returns the structural node graph of a tree. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<StructureResponse> GetTreeStructure(StructureRequest request, ServerCallContext context);

    /// <summary>Scans a key-ordered page of entries. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryScanResponse> ScanEntries(EntryScanRequest request, ServerCallContext context);

    /// <summary>Returns the full record for a single key. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryGetResponse> GetEntry(EntryGetRequest request, ServerCallContext context);

    /// <summary>Returns a page of a single key's change-history timeline. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryHistoryResponse> GetEntryHistory(EntryHistoryRequest request, ServerCallContext context);

    /// <summary>Releases a snapshot scan cursor named by a continuation token. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<EntryScanCancelResponse> CancelScan(EntryScanCancelRequest request, ServerCallContext context);

    /// <summary>Streams change notifications for a tree until the call is cancelled. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task ObserveChanges(
        StateObserveRequest request,
        IServerStreamWriter<StateChangeNotification> responseStream,
        ServerCallContext context);

    /// <summary>Streams live metric snapshots until the call is cancelled. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task ObserveMetrics(
        TreeMetricsRequest request,
        IServerStreamWriter<TreeMetricsSnapshot> responseStream,
        ServerCallContext context);

    /// <summary>Returns a single live metrics snapshot. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<TreeMetricsSnapshot> GetMetricsSnapshot(TreeMetricsRequest request, ServerCallContext context);

    /// <summary>Returns identity and metadata for the connected cluster. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<ClusterInfo> GetClusterInfo(ClusterInfoRequest request, ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeStateGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>Counts a tree's strict-mode dead-letter entries. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<DeadLetterCountResponse> GetDeadLetterCount(DeadLetterCountRequest request, ServerCallContext context);

    /// <summary>Lists a tree's strict-mode dead-letter queue as a paged read. Implemented in <see cref="LatticeStateGrpcService"/>.</summary>
    public abstract Task<DeadLetterQueuePage> ListDeadLetters(DeadLetterQueueRequest request, ServerCallContext context);

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
            binder.AddMethod(methods.ListTagIndexes, (UnaryServerMethod<CatalogRequest, TagIndexCatalogPage>?)null);
            binder.AddMethod(methods.ListTagValues, (UnaryServerMethod<CatalogRequest, TagValueCatalogPage>?)null);
            binder.AddMethod(methods.ListCoveredTrees, (UnaryServerMethod<CatalogRequest, CoveredTreeCatalogPage>?)null);
            binder.AddMethod(methods.ListIndexTags, (UnaryServerMethod<CatalogRequest, TagValueCatalogPage>?)null);
            binder.AddMethod(methods.ScanTagMembers, (UnaryServerMethod<TagMemberScanRequest, TagMemberScanPage>?)null);
            binder.AddMethod(methods.GetTreeStructure, (UnaryServerMethod<StructureRequest, StructureResponse>?)null);
            binder.AddMethod(methods.ScanEntries, (UnaryServerMethod<EntryScanRequest, EntryScanResponse>?)null);
            binder.AddMethod(methods.GetEntry, (UnaryServerMethod<EntryGetRequest, EntryGetResponse>?)null);
            binder.AddMethod(methods.GetEntryHistory, (UnaryServerMethod<EntryHistoryRequest, EntryHistoryResponse>?)null);
            binder.AddMethod(methods.CancelScan, (UnaryServerMethod<EntryScanCancelRequest, EntryScanCancelResponse>?)null);
            binder.AddMethod(methods.ObserveChanges, (ServerStreamingServerMethod<StateObserveRequest, StateChangeNotification>?)null);
            binder.AddMethod(methods.ObserveMetrics, (ServerStreamingServerMethod<TreeMetricsRequest, TreeMetricsSnapshot>?)null);
            binder.AddMethod(methods.GetMetricsSnapshot, (UnaryServerMethod<TreeMetricsRequest, TreeMetricsSnapshot>?)null);
            binder.AddMethod(methods.GetClusterInfo, (UnaryServerMethod<ClusterInfoRequest, ClusterInfo>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            binder.AddMethod(methods.GetDeadLetterCount, (UnaryServerMethod<DeadLetterCountRequest, DeadLetterCountResponse>?)null);
            binder.AddMethod(methods.ListDeadLetters, (UnaryServerMethod<DeadLetterQueueRequest, DeadLetterQueuePage>?)null);
            return;
        }

        binder.AddMethod(methods.ListTrees, new UnaryServerMethod<CatalogRequest, TreeCatalogPage>(serviceImpl.ListTrees));
        binder.AddMethod(methods.ListViews, new UnaryServerMethod<CatalogRequest, ViewCatalogPage>(serviceImpl.ListViews));
        binder.AddMethod(methods.ListTagIndexes, new UnaryServerMethod<CatalogRequest, TagIndexCatalogPage>(serviceImpl.ListTagIndexes));
        binder.AddMethod(methods.ListTagValues, new UnaryServerMethod<CatalogRequest, TagValueCatalogPage>(serviceImpl.ListTagValues));
        binder.AddMethod(methods.ListCoveredTrees, new UnaryServerMethod<CatalogRequest, CoveredTreeCatalogPage>(serviceImpl.ListCoveredTrees));
        binder.AddMethod(methods.ListIndexTags, new UnaryServerMethod<CatalogRequest, TagValueCatalogPage>(serviceImpl.ListIndexTags));
        binder.AddMethod(methods.ScanTagMembers, new UnaryServerMethod<TagMemberScanRequest, TagMemberScanPage>(serviceImpl.ScanTagMembers));
        binder.AddMethod(methods.GetTreeStructure, new UnaryServerMethod<StructureRequest, StructureResponse>(serviceImpl.GetTreeStructure));
        binder.AddMethod(methods.ScanEntries, new UnaryServerMethod<EntryScanRequest, EntryScanResponse>(serviceImpl.ScanEntries));
        binder.AddMethod(methods.GetEntry, new UnaryServerMethod<EntryGetRequest, EntryGetResponse>(serviceImpl.GetEntry));
        binder.AddMethod(methods.GetEntryHistory, new UnaryServerMethod<EntryHistoryRequest, EntryHistoryResponse>(serviceImpl.GetEntryHistory));
        binder.AddMethod(methods.CancelScan, new UnaryServerMethod<EntryScanCancelRequest, EntryScanCancelResponse>(serviceImpl.CancelScan));
        binder.AddMethod(methods.ObserveChanges, new ServerStreamingServerMethod<StateObserveRequest, StateChangeNotification>(serviceImpl.ObserveChanges));
        binder.AddMethod(methods.ObserveMetrics, new ServerStreamingServerMethod<TreeMetricsRequest, TreeMetricsSnapshot>(serviceImpl.ObserveMetrics));
        binder.AddMethod(methods.GetMetricsSnapshot, new UnaryServerMethod<TreeMetricsRequest, TreeMetricsSnapshot>(serviceImpl.GetMetricsSnapshot));
        binder.AddMethod(methods.GetClusterInfo, new UnaryServerMethod<ClusterInfoRequest, ClusterInfo>(serviceImpl.GetClusterInfo));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
        binder.AddMethod(methods.GetDeadLetterCount, new UnaryServerMethod<DeadLetterCountRequest, DeadLetterCountResponse>(serviceImpl.GetDeadLetterCount));
        binder.AddMethod(methods.ListDeadLetters, new UnaryServerMethod<DeadLetterQueueRequest, DeadLetterQueuePage>(serviceImpl.ListDeadLetters));
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
    private readonly ILatticeStateObserver _observer;
    private readonly ILatticeStateMetricsObserver _metricsObserver;
    private readonly ILatticeStateApiCredentialBridge _credentialBridge;
    private readonly ILatticeStateApiAuthSchemeSource _authSchemeSource;
    private readonly IOptions<LatticeStateApiGrpcOptions> _options;
    private readonly ILogger<LatticeStateGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeStateGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeStateGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="LatticeStateGrpcServiceBase.BindService"/> hook always observes a
    /// populated holder.
    /// </summary>
    public LatticeStateGrpcService(
        LatticeStateGrpcMethods methods,
        ILatticeStateQuery query,
        ILatticeStateObserver observer,
        ILatticeStateMetricsObserver metricsObserver,
        ILatticeStateApiCredentialBridge credentialBridge,
        ILatticeStateApiAuthSchemeSource authSchemeSource,
        IOptions<LatticeStateApiGrpcOptions> options,
        ILogger<LatticeStateGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(observer);
        ArgumentNullException.ThrowIfNull(metricsObserver);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _query = query;
        _observer = observer;
        _metricsObserver = metricsObserver;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the gated data-plane surface resolves the caller's subject and
    /// filters the read. Returns <see langword="null"/> (no scope) when the call
    /// carries no credential, leaving the caller anonymous - fail-closed when
    /// auth-backed visibility is active. This is orthogonal to, and runs after,
    /// the transport-level <see cref="ILatticeStateApiAuthorizer"/> gate.
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
    public override Task<TreeCatalogPage> ListTrees(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListTreesAsync(req, ct));

    /// <inheritdoc />
    public override Task<ViewCatalogPage> ListViews(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListViewsAsync(req, ct));

    /// <inheritdoc />
    public override Task<TagIndexCatalogPage> ListTagIndexes(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListTagIndexesAsync(req, ct));

    public override Task<TagValueCatalogPage> ListTagValues(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListTagValuesAsync(req, ct));

    /// <inheritdoc />
    public override Task<CoveredTreeCatalogPage> ListCoveredTrees(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListCoveredTreesAsync(req, ct));

    /// <inheritdoc />
    public override Task<TagValueCatalogPage> ListIndexTags(CatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ListIndexTagsAsync(req, ct));

    /// <inheritdoc />
    public override Task<TagMemberScanPage> ScanTagMembers(TagMemberScanRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (q, req, ct) => q.ScanTagMembersAsync(req, ct));

    /// <inheritdoc />
    public override Task<StructureResponse> GetTreeStructure(StructureRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            var result = await q.GetTreeStructureAsync(req, ct).ConfigureAwait(false);

            // A TreeNotFound outcome is part of the typed contract, not a fault:
            // the response carries a Status field, so it rides as structured
            // content exactly like GetEntry (issue #1339) rather than collapsing
            // into an opaque NotFound transport error.
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

            // TreeNotFound (and IndexNotFound, for a tag-filtered scan naming an
            // unknown index) are typed statuses, not faults: they ride as
            // structured content like GetEntry (issue #1339) rather than as an
            // opaque NotFound transport error.
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
            // A not-found outcome (unknown tree or missing key) is part of the
            // typed contract, not a fault: the response carries a Status field that
            // distinguishes TreeNotFound from KeyNotFound, so both cases return the
            // structured response rather than throwing a NotFound RpcException. The
            // client maps the status onto its typed not-found result; throwing here
            // would collapse both cases into one opaque transport error and force
            // the caller to treat a routine miss as an exception (issue #1339).
            var result = await q.GetEntryAsync(req.TreeId, req.Key, ct).ConfigureAwait(false);
            return new EntryGetResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Key = result.Key,
                Entry = result.Entry,
            };
        });

    /// <inheritdoc />
    public override Task<EntryHistoryResponse> GetEntryHistory(EntryHistoryRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            var result = await q.GetEntryHistoryAsync(req, ct).ConfigureAwait(false);

            // TreeNotFound (and KeyNotFound, for a key that never existed) are
            // typed statuses, not faults: they ride as structured content like
            // GetEntry (issue #1339) rather than as an opaque NotFound transport
            // error.
            return new EntryHistoryResponse
            {
                Status = result.Status,
                TreeId = result.TreeId,
                Key = result.Key,
                Revisions = result.Revisions,
                ContinuationToken = result.ContinuationToken,
                Bound = result.Bound,
                EarliestAvailable = result.EarliestAvailable,
            };
        });

    /// <inheritdoc />
    public override Task<EntryScanCancelResponse> CancelScan(EntryScanCancelRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (q, req, ct) =>
        {
            await q.CancelScanAsync(req.TreeId, req.ContinuationToken, ct).ConfigureAwait(false);
            return new EntryScanCancelResponse();
        });

    /// <inheritdoc />
    public override async Task ObserveChanges(
        StateObserveRequest request,
        IServerStreamWriter<StateChangeNotification> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);
        using var activeTenantScope = StampActiveTenant(context);

        try
        {
            await foreach (var notification in _observer
                .ObserveAsync(request, context.CancellationToken)
                .ConfigureAwait(false))
            {
                await responseStream.WriteAsync(notification).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // Client tore down the subscription; a clean return ends the stream.
        }
        catch (LatticeStateCursorExpiredException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.State: gRPC change subscription for tree {TreeId} failed.", request.TreeId);
            throw new RpcException(new Status(StatusCode.Internal, "The state-API change subscription failed."));
        }
    }

    /// <inheritdoc />
    public override async Task ObserveMetrics(
        TreeMetricsRequest request,
        IServerStreamWriter<TreeMetricsSnapshot> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);
        using var activeTenantScope = StampActiveTenant(context);

        try
        {
            await foreach (var snapshot in _metricsObserver
                .ObserveAsync(request, context.CancellationToken)
                .ConfigureAwait(false))
            {
                await responseStream.WriteAsync(snapshot).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // Client tore down the subscription; a clean return ends the stream.
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.State: gRPC metrics subscription failed.");
            throw new RpcException(new Status(StatusCode.Internal, "The state-API metrics subscription failed."));
        }
    }

    /// <inheritdoc />
    public override async Task<TreeMetricsSnapshot> GetMetricsSnapshot(TreeMetricsRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);
        using var activeTenantScope = StampActiveTenant(context);

        try
        {
            return await _metricsObserver.SampleAsync(request, context.CancellationToken).ConfigureAwait(false);
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

    /// <inheritdoc />
    public override Task<ClusterInfo> GetClusterInfo(ClusterInfoRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (query, _, ct) => query.GetClusterInfoAsync(ct));

    /// <inheritdoc />
    public override Task<DeadLetterCountResponse> GetDeadLetterCount(DeadLetterCountRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (query, req, ct) =>
        {
            var count = await query.GetDeadLetterCountAsync(req.TreeId, ct).ConfigureAwait(false);
            return new DeadLetterCountResponse { TreeId = req.TreeId, Count = count };
        });

    /// <inheritdoc />
    public override Task<DeadLetterQueuePage> ListDeadLetters(DeadLetterQueueRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (query, req, ct) => query.ListDeadLettersAsync(req, ct));

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
        Func<ILatticeStateQuery, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);
        using var activeTenantScope = StampActiveTenant(context);

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
        catch (LatticeSaturatedException)
        {
            // The tree is WAL-saturated and shed the operation (e.g. a snapshot
            // cursor open refused at admission, issue #1053). Map to the canonical
            // gRPC "resource exhausted / busy, retry later" code so the client can
            // back off and retry rather than treating it as a hard failure - and
            // so it is never re-thrown as an opaque Internal 500. The user-facing
            // wording stays non-expert; callers key off the ResourceExhausted code.
            throw new RpcException(new Status(
                StatusCode.ResourceExhausted,
                "The requested tree is busy (storage back-pressure) and the operation was refused. Retry after a short backoff."));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.State: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The state-API request failed."));
        }
    }
}
