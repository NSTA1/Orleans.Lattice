using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for
/// the read-only cluster state API. Each method is a unary RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the (future) client invoker and the server-side binder
/// wire up identical marshallers.
/// </summary>
/// <remarks>
/// The contract is intentionally a flat set of unary RPCs: discovery
/// (<c>ListTrees</c> / <c>ListViews</c>), tree structure
/// (<c>GetTreeStructure</c>), and entry inspection (<c>ScanEntries</c> /
/// <c>GetEntry</c>). Server-streaming subscription and live-metadata
/// streaming are added additively by later issues as new methods on the same
/// service. Contract-versioning policy: fields on the wire messages are
/// additive-only (new <c>[Id(n)]</c>); aliases and field numbers are never
/// renumbered, so a newer response decodes cleanly under an older client.
/// </remarks>
internal sealed class LatticeStateGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.state";

    /// <summary>The unary tree-catalog discovery RPC method name.</summary>
    public const string ListTreesMethodName = "ListTrees";

    /// <summary>The unary view-catalog discovery RPC method name.</summary>
    public const string ListViewsMethodName = "ListViews";

    /// <summary>The unary tag-index-catalog discovery RPC method name.</summary>
    public const string ListTagIndexesMethodName = "ListTagIndexes";

    /// <summary>The unary tree-structure RPC method name.</summary>
    public const string GetTreeStructureMethodName = "GetTreeStructure";

    /// <summary>The unary entry-scan RPC method name.</summary>
    public const string ScanEntriesMethodName = "ScanEntries";

    /// <summary>The unary single-entry get RPC method name.</summary>
    public const string GetEntryMethodName = "GetEntry";

    /// <summary>The unary scan-cursor cancel RPC method name.</summary>
    public const string CancelScanMethodName = "CancelScan";

    /// <summary>The server-streaming change-observation RPC method name.</summary>
    public const string ObserveChangesMethodName = "ObserveChanges";

    /// <summary>The server-streaming metrics-observation RPC method name.</summary>
    public const string ObserveMetricsMethodName = "ObserveMetrics";

    /// <summary>The unary one-shot metrics-snapshot RPC method name.</summary>
    public const string GetMetricsSnapshotMethodName = "GetMetricsSnapshot";

    /// <summary>The unary cluster-info RPC method name.</summary>
    public const string GetClusterInfoMethodName = "GetClusterInfo";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeStateGrpcMethods(
        Serializer<CatalogRequest> catalogRequestSerializer,
        Serializer<TreeCatalogPage> treeCatalogPageSerializer,
        Serializer<ViewCatalogPage> viewCatalogPageSerializer,
        Serializer<TagIndexCatalogPage> tagIndexCatalogPageSerializer,
        Serializer<StructureRequest> structureRequestSerializer,
        Serializer<StructureResponse> structureResponseSerializer,
        Serializer<EntryScanRequest> entryScanRequestSerializer,
        Serializer<EntryScanResponse> entryScanResponseSerializer,
        Serializer<EntryGetRequest> entryGetRequestSerializer,
        Serializer<EntryGetResponse> entryGetResponseSerializer,
        Serializer<EntryScanCancelRequest> entryScanCancelRequestSerializer,
        Serializer<EntryScanCancelResponse> entryScanCancelResponseSerializer,
        Serializer<StateObserveRequest> observeRequestSerializer,
        Serializer<StateChangeNotification> changeNotificationSerializer,
        Serializer<TreeMetricsRequest> metricsRequestSerializer,
        Serializer<TreeMetricsSnapshot> metricsSnapshotSerializer,
        Serializer<ClusterInfoRequest> clusterInfoRequestSerializer,
        Serializer<ClusterInfo> clusterInfoSerializer)
    {
        ArgumentNullException.ThrowIfNull(catalogRequestSerializer);
        ArgumentNullException.ThrowIfNull(treeCatalogPageSerializer);
        ArgumentNullException.ThrowIfNull(viewCatalogPageSerializer);
        ArgumentNullException.ThrowIfNull(tagIndexCatalogPageSerializer);
        ArgumentNullException.ThrowIfNull(structureRequestSerializer);
        ArgumentNullException.ThrowIfNull(structureResponseSerializer);
        ArgumentNullException.ThrowIfNull(entryScanRequestSerializer);
        ArgumentNullException.ThrowIfNull(entryScanResponseSerializer);
        ArgumentNullException.ThrowIfNull(entryGetRequestSerializer);
        ArgumentNullException.ThrowIfNull(entryGetResponseSerializer);
        ArgumentNullException.ThrowIfNull(entryScanCancelRequestSerializer);
        ArgumentNullException.ThrowIfNull(entryScanCancelResponseSerializer);
        ArgumentNullException.ThrowIfNull(observeRequestSerializer);
        ArgumentNullException.ThrowIfNull(changeNotificationSerializer);
        ArgumentNullException.ThrowIfNull(metricsRequestSerializer);
        ArgumentNullException.ThrowIfNull(metricsSnapshotSerializer);
        ArgumentNullException.ThrowIfNull(clusterInfoRequestSerializer);
        ArgumentNullException.ThrowIfNull(clusterInfoSerializer);

        ListTrees = new Method<CatalogRequest, TreeCatalogPage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListTreesMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(catalogRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(treeCatalogPageSerializer));

        ListViews = new Method<CatalogRequest, ViewCatalogPage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListViewsMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(catalogRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(viewCatalogPageSerializer));

        ListTagIndexes = new Method<CatalogRequest, TagIndexCatalogPage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListTagIndexesMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(catalogRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(tagIndexCatalogPageSerializer));

        GetTreeStructure = new Method<StructureRequest, StructureResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeStructureMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(structureRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(structureResponseSerializer));

        ScanEntries = new Method<EntryScanRequest, EntryScanResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ScanEntriesMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(entryScanRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(entryScanResponseSerializer));

        GetEntry = new Method<EntryGetRequest, EntryGetResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetEntryMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(entryGetRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(entryGetResponseSerializer));

        CancelScan = new Method<EntryScanCancelRequest, EntryScanCancelResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CancelScanMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(entryScanCancelRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(entryScanCancelResponseSerializer));

        ObserveChanges = new Method<StateObserveRequest, StateChangeNotification>(
            type: MethodType.ServerStreaming,
            serviceName: ServiceName,
            name: ObserveChangesMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(observeRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(changeNotificationSerializer));

        ObserveMetrics = new Method<TreeMetricsRequest, TreeMetricsSnapshot>(
            type: MethodType.ServerStreaming,
            serviceName: ServiceName,
            name: ObserveMetricsMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(metricsRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(metricsSnapshotSerializer));

        GetMetricsSnapshot = new Method<TreeMetricsRequest, TreeMetricsSnapshot>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetMetricsSnapshotMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(metricsRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(metricsSnapshotSerializer));

        GetClusterInfo = new Method<ClusterInfoRequest, ClusterInfo>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetClusterInfoMethodName,
            requestMarshaller: LatticeStateGrpcMarshallers.Create(clusterInfoRequestSerializer),
            responseMarshaller: LatticeStateGrpcMarshallers.Create(clusterInfoSerializer));
    }

    /// <summary>The unary <c>ListTrees</c> discovery RPC.</summary>
    public Method<CatalogRequest, TreeCatalogPage> ListTrees { get; }

    /// <summary>The unary <c>ListViews</c> discovery RPC.</summary>
    public Method<CatalogRequest, ViewCatalogPage> ListViews { get; }

    /// <summary>The unary <c>ListTagIndexes</c> discovery RPC.</summary>
    public Method<CatalogRequest, TagIndexCatalogPage> ListTagIndexes { get; }

    /// <summary>The unary <c>GetTreeStructure</c> RPC.</summary>
    public Method<StructureRequest, StructureResponse> GetTreeStructure { get; }

    /// <summary>The unary <c>ScanEntries</c> RPC.</summary>
    public Method<EntryScanRequest, EntryScanResponse> ScanEntries { get; }

    /// <summary>The unary <c>GetEntry</c> RPC.</summary>
    public Method<EntryGetRequest, EntryGetResponse> GetEntry { get; }

    /// <summary>The unary <c>CancelScan</c> RPC.</summary>
    public Method<EntryScanCancelRequest, EntryScanCancelResponse> CancelScan { get; }

    /// <summary>The server-streaming <c>ObserveChanges</c> subscription RPC.</summary>
    public Method<StateObserveRequest, StateChangeNotification> ObserveChanges { get; }

    /// <summary>The server-streaming <c>ObserveMetrics</c> live-metrics RPC.</summary>
    public Method<TreeMetricsRequest, TreeMetricsSnapshot> ObserveMetrics { get; }

    /// <summary>The unary one-shot <c>GetMetricsSnapshot</c> RPC.</summary>
    public Method<TreeMetricsRequest, TreeMetricsSnapshot> GetMetricsSnapshot { get; }

    /// <summary>The unary <c>GetClusterInfo</c> RPC.</summary>
    public Method<ClusterInfoRequest, ClusterInfo> GetClusterInfo { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out
    /// of <paramref name="serializerProvider"/>. Shared by the server-side DI
    /// factory and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeStateGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeStateGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<CatalogRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeCatalogPage>>(),
            serializerProvider.GetRequiredService<Serializer<ViewCatalogPage>>(),
            serializerProvider.GetRequiredService<Serializer<TagIndexCatalogPage>>(),
            serializerProvider.GetRequiredService<Serializer<StructureRequest>>(),
            serializerProvider.GetRequiredService<Serializer<StructureResponse>>(),
            serializerProvider.GetRequiredService<Serializer<EntryScanRequest>>(),
            serializerProvider.GetRequiredService<Serializer<EntryScanResponse>>(),
            serializerProvider.GetRequiredService<Serializer<EntryGetRequest>>(),
            serializerProvider.GetRequiredService<Serializer<EntryGetResponse>>(),
            serializerProvider.GetRequiredService<Serializer<EntryScanCancelRequest>>(),
            serializerProvider.GetRequiredService<Serializer<EntryScanCancelResponse>>(),
            serializerProvider.GetRequiredService<Serializer<StateObserveRequest>>(),
            serializerProvider.GetRequiredService<Serializer<StateChangeNotification>>(),
            serializerProvider.GetRequiredService<Serializer<TreeMetricsRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeMetricsSnapshot>>(),
            serializerProvider.GetRequiredService<Serializer<ClusterInfoRequest>>(),
            serializerProvider.GetRequiredService<Serializer<ClusterInfo>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeStateGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeStateGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeStateGrpcMethods? Current { get; set; }
}
