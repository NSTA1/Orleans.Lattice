using Grpc.Core;
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
    public const string ServiceName = "orleans.lattice.api.state.LatticeState";

    /// <summary>The unary tree-catalog discovery RPC method name.</summary>
    public const string ListTreesMethodName = "ListTrees";

    /// <summary>The unary view-catalog discovery RPC method name.</summary>
    public const string ListViewsMethodName = "ListViews";

    /// <summary>The unary tree-structure RPC method name.</summary>
    public const string GetTreeStructureMethodName = "GetTreeStructure";

    /// <summary>The unary entry-scan RPC method name.</summary>
    public const string ScanEntriesMethodName = "ScanEntries";

    /// <summary>The unary single-entry get RPC method name.</summary>
    public const string GetEntryMethodName = "GetEntry";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeStateGrpcMethods(
        Serializer<CatalogRequest> catalogRequestSerializer,
        Serializer<TreeCatalogPage> treeCatalogPageSerializer,
        Serializer<ViewCatalogPage> viewCatalogPageSerializer,
        Serializer<StructureRequest> structureRequestSerializer,
        Serializer<StructureResponse> structureResponseSerializer,
        Serializer<EntryScanRequest> entryScanRequestSerializer,
        Serializer<EntryScanResponse> entryScanResponseSerializer,
        Serializer<EntryGetRequest> entryGetRequestSerializer,
        Serializer<EntryGetResponse> entryGetResponseSerializer)
    {
        ArgumentNullException.ThrowIfNull(catalogRequestSerializer);
        ArgumentNullException.ThrowIfNull(treeCatalogPageSerializer);
        ArgumentNullException.ThrowIfNull(viewCatalogPageSerializer);
        ArgumentNullException.ThrowIfNull(structureRequestSerializer);
        ArgumentNullException.ThrowIfNull(structureResponseSerializer);
        ArgumentNullException.ThrowIfNull(entryScanRequestSerializer);
        ArgumentNullException.ThrowIfNull(entryScanResponseSerializer);
        ArgumentNullException.ThrowIfNull(entryGetRequestSerializer);
        ArgumentNullException.ThrowIfNull(entryGetResponseSerializer);

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
    }

    /// <summary>The unary <c>ListTrees</c> discovery RPC.</summary>
    public Method<CatalogRequest, TreeCatalogPage> ListTrees { get; }

    /// <summary>The unary <c>ListViews</c> discovery RPC.</summary>
    public Method<CatalogRequest, ViewCatalogPage> ListViews { get; }

    /// <summary>The unary <c>GetTreeStructure</c> RPC.</summary>
    public Method<StructureRequest, StructureResponse> GetTreeStructure { get; }

    /// <summary>The unary <c>ScanEntries</c> RPC.</summary>
    public Method<EntryScanRequest, EntryScanResponse> ScanEntries { get; }

    /// <summary>The unary <c>GetEntry</c> RPC.</summary>
    public Method<EntryGetRequest, EntryGetResponse> GetEntry { get; }
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
