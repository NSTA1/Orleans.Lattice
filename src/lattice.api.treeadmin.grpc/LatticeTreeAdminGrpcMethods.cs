using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// tree-administration control API. Each method is a unary RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder wire
/// up identical marshallers.
/// </summary>
/// <remarks>
/// This foundation contract is a minimal set of RPCs over the transport-agnostic
/// <see cref="ILatticeTreeAdmin"/> facade: the capability probe
/// (<c>ProbeCapabilities</c>) and unauthenticated discovery (<c>GetAuthScheme</c>).
/// The whole-tree lifecycle operations land in later releases, each appending an
/// RPC here. Contract-versioning policy: fields on the wire messages are
/// additive-only (new <c>[Id(n)]</c>); aliases and field numbers are never
/// renumbered, so a newer response decodes cleanly under an older client, and new
/// RPCs are added without renaming or renumbering the existing ones.
/// </remarks>
internal sealed class LatticeTreeAdminGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.treeadmin";

    /// <summary>The unary capability-probe RPC method name.</summary>
    public const string ProbeCapabilitiesMethodName = "ProbeCapabilities";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>The unary shard-hotness RPC method name.</summary>
    public const string GetShardHotnessMethodName = "GetShardHotness";

    /// <summary>The unary shard-diagnostics RPC method name.</summary>
    public const string GetDiagnosticsMethodName = "GetDiagnostics";

    /// <summary>The unary shard-map inspection RPC method name.</summary>
    public const string InspectShardMapMethodName = "InspectShardMap";

    /// <summary>The unary projection-digest RPC method name.</summary>
    public const string GetProjectionDigestMethodName = "GetProjectionDigest";

    /// <summary>The unary tree-statistics RPC method name.</summary>
    public const string GetTreeStatsMethodName = "GetTreeStats";

    /// <summary>The unary cluster-wide storage-usage RPC method name.</summary>
    public const string GetStorageUsageMethodName = "GetStorageUsage";

    /// <summary>The unary explicit tree-creation RPC method name.</summary>
    public const string CreateTreeMethodName = "CreateTree";

    /// <summary>The unary tree-existence RPC method name.</summary>
    public const string CheckTreeExistsMethodName = "CheckTreeExists";

    /// <summary>The unary set-alias RPC method name.</summary>
    public const string SetTreeAliasMethodName = "SetTreeAlias";

    /// <summary>The unary resolve-alias RPC method name.</summary>
    public const string ResolveTreeAliasMethodName = "ResolveTreeAlias";

    /// <summary>The unary get-config RPC method name.</summary>
    public const string GetTreeConfigMethodName = "GetTreeConfig";

    /// <summary>The unary set-config RPC method name.</summary>
    public const string SetTreeConfigMethodName = "SetTreeConfig";

    /// <summary>The unary registry-persisted shard-map RPC method name.</summary>
    public const string GetShardMapMethodName = "GetShardMap";

    /// <summary>The unary tree soft-delete RPC method name.</summary>
    public const string DeleteTreeMethodName = "DeleteTree";

    /// <summary>The unary tree recover RPC method name.</summary>
    public const string RecoverTreeMethodName = "RecoverTree";

    /// <summary>The unary tree hard-purge RPC method name.</summary>
    public const string PurgeTreeMethodName = "PurgeTree";

    /// <summary>The unary tree deletion-status RPC method name.</summary>
    public const string GetTreeDeletionStatusMethodName = "GetTreeDeletionStatus";

    /// <summary>The unary bulk-load begin (session-open) RPC method name.</summary>
    public const string BeginBulkLoadMethodName = "BeginBulkLoad";

    /// <summary>The unary bulk-load append (chunk-graft) RPC method name.</summary>
    public const string AppendBulkLoadMethodName = "AppendBulkLoad";

    /// <summary>The unary bulk-load commit (session-close) RPC method name.</summary>
    public const string CommitBulkLoadMethodName = "CommitBulkLoad";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeTreeAdminGrpcMethods(
        Serializer<TreeAdminTreeRequest> treeRequestSerializer,
        Serializer<LatticeTreeAdminCapabilities> capabilitiesSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer,
        Serializer<TreeAdminShardRequest> shardRequestSerializer,
        Serializer<TreeAdminDiagnosticsRequest> diagnosticsRequestSerializer,
        Serializer<TreeAdminStorageUsageRequest> storageUsageRequestSerializer,
        Serializer<TreeHotnessReport> hotnessReportSerializer,
        Serializer<TreeAdminDiagnosticReport> diagnosticReportSerializer,
        Serializer<ShardMapInspection> shardMapInspectionSerializer,
        Serializer<ShardProjectionDigestReport> projectionDigestSerializer,
        Serializer<TreeStatsReport> treeStatsSerializer,
        Serializer<ClusterStorageUsageSummary> storageUsageSummarySerializer,
        Serializer<TreeAdminCreateRequest> createRequestSerializer,
        Serializer<TreeAdminSetAliasRequest> setAliasRequestSerializer,
        Serializer<TreeAdminSetConfigRequest> setConfigRequestSerializer,
        Serializer<TreeCreationResult> creationResultSerializer,
        Serializer<TreeExistenceResult> existenceResultSerializer,
        Serializer<TreeAliasResolution> aliasResolutionSerializer,
        Serializer<TreeConfigurationReport> configurationReportSerializer,
        Serializer<TreeShardMapView> shardMapViewSerializer,
        Serializer<TreeAdminPurgeRequest> purgeRequestSerializer,
        Serializer<TreeDeletionStatus> deletionStatusSerializer,
        Serializer<TreeAdminBulkLoadSessionRequest> bulkLoadSessionRequestSerializer,
        Serializer<TreeAdminBulkLoadAppendRequest> bulkLoadAppendRequestSerializer,
        Serializer<TreeBulkLoadSession> bulkLoadSessionSerializer,
        Serializer<TreeBulkLoadChunkAck> bulkLoadChunkAckSerializer,
        Serializer<TreeBulkLoadResult> bulkLoadResultSerializer)
    {
        ArgumentNullException.ThrowIfNull(treeRequestSerializer);
        ArgumentNullException.ThrowIfNull(capabilitiesSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);
        ArgumentNullException.ThrowIfNull(shardRequestSerializer);
        ArgumentNullException.ThrowIfNull(diagnosticsRequestSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageRequestSerializer);
        ArgumentNullException.ThrowIfNull(hotnessReportSerializer);
        ArgumentNullException.ThrowIfNull(diagnosticReportSerializer);
        ArgumentNullException.ThrowIfNull(shardMapInspectionSerializer);
        ArgumentNullException.ThrowIfNull(projectionDigestSerializer);
        ArgumentNullException.ThrowIfNull(treeStatsSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageSummarySerializer);
        ArgumentNullException.ThrowIfNull(createRequestSerializer);
        ArgumentNullException.ThrowIfNull(setAliasRequestSerializer);
        ArgumentNullException.ThrowIfNull(setConfigRequestSerializer);
        ArgumentNullException.ThrowIfNull(creationResultSerializer);
        ArgumentNullException.ThrowIfNull(existenceResultSerializer);
        ArgumentNullException.ThrowIfNull(aliasResolutionSerializer);
        ArgumentNullException.ThrowIfNull(configurationReportSerializer);
        ArgumentNullException.ThrowIfNull(shardMapViewSerializer);
        ArgumentNullException.ThrowIfNull(purgeRequestSerializer);
        ArgumentNullException.ThrowIfNull(deletionStatusSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadSessionRequestSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadAppendRequestSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadSessionSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadChunkAckSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadResultSerializer);

        ProbeCapabilities = new Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeCapabilitiesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(capabilitiesSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeAdvertisementSerializer));

        GetShardHotness = new Method<TreeAdminTreeRequest, TreeHotnessReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetShardHotnessMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(hotnessReportSerializer));

        GetDiagnostics = new Method<TreeAdminDiagnosticsRequest, TreeAdminDiagnosticReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetDiagnosticsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(diagnosticsRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(diagnosticReportSerializer));

        InspectShardMap = new Method<TreeAdminTreeRequest, ShardMapInspection>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: InspectShardMapMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardMapInspectionSerializer));

        GetProjectionDigest = new Method<TreeAdminShardRequest, ShardProjectionDigestReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetProjectionDigestMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(projectionDigestSerializer));

        GetTreeStats = new Method<TreeAdminTreeRequest, TreeStatsReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeStatsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeStatsSerializer));

        GetStorageUsage = new Method<TreeAdminStorageUsageRequest, ClusterStorageUsageSummary>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetStorageUsageMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(storageUsageRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(storageUsageSummarySerializer));

        CreateTree = new Method<TreeAdminCreateRequest, TreeCreationResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(createRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(creationResultSerializer));

        CheckTreeExists = new Method<TreeAdminTreeRequest, TreeExistenceResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CheckTreeExistsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(existenceResultSerializer));

        SetTreeAlias = new Method<TreeAdminSetAliasRequest, TreeAliasResolution>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetTreeAliasMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(setAliasRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(aliasResolutionSerializer));

        ResolveTreeAlias = new Method<TreeAdminTreeRequest, TreeAliasResolution>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ResolveTreeAliasMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(aliasResolutionSerializer));

        GetTreeConfig = new Method<TreeAdminTreeRequest, TreeConfigurationReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeConfigMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(configurationReportSerializer));

        SetTreeConfig = new Method<TreeAdminSetConfigRequest, TreeConfigurationReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetTreeConfigMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(setConfigRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(configurationReportSerializer));

        GetShardMap = new Method<TreeAdminTreeRequest, TreeShardMapView>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetShardMapMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardMapViewSerializer));

        DeleteTree = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DeleteTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        RecoverTree = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RecoverTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        PurgeTree = new Method<TreeAdminPurgeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PurgeTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(purgeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        GetTreeDeletionStatus = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeDeletionStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        BeginBulkLoad = new Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadSession>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: BeginBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionSerializer));

        AppendBulkLoad = new Method<TreeAdminBulkLoadAppendRequest, TreeBulkLoadChunkAck>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: AppendBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadAppendRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadChunkAckSerializer));

        CommitBulkLoad = new Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CommitBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadResultSerializer));
    }

    /// <summary>The unary <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    public Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities> ProbeCapabilities { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>The unary <c>GetShardHotness</c> read-only hotness RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeHotnessReport> GetShardHotness { get; }

    /// <summary>The unary <c>GetDiagnostics</c> read-only diagnostics RPC.</summary>
    public Method<TreeAdminDiagnosticsRequest, TreeAdminDiagnosticReport> GetDiagnostics { get; }

    /// <summary>The unary <c>InspectShardMap</c> read-only topology RPC.</summary>
    public Method<TreeAdminTreeRequest, ShardMapInspection> InspectShardMap { get; }

    /// <summary>The unary <c>GetProjectionDigest</c> read-only digest RPC.</summary>
    public Method<TreeAdminShardRequest, ShardProjectionDigestReport> GetProjectionDigest { get; }

    /// <summary>The unary <c>GetTreeStats</c> read-only statistics RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeStatsReport> GetTreeStats { get; }

    /// <summary>The unary <c>GetStorageUsage</c> read-only cluster-storage RPC.</summary>
    public Method<TreeAdminStorageUsageRequest, ClusterStorageUsageSummary> GetStorageUsage { get; }

    /// <summary>The unary <c>CreateTree</c> explicit-creation lifecycle RPC.</summary>
    public Method<TreeAdminCreateRequest, TreeCreationResult> CreateTree { get; }

    /// <summary>The unary <c>CheckTreeExists</c> read-only existence RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeExistenceResult> CheckTreeExists { get; }

    /// <summary>The unary <c>SetTreeAlias</c> alias-assignment lifecycle RPC.</summary>
    public Method<TreeAdminSetAliasRequest, TreeAliasResolution> SetTreeAlias { get; }

    /// <summary>The unary <c>ResolveTreeAlias</c> read-only alias-resolution RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeAliasResolution> ResolveTreeAlias { get; }

    /// <summary>The unary <c>GetTreeConfig</c> read-only configuration RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeConfigurationReport> GetTreeConfig { get; }

    /// <summary>The unary <c>SetTreeConfig</c> configuration-update lifecycle RPC.</summary>
    public Method<TreeAdminSetConfigRequest, TreeConfigurationReport> SetTreeConfig { get; }

    /// <summary>The unary <c>GetShardMap</c> read-only registry-persisted shard-map RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeShardMapView> GetShardMap { get; }

    /// <summary>The unary <c>DeleteTree</c> soft-delete lifecycle RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> DeleteTree { get; }

    /// <summary>The unary <c>RecoverTree</c> recovery lifecycle RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> RecoverTree { get; }

    /// <summary>The unary <c>PurgeTree</c> irreversible hard-purge lifecycle RPC.</summary>
    public Method<TreeAdminPurgeRequest, TreeDeletionStatus> PurgeTree { get; }

    /// <summary>The unary <c>GetTreeDeletionStatus</c> read-only deletion-status RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> GetTreeDeletionStatus { get; }

    /// <summary>The unary <c>BeginBulkLoad</c> session-open RPC.</summary>
    public Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadSession> BeginBulkLoad { get; }

    /// <summary>The unary <c>AppendBulkLoad</c> chunk-graft RPC.</summary>
    public Method<TreeAdminBulkLoadAppendRequest, TreeBulkLoadChunkAck> AppendBulkLoad { get; }

    /// <summary>The unary <c>CommitBulkLoad</c> session-close RPC.</summary>
    public Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadResult> CommitBulkLoad { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeTreeAdminGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTreeAdminGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<TreeAdminTreeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeTreeAdminCapabilities>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminShardRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminDiagnosticsRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminStorageUsageRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeHotnessReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminDiagnosticReport>>(),
            serializerProvider.GetRequiredService<Serializer<ShardMapInspection>>(),
            serializerProvider.GetRequiredService<Serializer<ShardProjectionDigestReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeStatsReport>>(),
            serializerProvider.GetRequiredService<Serializer<ClusterStorageUsageSummary>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminCreateRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSetAliasRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSetConfigRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeCreationResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeExistenceResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAliasResolution>>(),
            serializerProvider.GetRequiredService<Serializer<TreeConfigurationReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeShardMapView>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminPurgeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeDeletionStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminBulkLoadSessionRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminBulkLoadAppendRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadSession>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadChunkAck>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadResult>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeTreeAdminGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed: subsequent registrations
/// replace the prior instance, matching the "last-host-wins" semantics
/// integration-test fixtures rely on.
/// </summary>
internal static class LatticeTreeAdminGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeTreeAdminGrpcMethods? Current { get; set; }
}
