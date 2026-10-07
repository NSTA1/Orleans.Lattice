using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// schema control API. Each method is a unary or server-streaming RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder
/// wire up identical marshallers.
/// </summary>
/// <remarks>
/// The contract is a flat set of RPCs over the transport-agnostic
/// <see cref="ILatticeSchemaControl"/> facade: policy management
/// (<c>SetPolicy</c> / <c>ClearPolicy</c> / <c>GetPolicy</c>), dead letters
/// (<c>StreamDeadLetters</c> server-streaming + <c>CountDeadLetters</c>),
/// versioning (<c>SetVersionConfig</c> / <c>GetVersionConfig</c> /
/// <c>AdvanceTargetVersion</c> / <c>ClearVersionConfig</c>), remediation
/// (<c>GetRemediationStatus</c>), the read-only compliance
/// audit, the capability probe (<c>ProbeCapabilities</c>),
/// unauthenticated discovery (<c>GetAuthScheme</c>), and the accept-then-poll
/// compliance-scan operation RPCs (<c>StartComplianceScan</c>,
/// <c>GetComplianceScanStatus</c>, <c>ListComplianceScans</c>,
/// <c>CancelComplianceScan</c>) and the remediation and migration operation RPCs
/// (<c>StartRemediation</c>, <c>StartMigration</c>, <c>StartAdvanceAndMigrate</c>,
/// <c>GetSchemaOperationStatus</c>, <c>ListSchemaOperations</c>,
/// <c>CancelSchemaOperation</c>). Contract-versioning policy: fields on the wire
/// messages are additive-only (new <c>[Id(n)]</c>);
/// aliases and field numbers are never renumbered, so a newer response decodes
/// cleanly under an older client.
/// </remarks>
internal sealed class LatticeSchemaGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.schema";

    /// <summary>The unary set-policy RPC method name.</summary>
    public const string SetPolicyMethodName = "SetPolicy";

    /// <summary>The unary clear-policy RPC method name.</summary>
    public const string ClearPolicyMethodName = "ClearPolicy";

    /// <summary>The unary get-policy RPC method name.</summary>
    public const string GetPolicyMethodName = "GetPolicy";

    /// <summary>The server-streaming dead-letter drain RPC method name.</summary>
    public const string StreamDeadLettersMethodName = "StreamDeadLetters";

    /// <summary>The unary count-dead-letters RPC method name.</summary>
    public const string CountDeadLettersMethodName = "CountDeadLetters";

    /// <summary>The unary set-version-config RPC method name.</summary>
    public const string SetVersionConfigMethodName = "SetVersionConfig";

    /// <summary>The unary get-version-config RPC method name.</summary>
    public const string GetVersionConfigMethodName = "GetVersionConfig";

    /// <summary>The unary advance-target-version RPC method name.</summary>
    public const string AdvanceTargetVersionMethodName = "AdvanceTargetVersion";

    /// <summary>The unary clear-version-config RPC method name.</summary>
    public const string ClearVersionConfigMethodName = "ClearVersionConfig";

    /// <summary>The unary get-remediation-status RPC method name.</summary>
    public const string GetRemediationStatusMethodName = "GetRemediationStatus";

    /// <summary>The unary capability-probe RPC method name.</summary>
    public const string ProbeCapabilitiesMethodName = "ProbeCapabilities";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>The unary accept-then-poll compliance-scan start RPC method name.</summary>
    public const string StartComplianceScanMethodName = "StartComplianceScan";

    /// <summary>The unary compliance-scan operation status RPC method name.</summary>
    public const string GetComplianceScanStatusMethodName = "GetComplianceScanStatus";

    /// <summary>The unary compliance-scan operation listing RPC method name.</summary>
    public const string ListComplianceScansMethodName = "ListComplianceScans";

    /// <summary>The unary compliance-scan operation cancellation RPC method name.</summary>
    public const string CancelComplianceScanMethodName = "CancelComplianceScan";

    /// <summary>The unary accept-then-poll remediation start RPC method name.</summary>
    public const string StartRemediationMethodName = "StartRemediation";

    /// <summary>The unary accept-then-poll migration start RPC method name.</summary>
    public const string StartMigrationMethodName = "StartMigration";

    /// <summary>The unary accept-then-poll advance-and-migrate start RPC method name.</summary>
    public const string StartAdvanceAndMigrateMethodName = "StartAdvanceAndMigrate";

    /// <summary>The unary schema-operation status RPC method name.</summary>
    public const string GetSchemaOperationStatusMethodName = "GetSchemaOperationStatus";

    /// <summary>The unary schema-operation listing RPC method name.</summary>
    public const string ListSchemaOperationsMethodName = "ListSchemaOperations";

    /// <summary>The unary schema-operation cancellation RPC method name.</summary>
    public const string CancelSchemaOperationMethodName = "CancelSchemaOperation";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeSchemaGrpcMethods(
        Serializer<SetPolicyRequest> setPolicyRequestSerializer,
        Serializer<SchemaTreeRequest> treeRequestSerializer,
        Serializer<SchemaAckResponse> ackResponseSerializer,
        Serializer<SchemaRemovedResponse> removedResponseSerializer,
        Serializer<GetPolicyResponse> getPolicyResponseSerializer,
        Serializer<LatticeSchemaDeadLetterEntry> deadLetterSerializer,
        Serializer<SchemaCountResponse> countResponseSerializer,
        Serializer<SetVersionConfigRequest> setVersionConfigRequestSerializer,
        Serializer<GetVersionConfigResponse> getVersionConfigResponseSerializer,
        Serializer<AdvanceVersionRequest> advanceVersionRequestSerializer,
        Serializer<VersionConfigResponse> versionConfigResponseSerializer,
        Serializer<SchemaRemediationReportResponse> remediationReportResponseSerializer,
        Serializer<RemediateRequest> remediateRequestSerializer,
        Serializer<SchemaComplianceReportResponse> complianceReportResponseSerializer,
        Serializer<LatticeSchemaCapabilities> capabilitiesSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer,
        Serializer<SchemaComplianceScanStartRequest> complianceScanStartRequestSerializer,
        Serializer<LatticeOperationHandle> operationHandleSerializer,
        Serializer<SchemaComplianceOperationRequest> complianceOperationRequestSerializer,
        Serializer<SchemaComplianceOperationStatusResponse> complianceOperationStatusResponseSerializer,
        Serializer<SchemaMigrationStartRequest> migrationStartRequestSerializer,
        Serializer<SchemaOperationRequest> operationRequestSerializer,
        Serializer<SchemaOperationStatusResponse> operationStatusResponseSerializer,
        Serializer<LatticeOperationListRequest> operationListRequestSerializer,
        Serializer<LatticeOperationPage> operationPageSerializer)
    {
        ArgumentNullException.ThrowIfNull(setPolicyRequestSerializer);
        ArgumentNullException.ThrowIfNull(treeRequestSerializer);
        ArgumentNullException.ThrowIfNull(ackResponseSerializer);
        ArgumentNullException.ThrowIfNull(removedResponseSerializer);
        ArgumentNullException.ThrowIfNull(getPolicyResponseSerializer);
        ArgumentNullException.ThrowIfNull(deadLetterSerializer);
        ArgumentNullException.ThrowIfNull(countResponseSerializer);
        ArgumentNullException.ThrowIfNull(setVersionConfigRequestSerializer);
        ArgumentNullException.ThrowIfNull(getVersionConfigResponseSerializer);
        ArgumentNullException.ThrowIfNull(advanceVersionRequestSerializer);
        ArgumentNullException.ThrowIfNull(versionConfigResponseSerializer);
        ArgumentNullException.ThrowIfNull(remediationReportResponseSerializer);
        ArgumentNullException.ThrowIfNull(remediateRequestSerializer);
        ArgumentNullException.ThrowIfNull(complianceReportResponseSerializer);
        ArgumentNullException.ThrowIfNull(capabilitiesSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);
        ArgumentNullException.ThrowIfNull(complianceScanStartRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationHandleSerializer);
        ArgumentNullException.ThrowIfNull(complianceOperationRequestSerializer);
        ArgumentNullException.ThrowIfNull(complianceOperationStatusResponseSerializer);
        ArgumentNullException.ThrowIfNull(migrationStartRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationStatusResponseSerializer);
        ArgumentNullException.ThrowIfNull(operationListRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationPageSerializer);

        SetPolicy = new Method<SetPolicyRequest, SchemaAckResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetPolicyMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(setPolicyRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(ackResponseSerializer));

        ClearPolicy = new Method<SchemaTreeRequest, SchemaRemovedResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ClearPolicyMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(removedResponseSerializer));

        GetPolicy = new Method<SchemaTreeRequest, GetPolicyResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetPolicyMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(getPolicyResponseSerializer));

        StreamDeadLetters = new Method<SchemaTreeRequest, LatticeSchemaDeadLetterEntry>(
            type: MethodType.ServerStreaming,
            serviceName: ServiceName,
            name: StreamDeadLettersMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(deadLetterSerializer));

        CountDeadLetters = new Method<SchemaTreeRequest, SchemaCountResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CountDeadLettersMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(countResponseSerializer));

        SetVersionConfig = new Method<SetVersionConfigRequest, SchemaAckResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetVersionConfigMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(setVersionConfigRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(ackResponseSerializer));

        GetVersionConfig = new Method<SchemaTreeRequest, GetVersionConfigResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetVersionConfigMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(getVersionConfigResponseSerializer));

        AdvanceTargetVersion = new Method<AdvanceVersionRequest, VersionConfigResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: AdvanceTargetVersionMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(advanceVersionRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(versionConfigResponseSerializer));



        ClearVersionConfig = new Method<SchemaTreeRequest, SchemaRemovedResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ClearVersionConfigMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(removedResponseSerializer));


        GetRemediationStatus = new Method<SchemaTreeRequest, SchemaRemediationReportResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetRemediationStatusMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(remediationReportResponseSerializer));


        ProbeCapabilities = new Method<SchemaTreeRequest, LatticeSchemaCapabilities>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeCapabilitiesMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(capabilitiesSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeSchemaGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeSchemaGrpcMarshallers.Create(authSchemeAdvertisementSerializer));

        var complianceOperationRequestMarshaller = LatticeSchemaGrpcMarshallers.Create(complianceOperationRequestSerializer);
        var complianceOperationStatusMarshaller = LatticeSchemaGrpcMarshallers.Create(complianceOperationStatusResponseSerializer);

        StartComplianceScan = new Method<SchemaComplianceScanStartRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartComplianceScanMethodName,
            LatticeSchemaGrpcMarshallers.Create(complianceScanStartRequestSerializer),
            LatticeSchemaGrpcMarshallers.Create(operationHandleSerializer));

        GetComplianceScanStatus = new Method<SchemaComplianceOperationRequest, SchemaComplianceOperationStatusResponse>(
            MethodType.Unary, ServiceName, GetComplianceScanStatusMethodName,
            complianceOperationRequestMarshaller, complianceOperationStatusMarshaller);

        ListComplianceScans = new Method<LatticeOperationListRequest, LatticeOperationPage>(
            MethodType.Unary, ServiceName, ListComplianceScansMethodName,
            LatticeSchemaGrpcMarshallers.Create(operationListRequestSerializer),
            LatticeSchemaGrpcMarshallers.Create(operationPageSerializer));

        CancelComplianceScan = new Method<SchemaComplianceOperationRequest, SchemaComplianceOperationStatusResponse>(
            MethodType.Unary, ServiceName, CancelComplianceScanMethodName,
            complianceOperationRequestMarshaller, complianceOperationStatusMarshaller);

        var handleMarshaller = LatticeSchemaGrpcMarshallers.Create(operationHandleSerializer);
        var operationRequestMarshaller = LatticeSchemaGrpcMarshallers.Create(operationRequestSerializer);
        var operationStatusMarshaller = LatticeSchemaGrpcMarshallers.Create(operationStatusResponseSerializer);

        StartRemediation = new Method<RemediateRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartRemediationMethodName,
            LatticeSchemaGrpcMarshallers.Create(remediateRequestSerializer), handleMarshaller);

        StartMigration = new Method<SchemaMigrationStartRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartMigrationMethodName,
            LatticeSchemaGrpcMarshallers.Create(migrationStartRequestSerializer), handleMarshaller);

        StartAdvanceAndMigrate = new Method<AdvanceVersionRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartAdvanceAndMigrateMethodName,
            LatticeSchemaGrpcMarshallers.Create(advanceVersionRequestSerializer), handleMarshaller);

        GetSchemaOperationStatus = new Method<SchemaOperationRequest, SchemaOperationStatusResponse>(
            MethodType.Unary, ServiceName, GetSchemaOperationStatusMethodName,
            operationRequestMarshaller, operationStatusMarshaller);

        ListSchemaOperations = new Method<LatticeOperationListRequest, LatticeOperationPage>(
            MethodType.Unary, ServiceName, ListSchemaOperationsMethodName,
            LatticeSchemaGrpcMarshallers.Create(operationListRequestSerializer),
            LatticeSchemaGrpcMarshallers.Create(operationPageSerializer));

        CancelSchemaOperation = new Method<SchemaOperationRequest, SchemaOperationStatusResponse>(
            MethodType.Unary, ServiceName, CancelSchemaOperationMethodName,
            operationRequestMarshaller, operationStatusMarshaller);
    }

    /// <summary>The unary <c>SetPolicy</c> RPC.</summary>
    public Method<SetPolicyRequest, SchemaAckResponse> SetPolicy { get; }

    /// <summary>The unary <c>ClearPolicy</c> RPC.</summary>
    public Method<SchemaTreeRequest, SchemaRemovedResponse> ClearPolicy { get; }

    /// <summary>The unary <c>GetPolicy</c> RPC.</summary>
    public Method<SchemaTreeRequest, GetPolicyResponse> GetPolicy { get; }

    /// <summary>The server-streaming <c>StreamDeadLetters</c> RPC.</summary>
    public Method<SchemaTreeRequest, LatticeSchemaDeadLetterEntry> StreamDeadLetters { get; }

    /// <summary>The unary <c>CountDeadLetters</c> RPC.</summary>
    public Method<SchemaTreeRequest, SchemaCountResponse> CountDeadLetters { get; }

    /// <summary>The unary <c>SetVersionConfig</c> RPC.</summary>
    public Method<SetVersionConfigRequest, SchemaAckResponse> SetVersionConfig { get; }

    /// <summary>The unary <c>GetVersionConfig</c> RPC.</summary>
    public Method<SchemaTreeRequest, GetVersionConfigResponse> GetVersionConfig { get; }

    /// <summary>The unary <c>AdvanceTargetVersion</c> RPC.</summary>
    public Method<AdvanceVersionRequest, VersionConfigResponse> AdvanceTargetVersion { get; }

    /// <summary>The unary <c>ClearVersionConfig</c> RPC.</summary>
    public Method<SchemaTreeRequest, SchemaRemovedResponse> ClearVersionConfig { get; }

    /// <summary>The unary <c>GetRemediationStatus</c> RPC.</summary>
    public Method<SchemaTreeRequest, SchemaRemediationReportResponse> GetRemediationStatus { get; }

    /// <summary>The unary <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    public Method<SchemaTreeRequest, LatticeSchemaCapabilities> ProbeCapabilities { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>The unary <c>StartComplianceScan</c> accept-then-poll compliance-scan RPC.</summary>
    public Method<SchemaComplianceScanStartRequest, LatticeOperationHandle> StartComplianceScan { get; }

    /// <summary>The unary <c>GetComplianceScanStatus</c> RPC.</summary>
    public Method<SchemaComplianceOperationRequest, SchemaComplianceOperationStatusResponse> GetComplianceScanStatus { get; }

    /// <summary>The unary <c>ListComplianceScans</c> RPC.</summary>
    public Method<LatticeOperationListRequest, LatticeOperationPage> ListComplianceScans { get; }

    /// <summary>The unary <c>CancelComplianceScan</c> RPC.</summary>
    public Method<SchemaComplianceOperationRequest, SchemaComplianceOperationStatusResponse> CancelComplianceScan { get; }

    /// <summary>The unary <c>StartRemediation</c> accept-then-poll RPC.</summary>
    public Method<RemediateRequest, LatticeOperationHandle> StartRemediation { get; }

    /// <summary>The unary <c>StartMigration</c> accept-then-poll RPC.</summary>
    public Method<SchemaMigrationStartRequest, LatticeOperationHandle> StartMigration { get; }

    /// <summary>The unary <c>StartAdvanceAndMigrate</c> accept-then-poll RPC.</summary>
    public Method<AdvanceVersionRequest, LatticeOperationHandle> StartAdvanceAndMigrate { get; }

    /// <summary>The unary <c>GetSchemaOperationStatus</c> RPC.</summary>
    public Method<SchemaOperationRequest, SchemaOperationStatusResponse> GetSchemaOperationStatus { get; }

    /// <summary>The unary <c>ListSchemaOperations</c> RPC.</summary>
    public Method<LatticeOperationListRequest, LatticeOperationPage> ListSchemaOperations { get; }

    /// <summary>The unary <c>CancelSchemaOperation</c> RPC.</summary>
    public Method<SchemaOperationRequest, SchemaOperationStatusResponse> CancelSchemaOperation { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out
    /// of <paramref name="serializerProvider"/>. Shared by the server-side DI
    /// factory and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeSchemaGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeSchemaGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<SetPolicyRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaTreeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaAckResponse>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaRemovedResponse>>(),
            serializerProvider.GetRequiredService<Serializer<GetPolicyResponse>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeSchemaDeadLetterEntry>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaCountResponse>>(),
            serializerProvider.GetRequiredService<Serializer<SetVersionConfigRequest>>(),
            serializerProvider.GetRequiredService<Serializer<GetVersionConfigResponse>>(),
            serializerProvider.GetRequiredService<Serializer<AdvanceVersionRequest>>(),
            serializerProvider.GetRequiredService<Serializer<VersionConfigResponse>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaRemediationReportResponse>>(),
            serializerProvider.GetRequiredService<Serializer<RemediateRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaComplianceReportResponse>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeSchemaCapabilities>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaComplianceScanStartRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationHandle>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaComplianceOperationRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaComplianceOperationStatusResponse>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaMigrationStartRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaOperationRequest>>(),
            serializerProvider.GetRequiredService<Serializer<SchemaOperationStatusResponse>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationListRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationPage>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeSchemaGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeSchemaGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeSchemaGrpcMethods? Current { get; set; }
}
