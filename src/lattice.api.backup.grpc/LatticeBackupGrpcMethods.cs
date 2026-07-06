using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Backup;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// backup control API. Each method is a unary or server-streaming RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder
/// wire up identical marshallers.
/// </summary>
/// <remarks>
/// The contract is a flat set of RPCs over the transport-agnostic
/// <see cref="Orleans.Lattice.Api.Backup.ILatticeBackupControl"/> facade:
/// capture (<c>CreateBackup</c> / <c>CreateIncrementalBackup</c>), catalog
/// (<c>ListBackups</c> unary + <c>StreamBackups</c> server-streaming), chain
/// inspection (<c>DescribeBackup</c>), lifecycle (<c>DeleteBackup</c> /
/// <c>RestoreBackup</c> / <c>RevertRestore</c>), artifact export
/// (<c>ExportArtifact</c> server-streaming), and unauthenticated discovery
/// (<c>GetAuthScheme</c>). Contract-versioning policy: fields on the wire
/// messages are additive-only (new <c>[Id(n)]</c>); aliases and field numbers
/// are never renumbered, so a newer response decodes cleanly under an older
/// client.
/// </remarks>
internal sealed class LatticeBackupGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.backup";

    /// <summary>The unary full-capture RPC method name.</summary>
    public const string CreateBackupMethodName = "CreateBackup";

    /// <summary>The unary incremental-capture RPC method name.</summary>
    public const string CreateIncrementalBackupMethodName = "CreateIncrementalBackup";

    /// <summary>The unary cursor-resumable catalog-listing RPC method name.</summary>
    public const string ListBackupsMethodName = "ListBackups";

    /// <summary>The server-streaming whole-catalog drain RPC method name.</summary>
    public const string StreamBackupsMethodName = "StreamBackups";

    /// <summary>The unary describe-chain RPC method name.</summary>
    public const string DescribeBackupMethodName = "DescribeBackup";

    /// <summary>The unary delete-backup RPC method name.</summary>
    public const string DeleteBackupMethodName = "DeleteBackup";

    /// <summary>The unary restore RPC method name.</summary>
    public const string RestoreBackupMethodName = "RestoreBackup";

    /// <summary>The unary revert-restore RPC method name.</summary>
    public const string RevertRestoreMethodName = "RevertRestore";

    /// <summary>The server-streaming artifact-export RPC method name.</summary>
    public const string ExportArtifactMethodName = "ExportArtifact";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeBackupGrpcMethods(
        Serializer<BackupCaptureRequestMessage> captureRequestSerializer,
        Serializer<BackupIncrementalCaptureRequestMessage> incrementalCaptureRequestSerializer,
        Serializer<BackupCaptureResponse> captureResponseSerializer,
        Serializer<Orleans.Lattice.Api.Backup.BackupCatalogRequest> catalogRequestSerializer,
        Serializer<Orleans.Lattice.Api.Backup.BackupCatalogPage> catalogPageSerializer,
        Serializer<BackupStreamRequest> streamRequestSerializer,
        Serializer<BackupManifest> manifestSerializer,
        Serializer<BackupDescribeRequest> describeRequestSerializer,
        Serializer<BackupChainResponse> chainResponseSerializer,
        Serializer<BackupDeleteRequest> deleteRequestSerializer,
        Serializer<BackupDeleteResponse> deleteResponseSerializer,
        Serializer<RestoreRequestMessage> restoreRequestSerializer,
        Serializer<RestoreResponse> restoreResponseSerializer,
        Serializer<RevertRestoreResponse> revertResponseSerializer,
        Serializer<ArtifactExportRequest> artifactExportRequestSerializer,
        Serializer<ArtifactChunk> artifactChunkSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer)
    {
        ArgumentNullException.ThrowIfNull(captureRequestSerializer);
        ArgumentNullException.ThrowIfNull(incrementalCaptureRequestSerializer);
        ArgumentNullException.ThrowIfNull(captureResponseSerializer);
        ArgumentNullException.ThrowIfNull(catalogRequestSerializer);
        ArgumentNullException.ThrowIfNull(catalogPageSerializer);
        ArgumentNullException.ThrowIfNull(streamRequestSerializer);
        ArgumentNullException.ThrowIfNull(manifestSerializer);
        ArgumentNullException.ThrowIfNull(describeRequestSerializer);
        ArgumentNullException.ThrowIfNull(chainResponseSerializer);
        ArgumentNullException.ThrowIfNull(deleteRequestSerializer);
        ArgumentNullException.ThrowIfNull(deleteResponseSerializer);
        ArgumentNullException.ThrowIfNull(restoreRequestSerializer);
        ArgumentNullException.ThrowIfNull(restoreResponseSerializer);
        ArgumentNullException.ThrowIfNull(revertResponseSerializer);
        ArgumentNullException.ThrowIfNull(artifactExportRequestSerializer);
        ArgumentNullException.ThrowIfNull(artifactChunkSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);

        CreateBackup = new Method<BackupCaptureRequestMessage, BackupCaptureResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateBackupMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(captureRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(captureResponseSerializer));

        CreateIncrementalBackup = new Method<BackupIncrementalCaptureRequestMessage, BackupCaptureResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateIncrementalBackupMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(incrementalCaptureRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(captureResponseSerializer));

        ListBackups = new Method<Orleans.Lattice.Api.Backup.BackupCatalogRequest, Orleans.Lattice.Api.Backup.BackupCatalogPage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListBackupsMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(catalogRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(catalogPageSerializer));

        StreamBackups = new Method<BackupStreamRequest, BackupManifest>(
            type: MethodType.ServerStreaming,
            serviceName: ServiceName,
            name: StreamBackupsMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(streamRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(manifestSerializer));

        DescribeBackup = new Method<BackupDescribeRequest, BackupChainResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DescribeBackupMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(describeRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(chainResponseSerializer));

        DeleteBackup = new Method<BackupDeleteRequest, BackupDeleteResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DeleteBackupMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(deleteRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(deleteResponseSerializer));

        RestoreBackup = new Method<RestoreRequestMessage, RestoreResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RestoreBackupMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(restoreRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(restoreResponseSerializer));

        RevertRestore = new Method<RestoreResponse, RevertRestoreResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RevertRestoreMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(restoreResponseSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(revertResponseSerializer));

        ExportArtifact = new Method<ArtifactExportRequest, ArtifactChunk>(
            type: MethodType.ServerStreaming,
            serviceName: ServiceName,
            name: ExportArtifactMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(artifactExportRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(artifactChunkSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeBackupGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeBackupGrpcMarshallers.Create(authSchemeAdvertisementSerializer));
    }

    /// <summary>The unary <c>CreateBackup</c> full-capture RPC.</summary>
    public Method<BackupCaptureRequestMessage, BackupCaptureResponse> CreateBackup { get; }

    /// <summary>The unary <c>CreateIncrementalBackup</c> incremental-capture RPC.</summary>
    public Method<BackupIncrementalCaptureRequestMessage, BackupCaptureResponse> CreateIncrementalBackup { get; }

    /// <summary>The unary <c>ListBackups</c> cursor-resumable catalog RPC.</summary>
    public Method<Orleans.Lattice.Api.Backup.BackupCatalogRequest, Orleans.Lattice.Api.Backup.BackupCatalogPage> ListBackups { get; }

    /// <summary>The server-streaming <c>StreamBackups</c> whole-catalog drain RPC.</summary>
    public Method<BackupStreamRequest, BackupManifest> StreamBackups { get; }

    /// <summary>The unary <c>DescribeBackup</c> chain-inspection RPC.</summary>
    public Method<BackupDescribeRequest, BackupChainResponse> DescribeBackup { get; }

    /// <summary>The unary <c>DeleteBackup</c> RPC.</summary>
    public Method<BackupDeleteRequest, BackupDeleteResponse> DeleteBackup { get; }

    /// <summary>The unary <c>RestoreBackup</c> RPC.</summary>
    public Method<RestoreRequestMessage, RestoreResponse> RestoreBackup { get; }

    /// <summary>The unary <c>RevertRestore</c> RPC.</summary>
    public Method<RestoreResponse, RevertRestoreResponse> RevertRestore { get; }

    /// <summary>The server-streaming <c>ExportArtifact</c> RPC.</summary>
    public Method<ArtifactExportRequest, ArtifactChunk> ExportArtifact { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out
    /// of <paramref name="serializerProvider"/>. Shared by the server-side DI
    /// factory and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeBackupGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeBackupGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<BackupCaptureRequestMessage>>(),
            serializerProvider.GetRequiredService<Serializer<BackupIncrementalCaptureRequestMessage>>(),
            serializerProvider.GetRequiredService<Serializer<BackupCaptureResponse>>(),
            serializerProvider.GetRequiredService<Serializer<Orleans.Lattice.Api.Backup.BackupCatalogRequest>>(),
            serializerProvider.GetRequiredService<Serializer<Orleans.Lattice.Api.Backup.BackupCatalogPage>>(),
            serializerProvider.GetRequiredService<Serializer<BackupStreamRequest>>(),
            serializerProvider.GetRequiredService<Serializer<BackupManifest>>(),
            serializerProvider.GetRequiredService<Serializer<BackupDescribeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<BackupChainResponse>>(),
            serializerProvider.GetRequiredService<Serializer<BackupDeleteRequest>>(),
            serializerProvider.GetRequiredService<Serializer<BackupDeleteResponse>>(),
            serializerProvider.GetRequiredService<Serializer<RestoreRequestMessage>>(),
            serializerProvider.GetRequiredService<Serializer<RestoreResponse>>(),
            serializerProvider.GetRequiredService<Serializer<RevertRestoreResponse>>(),
            serializerProvider.GetRequiredService<Serializer<ArtifactExportRequest>>(),
            serializerProvider.GetRequiredService<Serializer<ArtifactChunk>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeBackupGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeBackupGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeBackupGrpcMethods? Current { get; set; }
}
