namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.Backup.Grpc</c> binding adds on top of the
/// transport-agnostic backup control facade DTOs. Grpc-binding aliases use the
/// <c>oibg.</c> prefix (Orleans Lattice Api Backup Grpc) to avoid collision with
/// the backup control-API facade (<c>oib.</c>), the core (<c>ol.</c>), the
/// backup engine (<c>olb.</c>), and the state-API gRPC binding
/// (<c>olag.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcBackupTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the backup gRPC binding. Every alias
    /// constant added here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oibg.";

    /// <summary>Alias for <see cref="BackupCaptureRequestMessage"/>.</summary>
    public const string BackupCaptureRequestMessage = "oibg.capreq";

    /// <summary>Alias for <see cref="BackupIncrementalCaptureRequestMessage"/>.</summary>
    public const string BackupIncrementalCaptureRequestMessage = "oibg.icapreq";

    /// <summary>Alias for <see cref="BackupSetCaptureRequestMessage"/>.</summary>
    public const string BackupSetCaptureRequestMessage = "oibg.scapreq";

    /// <summary>Alias for <see cref="BackupCaptureResponse"/>.</summary>
    public const string BackupCaptureResponse = "oibg.capresp";

    /// <summary>Alias for <see cref="BackupSetCaptureResponse"/>.</summary>
    public const string BackupSetCaptureResponse = "oibg.scapresp";

    /// <summary>Alias for <see cref="BackupStreamRequest"/>.</summary>
    public const string BackupStreamRequest = "oibg.streq";

    /// <summary>Alias for <see cref="BackupDescribeRequest"/>.</summary>
    public const string BackupDescribeRequest = "oibg.dreq";

    /// <summary>Alias for <see cref="BackupChainResponse"/>.</summary>
    public const string BackupChainResponse = "oibg.chresp";

    /// <summary>Alias for <see cref="BackupDeleteRequest"/>.</summary>
    public const string BackupDeleteRequest = "oibg.delreq";

    /// <summary>Alias for <see cref="BackupDeleteResponse"/>.</summary>
    public const string BackupDeleteResponse = "oibg.delresp";

    /// <summary>Alias for <see cref="RestoreRequestMessage"/>.</summary>
    public const string RestoreRequestMessage = "oibg.rreq";

    /// <summary>Alias for <see cref="RestoreResponse"/>.</summary>
    public const string RestoreResponse = "oibg.rresp";

    /// <summary>Alias for <see cref="RevertRestoreResponse"/>.</summary>
    public const string RevertRestoreResponse = "oibg.rvresp";

    /// <summary>Alias for <see cref="ArtifactExportRequest"/>.</summary>
    public const string ArtifactExportRequest = "oibg.aereq";

    /// <summary>Alias for <see cref="ArtifactChunk"/>.</summary>
    public const string ArtifactChunk = "oibg.achunk";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oibg.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oibg.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oibg.asadv";

    /// <summary>Alias for <see cref="BackupCapabilityProbeRequest"/>.</summary>
    public const string BackupCapabilityProbeRequest = "oibg.cpreq";

    /// <summary>Alias for <see cref="BackupScheduleRequestMessage"/>.</summary>
    public const string BackupScheduleRequestMessage = "oibg.schreq";

    /// <summary>Alias for <see cref="BackupScheduleResponse"/>.</summary>
    public const string BackupScheduleResponse = "oibg.schresp";
}
