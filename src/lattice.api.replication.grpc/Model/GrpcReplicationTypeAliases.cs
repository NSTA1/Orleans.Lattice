namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.Replication.Grpc</c> binding adds on top of the
/// transport-agnostic replication control facade DTOs. Grpc-binding aliases use
/// the <c>oirg.</c> prefix (Orleans Lattice Api Replication Grpc) to avoid
/// collision with the replication control-API facade (<c>oir.</c>), the core
/// (<c>ol.</c>), the replication engine (<c>olr.</c>), and the backup gRPC
/// binding (<c>oibg.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcReplicationTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the replication gRPC binding. Every
    /// alias constant added here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oirg.";

    /// <summary>Alias for <see cref="ReplicationEnableRequestMessage"/>.</summary>
    public const string ReplicationEnableRequestMessage = "oirg.enreq";

    /// <summary>Alias for <see cref="ReplicationEnableResponse"/>.</summary>
    public const string ReplicationEnableResponse = "oirg.enresp";

    /// <summary>Alias for <see cref="ReplicationDisableRequestMessage"/>.</summary>
    public const string ReplicationDisableRequestMessage = "oirg.disreq";

    /// <summary>Alias for <see cref="ReplicationDisableResponse"/>.</summary>
    public const string ReplicationDisableResponse = "oirg.disresp";

    /// <summary>Alias for <see cref="ReplicationGetConfigRequest"/>.</summary>
    public const string ReplicationGetConfigRequest = "oirg.cfgreq";

    /// <summary>Alias for <see cref="ReplicationConfigResponse"/>.</summary>
    public const string ReplicationConfigResponse = "oirg.cfgresp";

    /// <summary>Alias for <see cref="ReplicationTreeConfigMessage"/>.</summary>
    public const string ReplicationTreeConfigMessage = "oirg.tree";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oirg.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oirg.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oirg.asadv";
}
