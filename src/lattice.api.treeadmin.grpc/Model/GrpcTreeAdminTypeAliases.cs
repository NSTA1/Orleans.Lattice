namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.TreeAdmin.Grpc</c> binding adds on top of the
/// transport-agnostic tree-administration control facade DTOs. Grpc-binding
/// aliases use the <c>oitg.</c> prefix (Orleans Lattice Api TreeAdmin Grpc) to
/// avoid collision with the tree-administration control-API facade (<c>oit.</c>),
/// the core (<c>ol.</c>), the schema engine (<c>ols.</c>), the schema control-API
/// (<c>ois.</c>), and the schema-API gRPC binding (<c>oisg.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcTreeAdminTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the tree-administration gRPC binding.
    /// Every alias constant added here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oitg.";

    /// <summary>Alias for <see cref="TreeAdminTreeRequest"/>.</summary>
    public const string TreeAdminTreeRequest = "oitg.treereq";

    /// <summary>Alias for <see cref="TreeAdminShardRequest"/>.</summary>
    public const string TreeAdminShardRequest = "oitg.shardreq";

    /// <summary>Alias for <see cref="TreeAdminDiagnosticsRequest"/>.</summary>
    public const string TreeAdminDiagnosticsRequest = "oitg.diagreq";

    /// <summary>Alias for <see cref="TreeAdminStorageUsageRequest"/>.</summary>
    public const string TreeAdminStorageUsageRequest = "oitg.storeq";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oitg.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oitg.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oitg.asadv";

    /// <summary>Alias for <see cref="TreeAdminCreateRequest"/>.</summary>
    public const string TreeAdminCreateRequest = "oitg.createq";

    /// <summary>Alias for <see cref="TreeAdminSetAliasRequest"/>.</summary>
    public const string TreeAdminSetAliasRequest = "oitg.aliasreq";

    /// <summary>Alias for <see cref="TreeAdminSetConfigRequest"/>.</summary>
    public const string TreeAdminSetConfigRequest = "oitg.cfgreq";

    /// <summary>Alias for <see cref="TreeAdminPurgeRequest"/>.</summary>
    public const string TreeAdminPurgeRequest = "oitg.purgreq";

    /// <summary>Alias for <see cref="TreeAdminBulkLoadSessionRequest"/>.</summary>
    public const string TreeAdminBulkLoadSessionRequest = "oitg.blsreq";

    /// <summary>Alias for <see cref="TreeAdminBulkLoadAppendRequest"/>.</summary>
    public const string TreeAdminBulkLoadAppendRequest = "oitg.blareq";

    /// <summary>Alias for <see cref="TreeAdminRestoreRequest"/>.</summary>
    public const string TreeAdminRestoreRequest = "oitg.rstreq";

    /// <summary>Alias for <see cref="TreeAdminRestoreSetRequest"/>.</summary>
    public const string TreeAdminRestoreSetRequest = "oitg.rssreq";

    /// <summary>Alias for <see cref="TreeAdminReshardRequest"/>.</summary>
    public const string TreeAdminReshardRequest = "oitg.rshreq";

    /// <summary>Alias for <see cref="TreeAdminResizeRequest"/>.</summary>
    public const string TreeAdminResizeRequest = "oitg.rszreq";
}
