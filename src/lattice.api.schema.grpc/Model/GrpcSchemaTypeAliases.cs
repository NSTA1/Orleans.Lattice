namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.Schema.Grpc</c> binding adds on top of the
/// transport-agnostic schema control facade DTOs. Grpc-binding aliases use the
/// <c>oisg.</c> prefix (Orleans Lattice Api Schema Grpc) to avoid collision with
/// the schema control-API facade (<c>ois.</c>), the core (<c>ol.</c>), the schema
/// engine (<c>ols.</c>), and the backup-API gRPC binding (<c>oibg.</c>) alias
/// namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcSchemaTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the schema gRPC binding. Every alias
    /// constant added here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oisg.";

    /// <summary>Alias for <see cref="SchemaTreeRequest"/>.</summary>
    public const string SchemaTreeRequest = "oisg.treereq";

    /// <summary>Alias for <see cref="SetPolicyRequest"/>.</summary>
    public const string SetPolicyRequest = "oisg.spreq";

    /// <summary>Alias for <see cref="SetVersionConfigRequest"/>.</summary>
    public const string SetVersionConfigRequest = "oisg.svcreq";

    /// <summary>Alias for <see cref="AdvanceVersionRequest"/>.</summary>
    public const string AdvanceVersionRequest = "oisg.avreq";

    /// <summary>Alias for <see cref="RemediateRequest"/>.</summary>
    public const string RemediateRequest = "oisg.rmreq";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oisg.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oisg.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oisg.asadv";

    /// <summary>Alias for <see cref="SchemaAckResponse"/>.</summary>
    public const string SchemaAckResponse = "oisg.ack";

    /// <summary>Alias for <see cref="SchemaRemovedResponse"/>.</summary>
    public const string SchemaRemovedResponse = "oisg.rmvresp";

    /// <summary>Alias for <see cref="GetPolicyResponse"/>.</summary>
    public const string GetPolicyResponse = "oisg.gpresp";

    /// <summary>Alias for <see cref="SchemaCountResponse"/>.</summary>
    public const string SchemaCountResponse = "oisg.cntresp";

    /// <summary>Alias for <see cref="GetVersionConfigResponse"/>.</summary>
    public const string GetVersionConfigResponse = "oisg.gvcresp";

    /// <summary>Alias for <see cref="VersionConfigResponse"/>.</summary>
    public const string VersionConfigResponse = "oisg.vcresp";

    /// <summary>Alias for <see cref="SchemaRemediationReportResponse"/>.</summary>
    public const string SchemaRemediationReportResponse = "oisg.rrresp";

    /// <summary>Alias for <see cref="SchemaComplianceReportResponse"/>.</summary>
    public const string SchemaComplianceReportResponse = "oisg.crresp";
}
