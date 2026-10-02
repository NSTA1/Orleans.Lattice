namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Centralized Orleans serialization alias constants for the wire messages the
/// <c>Orleans.Lattice.Api.TenantAdmin.Grpc</c> binding adds on top of the
/// transport-agnostic tenant-administration control facade DTOs. Grpc-binding
/// aliases use the <c>oitng.</c> prefix (Orleans Lattice Api TenantAdmin Grpc) to
/// avoid collision with the tenant-administration control-API facade
/// (<c>oitn.</c>), the tree-administration control-API facade (<c>oit.</c>), its
/// gRPC binding (<c>oitg.</c>), and the core (<c>ol.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class GrpcTenantAdminTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the tenant-administration gRPC binding.
    /// Every alias constant added here starts with this value.
    /// </summary>
    public const string AliasPrefix = "oitng.";

    /// <summary>Alias for <see cref="TenantAdminTenantRequest"/>.</summary>
    public const string TenantAdminTenantRequest = "oitng.tenreq";

    /// <summary>Alias for <see cref="TenantAdminCreateRequest"/>.</summary>
    public const string TenantAdminCreateRequest = "oitng.crtreq";

    /// <summary>Alias for <see cref="TenantAdminSetQuotasRequest"/>.</summary>
    public const string TenantAdminSetQuotasRequest = "oitng.setqreq";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisementRequest"/>.</summary>
    public const string AuthSchemeAdvertisementRequest = "oitng.asreq";

    /// <summary>Alias for <see cref="AuthSchemeDescriptor"/>.</summary>
    public const string AuthSchemeDescriptor = "oitng.asdesc";

    /// <summary>Alias for <see cref="AuthSchemeAdvertisement"/>.</summary>
    public const string AuthSchemeAdvertisement = "oitng.asadv";

    /// <summary>Alias for <see cref="TenantSelfCurrentRequest"/>.</summary>
    public const string TenantSelfCurrentRequest = "oitng.selfcur";

    /// <summary>Alias for <see cref="TenantSelfListRequest"/>.</summary>
    public const string TenantSelfListRequest = "oitng.selflist";

    /// <summary>Alias for <see cref="TenantSelfDescriptorList"/>.</summary>
    public const string TenantSelfDescriptorList = "oitng.selftdl";

    /// <summary>Alias for <see cref="TenantAdminRegionSetRequest"/>.</summary>
    public const string TenantAdminRegionSetRequest = "oitng.rgnset";

    /// <summary>Alias for <see cref="TenantAdminSubjectRequest"/>.</summary>
    public const string TenantAdminSubjectRequest = "oitng.subjreq";

    /// <summary>Alias for <see cref="TenantAdminGrantRequest"/>.</summary>
    public const string TenantAdminGrantRequest = "oitng.grntreq";

    /// <summary>Alias for <see cref="TenantAdminGrantOfferRequest"/>.</summary>
    public const string TenantAdminGrantOfferRequest = "oitng.grntoff";

    /// <summary>Alias for <see cref="TenantAdminAccessListRequest"/>.</summary>
    public const string TenantAdminAccessListRequest = "oitng.acclist";

    /// <summary>Alias for <see cref="TenantAdminGroupRequest"/>.</summary>
    public const string TenantAdminGroupRequest = "oitng.grpreq";

    /// <summary>Alias for <see cref="TenantAdminGroupUpsertRequest"/>.</summary>
    public const string TenantAdminGroupUpsertRequest = "oitng.grpups";

    /// <summary>Alias for <see cref="TenantAdminGroupMemberRequest"/>.</summary>
    public const string TenantAdminGroupMemberRequest = "oitng.grpmem";

    /// <summary>Alias for <see cref="TenantAdminMemberRequest"/>.</summary>
    public const string TenantAdminMemberRequest = "oitng.mbrreq";

    /// <summary>Alias for <see cref="TenantAdminRulePutRequest"/>.</summary>
    public const string TenantAdminRulePutRequest = "oitng.ruleput";

    /// <summary>Alias for <see cref="TenantAdminRuleRequest"/>.</summary>
    public const string TenantAdminRuleRequest = "oitng.rulereq";

    /// <summary>Alias for <see cref="TenantAdminExplainRequest"/>.</summary>
    public const string TenantAdminExplainRequest = "oitng.explreq";

    /// <summary>Alias for <see cref="TenantAdminEffectivePermissionsRequest"/>.</summary>
    public const string TenantAdminEffectivePermissionsRequest = "oitng.effreq";

    /// <summary>Alias for <see cref="TenantAdminGroupLookup"/>.</summary>
    public const string TenantAdminGroupLookup = "oitng.grplkp";

    /// <summary>Alias for <see cref="TenantAdminGroupMemberList"/>.</summary>
    public const string TenantAdminGroupMemberList = "oitng.grpmlst";

    /// <summary>Alias for <see cref="TenantAdminRuleLookup"/>.</summary>
    public const string TenantAdminRuleLookup = "oitng.rulelkp";

    /// <summary>Alias for <see cref="TenantAdminRuleRemoval"/>.</summary>
    public const string TenantAdminRuleRemoval = "oitng.rulerm";
}
