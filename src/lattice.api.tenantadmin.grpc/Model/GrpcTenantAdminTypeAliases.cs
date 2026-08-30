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
}
