using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The delegated tenant-access facades the Access area's tenant pages read: the
/// tenant directory (groups, group members, the tenant member set) and the tenant
/// policy (tenant-tier rules, explain, the posture probe). Either may be absent,
/// and an absent facade is a fail-closed "no": the tenant pages fall back to the
/// cluster-wide behaviour they had before delegated administration existed.
/// </summary>
/// <remarks>
/// The area binds to this seam rather than to the transport, so its pages are
/// tested against the contract fakes and the transports are supplied separately.
/// </remarks>
internal interface ITenantAccessFacades
{
    /// <summary>The tenant directory facade, or <see langword="null"/> when the head serves none.</summary>
    ILatticeTenantDirectoryAdmin? Directory { get; }

    /// <summary>The tenant policy facade, or <see langword="null"/> when the head serves none.</summary>
    ILatticeTenantPolicyAdmin? Policy { get; }
}
