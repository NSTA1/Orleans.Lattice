namespace Orleans.Lattice.Api.Region;

/// <summary>
/// Centralised Orleans serialization alias constants for every
/// <c>Orleans.Lattice.Api.Region</c> type that participates in the wire format.
/// Each alias is a short, fixed string that gives a type a stable wire identity
/// independent of its CLR name. Region contract aliases use the <c>olrg.</c>
/// prefix (Orleans Lattice ReGion) to avoid collision with the state-API facade
/// (<c>ola.</c>), the data-API facade (<c>olad.</c>), the core (<c>ol.</c>), and
/// the replication (<c>olr.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire and
/// persisted format. New types append new constants.
/// </remarks>
public static class ApiRegionTypeAliases
{
    /// <summary>Alias for <see cref="LatticeRegionGroupReachability"/>.</summary>
    public const string LatticeRegionGroupReachability = "olrg.gr";

    /// <summary>Alias for <see cref="LatticeRegionDescriptor"/>.</summary>
    public const string LatticeRegionDescriptor = "olrg.rd";

    /// <summary>Alias for <see cref="LatticeRegionTenantScope"/>.</summary>
    public const string LatticeRegionTenantScope = "olrg.ts";
}
