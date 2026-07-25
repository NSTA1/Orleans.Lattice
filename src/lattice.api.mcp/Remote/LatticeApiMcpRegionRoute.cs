namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The outcome of resolving an optional per-call <c>region</c> selector against
/// the <see cref="ILatticeApiMcpRegionRouter"/> for a specific facade group.
/// Either a valid route (<see cref="Fault"/> is <see langword="null"/>) naming the
/// region that will serve the call, or a fail-closed rejection carrying an
/// actionable, non-leaking fault message.
/// </summary>
internal readonly record struct LatticeApiMcpRegionRoute
{
    private LatticeApiMcpRegionRoute(bool isDefault, string servedRegionId, string? fault)
    {
        IsDefault = isDefault;
        ServedRegionId = servedRegionId;
        Fault = fault;
    }

    /// <summary>
    /// <see langword="true"/> when the call targets the default (current) region -
    /// either because no <c>region</c> was supplied or the supplied value names the
    /// default region - so the routing hot path stays on its zero-overhead branch.
    /// </summary>
    public bool IsDefault { get; }

    /// <summary>The region id that will serve the call. Empty when <see cref="Fault"/> is set.</summary>
    public string ServedRegionId { get; }

    /// <summary>
    /// The fail-closed rejection message when the requested region is unknown or
    /// does not serve the group; <see langword="null"/> on a valid route.
    /// </summary>
    public string? Fault { get; }

    /// <summary>Whether the route is valid (no fault).</summary>
    public bool IsRouted => Fault is null;

    /// <summary>Builds a valid route to the default (current) region.</summary>
    public static LatticeApiMcpRegionRoute Default(string regionId)
        => new(isDefault: true, regionId, fault: null);

    /// <summary>Builds a valid route to an explicitly named peer region.</summary>
    public static LatticeApiMcpRegionRoute ToRegion(string regionId)
        => new(isDefault: false, regionId, fault: null);

    /// <summary>Builds a fail-closed rejection carrying <paramref name="fault"/>.</summary>
    public static LatticeApiMcpRegionRoute Rejected(string fault)
        => new(isDefault: false, servedRegionId: string.Empty, fault);
}
