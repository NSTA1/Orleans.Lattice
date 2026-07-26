using System.Collections.Frozen;
using Orleans.Lattice.Api.Region;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The <see cref="ILatticeApiMcpRegionRouter"/> built once at startup from a set
/// of <see cref="LatticeApiMcpRegionDefinition"/>s. Resolution is a frozen
/// dictionary lookup with no per-call allocation, and the descriptor snapshot is
/// materialised once, so both region discovery and per-call routing are cheap and
/// consistent. Used by both topologies: the in-silo binding builds it with the
/// single current region, the remote-host binding with the current region plus
/// each configured peer.
/// </summary>
internal sealed class LatticeApiMcpRegionRouter : ILatticeApiMcpRegionRouter
{
    private readonly FrozenDictionary<string, FrozenSet<LatticeApiMcpGroup>> _regionGroups;
    private readonly IReadOnlyList<LatticeRegionDescriptor> _snapshot;

    /// <summary>
    /// Builds the router from <paramref name="definitions"/> (current region
    /// first) with <paramref name="defaultRegionId"/> as the region a call targets
    /// when no <c>region</c> is supplied.
    /// </summary>
    /// <param name="defaultRegionId">The default (current) region id.</param>
    /// <param name="definitions">The regions the server can route to.</param>
    public LatticeApiMcpRegionRouter(
        string defaultRegionId,
        IReadOnlyList<LatticeApiMcpRegionDefinition> definitions)
    {
        ArgumentException.ThrowIfNullOrEmpty(defaultRegionId);
        ArgumentNullException.ThrowIfNull(definitions);

        DefaultRegionId = defaultRegionId;

        var regionGroups = new Dictionary<string, FrozenSet<LatticeApiMcpGroup>>(
            definitions.Count, StringComparer.Ordinal);
        var descriptors = new List<LatticeRegionDescriptor>(definitions.Count);

        for (var i = 0; i < definitions.Count; i++)
        {
            var definition = definitions[i];
            regionGroups[definition.RegionId] = definition.Groups.Keys.ToFrozenSet();
            descriptors.Add(BuildDescriptor(definition));
        }

        _regionGroups = regionGroups.ToFrozenDictionary(StringComparer.Ordinal);
        _snapshot = descriptors;
    }

    /// <inheritdoc />
    public string DefaultRegionId { get; }

    /// <inheritdoc />
    public LatticeApiMcpRegionRoute Resolve(string? requestedRegionId, LatticeApiMcpGroup group)
    {
        // Omitted selector: the default-region path, byte-for-byte unchanged and
        // never group-checked so an existing call behaves exactly as before.
        if (string.IsNullOrWhiteSpace(requestedRegionId))
        {
            return LatticeApiMcpRegionRoute.Default(DefaultRegionId);
        }

        var requested = requestedRegionId.Trim();

        // Explicit current-region target: routes to the default channel. Not
        // group-checked, for the same reason as the omitted path - the tool would
        // not have been advertised in the current region if its group were absent.
        if (string.Equals(requested, DefaultRegionId, StringComparison.Ordinal))
        {
            return LatticeApiMcpRegionRoute.Default(DefaultRegionId);
        }

        if (!_regionGroups.TryGetValue(requested, out var groups))
        {
            return LatticeApiMcpRegionRoute.Rejected(
                $"Unknown region '{requested}'. Call lattice_list_regions to discover the regions this "
                + "server can route to.");
        }

        if (!groups.Contains(group))
        {
            return LatticeApiMcpRegionRoute.Rejected(
                $"Region '{requested}' does not serve the '{LatticeApiMcpGroupCapabilityMap.DisplayName(group)}' "
                + "group. Call lattice_list_regions for each region's per-group availability.");
        }

        return LatticeApiMcpRegionRoute.ToRegion(requested);
    }

    /// <inheritdoc />
    public IReadOnlyList<LatticeRegionDescriptor> Snapshot() => _snapshot;

    private static LatticeRegionDescriptor BuildDescriptor(LatticeApiMcpRegionDefinition definition)
    {
        var allGroups = LatticeApiMcpGroupCapabilityMap.AllGroups;
        var reachability = new LatticeRegionGroupReachability[allGroups.Count];
        for (var i = 0; i < allGroups.Count; i++)
        {
            var group = allGroups[i];
            var available = definition.Groups.TryGetValue(group, out var endpoint);
            reachability[i] = new LatticeRegionGroupReachability
            {
                Group = LatticeApiMcpGroupCapabilityMap.DisplayName(group),
                Available = available,
                Endpoint = available ? endpoint : null,
            };
        }

        return new LatticeRegionDescriptor
        {
            RegionId = definition.RegionId,
            ClusterId = definition.ClusterId,
            IsCurrent = definition.IsCurrent,
            Groups = reachability,
        };
    }
}
