using Orleans.Lattice.Api.Region;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The structured result of the <c>lattice_list_regions</c> discovery tool: the
/// regions the server can route to (the current cluster plus any reachable,
/// credentialed peer), each with per-group reachability, and the id of the
/// current region a call targets when no <c>region</c> selector is supplied.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes. It is a thin
/// projection of the transport-agnostic <see cref="LatticeRegionDescriptor"/>
/// contract, so a client reads the same region model the shared facade contract
/// exposes.
/// </remarks>
public sealed record LatticeApiMcpRegionsToolResult
{
    /// <summary>
    /// The id of the current (default) region - the one a tool call targets when
    /// its optional <c>region</c> selector is omitted.
    /// </summary>
    public required string CurrentRegion { get; init; }

    /// <summary>
    /// The regions the server can route to, current region first, each with its
    /// per-group reachability. A region with no route is omitted (fail-closed).
    /// </summary>
    public IReadOnlyList<LatticeRegionDescriptor> Regions { get; init; }
        = Array.Empty<LatticeRegionDescriptor>();
}
