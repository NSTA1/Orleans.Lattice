using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The telemetry tool module: the <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.Telemetry"/>. It plugs the companion telemetry
/// package into the MCP binding's permission-aware discovery core so a caller
/// holding a <c>LatticeOperation.Telemetry</c> grant is offered the telemetry
/// tools.
/// </summary>
/// <remarks>
/// C1 is the package skeleton: this group contributes <b>no</b> tools yet. Phase
/// D materialises the metric-query tools into <see cref="Tools"/>. The group is
/// built once (its <see cref="Tools"/> list is the empty singleton), so the
/// per-session discovery filter selects from a prebuilt list and never
/// re-materialises the group.
/// </remarks>
internal sealed class TelemetryToolGroup : ILatticeApiMcpToolGroup
{
    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Telemetry;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; } = Array.Empty<McpServerTool>();
}
