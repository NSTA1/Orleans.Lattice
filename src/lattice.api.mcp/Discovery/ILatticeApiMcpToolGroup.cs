using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The registration seam a per-facade tool module plugs into so the
/// permission-aware discovery core can advertise the module's tools per session.
/// Each module (state, data, backup, auth - added by later work) registers one
/// implementation into DI declaring which <see cref="LatticeApiMcpGroup"/> it
/// serves and the <see cref="McpServerTool"/> instances it contributes. The
/// discovery core consults every registered group when a session initialises,
/// includes a group's <see cref="Tools"/> only when the caller's effective
/// permissions grant an operation the group covers, and otherwise omits the
/// group entirely so an unauthorized caller can neither see nor invoke its
/// tools.
/// </summary>
/// <remarks>
/// Implementations build their <see cref="Tools"/> once (the tools are stateless
/// adapters that resolve their collaborators from the request service provider)
/// so the per-session filtering path selects from prebuilt lists and never
/// re-materialises a tool per <c>tools/list</c>.
/// </remarks>
internal interface ILatticeApiMcpToolGroup
{
    /// <summary>The facade group this module serves.</summary>
    LatticeApiMcpGroup Group { get; }

    /// <summary>
    /// The tools this module contributes, built once. Advertised to a caller
    /// only when the caller may use <see cref="Group"/>.
    /// </summary>
    IReadOnlyList<McpServerTool> Tools { get; }
}
