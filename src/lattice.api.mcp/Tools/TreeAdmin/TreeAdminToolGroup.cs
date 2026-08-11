using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The tree-administration tool module: an <see cref="ILatticeApiMcpToolGroup"/>
/// for <see cref="LatticeApiMcpGroup.TreeAdmin"/> whose tools are thin adapters
/// over the <see cref="Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin"/> facade.
/// </summary>
/// <remarks>
/// <para>
/// This is the scaffolding foundation for the tree-administration control plane.
/// The group is <b>discoverable but empty</b>: it contributes no tools yet, so an
/// administrator-granted caller sees the group advertised in
/// <c>lattice_capabilities</c> (proving the wiring is complete end to end) but is
/// offered no tree-administration verbs. The whole-tree lifecycle tools
/// (bulk-load, delete, resize, reshard, and the rest) land in later work, each
/// appended to <see cref="Tools"/> as a thin adapter that stamps the caller
/// credential onto the ambient <see cref="LatticeCredentialContext"/> and defers
/// to the facade's own fail-closed access gate.
/// </para>
/// <para>
/// The group is advertised only to a caller whose effective permissions grant
/// <see cref="LatticeOperation.Admin"/> - an agent without the administrator grant
/// is offered no tree-administration group at all.
/// </para>
/// </remarks>
internal sealed class TreeAdminToolGroup : ILatticeApiMcpToolGroup
{
    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.TreeAdmin;

    /// <inheritdoc />
    /// <remarks>
    /// Empty at this scaffolding stage: the group is discoverable but ships no
    /// operations yet. Built once (an empty, shared, immutable list) so the
    /// per-session filtering path never re-materialises it.
    /// </remarks>
    public IReadOnlyList<McpServerTool> Tools { get; } = Array.Empty<McpServerTool>();
}
