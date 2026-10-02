using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The handler behind <c>lattice_treeadmin_wal_reclamation</c> (#4237): delegates to
/// <see cref="ILatticeWalReclamation.GetWalReclamationAsync"/> and projects its
/// report onto <see cref="McpTreeWalReclamation"/>. The facade is resolved from the
/// request service provider at call time rather than bound as a tool parameter, so a
/// host that registers no WAL reclamation read fails the call with a clear message
/// instead of advertising the facade as an argument. The facade owns the
/// authorization decision; this handler adds none.
/// </summary>
internal static class TreeAdminWalReclamationToolHandlers
{
    /// <summary>The tool name.</summary>
    public const string ToolName = "lattice_treeadmin_wal_reclamation";

    /// <summary>Reads which durable pin holds a tree's WAL floor and whether it has wedged reclamation.</summary>
    /// <param name="context">The request context the facade is resolved from.</param>
    /// <param name="treeId">The tree to inspect.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The projected reclamation report.</returns>
    /// <exception cref="InvalidOperationException">The host registers no <see cref="ILatticeWalReclamation"/>.</exception>
    public static async Task<McpTreeWalReclamation> GetWalReclamationAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The tree whose WAL reclamation to inspect. Must not be null or empty. An aliased tree is resolved to its physical tree.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        var report = await Facade(context).GetWalReclamationAsync(treeId, cancellationToken).ConfigureAwait(false);
        return ToMcp(report);
    }

    /// <summary>Projects a facade report onto the MCP result.</summary>
    /// <param name="report">The facade report.</param>
    /// <returns>The MCP result.</returns>
    internal static McpTreeWalReclamation ToMcp(TreeWalReclamationReport report) => new()
    {
        TreeId = report.TreeId,
        PinStoreReadable = report.PinStoreReadable,
        PinCount = report.PinCount,
        PinsWithoutOffset = report.PinsWithoutOffset,
        IsWedged = report.IsWedged,
        FloorHolder = report.FloorHolder is { } holder
            ? new McpTreeWalFloorHolder
            {
                ConsumerId = holder.ConsumerId,
                LeafId = holder.LeafId,
                Partition = holder.Partition,
                PinOffset = holder.PinOffset,
                PersistedCheckpoint = holder.PersistedCheckpoint,
                State = holder.State.ToString(),
                HoldsOffsetFloor = holder.HoldsOffsetFloor,
            }
            : null,
    };

    private static ILatticeWalReclamation Facade(RequestContext<CallToolRequestParams> context) =>
        context.Services?.GetService<ILatticeWalReclamation>()
        ?? throw new InvalidOperationException(
            "This server registers no ILatticeWalReclamation, so the WAL reclamation read is unavailable.");
}
