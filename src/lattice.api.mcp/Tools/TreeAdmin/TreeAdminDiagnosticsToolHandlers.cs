using System.ComponentModel;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods the tree-administration tool module exposes as MCP
/// read-only diagnostics tools. Every method is a stateless, static shim over the
/// transport-agnostic <see cref="ILatticeTreeAdmin"/> facade: it resolves the facade
/// from the tool invocation's request service provider (bound by the MCP SDK from
/// <c>RequestContext.Services</c>), marshals the tool-call arguments into the
/// facade's parameters, and returns the facade result verbatim. No authorization,
/// read, or diagnostics logic lives here - the facade owns it, and its fail-closed
/// access gate (whole-tree read for the per-tree verbs, cluster telemetry for the
/// storage summary) refuses an unauthorized caller even if one somehow reaches an
/// invocation.
/// </summary>
/// <remarks>
/// Every operation is advertised read-only. The methods are held as static method
/// groups so the tool module materialises each tool's delegate exactly once when it
/// builds its tool list, never per <c>tools/call</c>. The facade DTOs are reused
/// verbatim as the tool result shapes, so this surface adds no new serializable wire
/// type.
/// </remarks>
internal static class TreeAdminDiagnosticsToolHandlers
{
    /// <summary>Reads a per-shard read/write hotness report for a tree.</summary>
    public static Task<TreeHotnessReport> GetShardHotnessAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to sample the per-shard read/write hotness of. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetShardHotnessAsync(treeId, cancellationToken);
    }

    /// <summary>Reads a whole-tree diagnostic report, optionally walking leaf state.</summary>
    public static Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to diagnose. Must not be null or empty.")]
        string treeId,
        [Description("When true, walk leaf state for authoritative counts (more expensive); when false (the default), use the cheap shard-root projection.")]
        bool deep = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetDiagnosticsAsync(treeId, deep, cancellationToken);
    }

    /// <summary>Inspects a tree's shard-map topology.</summary>
    public static Task<ShardMapInspection> InspectShardMapAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree whose shard-map topology to inspect. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.InspectShardMapAsync(treeId, cancellationToken);
    }

    /// <summary>Reads a single shard's leaf-projection digest.</summary>
    public static Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree the shard belongs to. Must not be null or empty.")]
        string treeId,
        [Description("The zero-based physical shard index whose leaf-projection digest to read. Must not be negative.")]
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetProjectionDigestAsync(treeId, shardIndex, cancellationToken);
    }

    /// <summary>Reads a tree's rolled-up statistics snapshot.</summary>
    public static Task<TreeStatsReport> GetTreeStatsAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to summarize topology, live-key counts, and storage bytes for. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetTreeStatsAsync(treeId, cancellationToken);
    }

    /// <summary>Reads the cluster-wide storage accounting summary.</summary>
    public static Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("When true, force an expensive fresh leaf-walk that re-measures every shard; when false (the default), return the cheap cached WAL-poll aggregate.")]
        bool deep = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetStorageUsageAsync(deep, cancellationToken);
    }
}
