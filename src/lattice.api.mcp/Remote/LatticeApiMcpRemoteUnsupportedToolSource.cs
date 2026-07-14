namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The remote-host <see cref="ILatticeApiMcpUnsupportedToolSource"/>: the fixed
/// set of tools whose backing gRPC method is not yet bound, so they cannot be
/// invoked over the wire and are deferred (omitted) from a remote session's tool
/// set rather than listed-then-erroring. The set is a property of the remote gRPC
/// surface, not of per-group configuration, so it is static.
/// </summary>
/// <remarks>
/// The three <c>lattice_state_*</c> summaries have no gRPC method
/// (<c>GetTreeSummary</c> / <c>GetShardSummaries</c> / <c>GetPhysicalShardCount</c>)
/// and <c>lattice_backup_inventory</c> has no <c>GetInventory</c> binding. When
/// those gRPC methods are added, remove the corresponding name here and the tool
/// becomes discoverable remotely with no other change.
/// </remarks>
internal sealed class LatticeApiMcpRemoteUnsupportedToolSource : ILatticeApiMcpUnsupportedToolSource
{
    /// <summary>The <c>lattice_state_get_tree_summary</c> tool - no gRPC <c>GetTreeSummary</c> binding.</summary>
    public const string StateGetTreeSummary = "lattice_state_get_tree_summary";

    /// <summary>The <c>lattice_state_get_shard_summaries</c> tool - no gRPC <c>GetShardSummaries</c> binding.</summary>
    public const string StateGetShardSummaries = "lattice_state_get_shard_summaries";

    /// <summary>The <c>lattice_state_get_physical_shard_count</c> tool - no gRPC <c>GetPhysicalShardCount</c> binding.</summary>
    public const string StateGetPhysicalShardCount = "lattice_state_get_physical_shard_count";

    /// <summary>The <c>lattice_backup_inventory</c> tool - no gRPC <c>GetInventory</c> binding.</summary>
    public const string BackupInventory = "lattice_backup_inventory";

    private static readonly HashSet<string> Deferred = new(StringComparer.Ordinal)
    {
        StateGetTreeSummary,
        StateGetShardSummaries,
        StateGetPhysicalShardCount,
        BackupInventory,
    };

    /// <summary>The tool names deferred under the remote-host topology, in a stable order.</summary>
    public static IReadOnlyList<string> DeferredToolNames { get; } = new[]
    {
        StateGetTreeSummary,
        StateGetShardSummaries,
        StateGetPhysicalShardCount,
        BackupInventory,
    };

    /// <inheritdoc />
    public bool IsUnsupported(string toolName)
    {
        ArgumentNullException.ThrowIfNull(toolName);
        return Deferred.Contains(toolName);
    }
}
