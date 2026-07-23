using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter layer between the replication MCP tools and the
/// <see cref="ILatticeReplicationControl"/> facade: one method per tool that maps
/// the tool's arguments onto a facade call and projects the facade result onto
/// the compact MCP DTO. These methods hold no transport or authorization concern
/// - the fail-closed replication access gate lives in the facade and the caller
/// credential is stamped on the ambient context by the tool delegate before the
/// method runs - so they are directly unit-testable against a fake facade.
/// </summary>
internal static class ReplicationToolInvocations
{
    /// <summary>
    /// Reports the runtime replicated-tree set the caller is authorized to see.
    /// </summary>
    public static async Task<McpReplicationConfig> GetReplicationConfigAsync(
        ILatticeReplicationControl control,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var report = await control.GetReplicationConfigAsync(cancellationToken).ConfigureAwait(false);
        return ReplicationToolMappings.ToMcp(report);
    }

    /// <summary>Enables replication for a tree under a fixed merge mode.</summary>
    public static async Task<McpReplicationEnableResult> EnableReplicationAsync(
        ILatticeReplicationControl control,
        string treeId,
        string mode,
        string? bootstrapSourceClusterId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var mergeMode = ReplicationToolMappings.ToMergeMode(mode);
        var result = await control.EnableReplicationAsync(
            treeId,
            mergeMode,
            string.IsNullOrEmpty(bootstrapSourceClusterId) ? null : bootstrapSourceClusterId,
            cancellationToken).ConfigureAwait(false);
        return ReplicationToolMappings.ToMcp(result);
    }

    /// <summary>Disables replication for a tree.</summary>
    public static async Task<McpReplicationDisableResult> DisableReplicationAsync(
        ILatticeReplicationControl control,
        string treeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var result = await control.DisableReplicationAsync(treeId, cancellationToken).ConfigureAwait(false);
        return ReplicationToolMappings.ToMcp(result);
    }
}
