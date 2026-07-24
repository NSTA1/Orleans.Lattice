using Grpc.Core;
using ModelContextProtocol;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

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

    /// <summary>
    /// Enables replication for a tree under a fixed merge mode.
    /// </summary>
    /// <remarks>
    /// A rejected in-place merge-mode change (the tree is already enabled under a
    /// different or ambiguous mode) is re-surfaced as an <see cref="McpException"/>
    /// carrying the facade's actionable message, so the MCP client sees "the mode
    /// of an already-enabled tree cannot be changed; disable then re-enable"
    /// instead of the SDK's generic "an error occurred invoking the tool" mask
    /// (issue #1339). Both hosting topologies are covered: the in-silo facade
    /// throws <see cref="LatticeReplicationModeChangeRejectedException"/> directly,
    /// while the remote gRPC binding maps it to an
    /// <see cref="StatusCode.FailedPrecondition"/> <see cref="RpcException"/> whose
    /// detail carries the same message. A fail-closed authorization denial still
    /// propagates unchanged - only the actionable precondition rejection is
    /// translated.
    /// </remarks>
    public static async Task<McpReplicationEnableResult> EnableReplicationAsync(
        ILatticeReplicationControl control,
        string treeId,
        string mode,
        string? bootstrapSourceClusterId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var mergeMode = ReplicationToolMappings.ToMergeMode(mode);
        try
        {
            var result = await control.EnableReplicationAsync(
                treeId,
                mergeMode,
                string.IsNullOrEmpty(bootstrapSourceClusterId) ? null : bootstrapSourceClusterId,
                cancellationToken).ConfigureAwait(false);
            return ReplicationToolMappings.ToMcp(result);
        }
        catch (LatticeReplicationModeChangeRejectedException ex)
        {
            // In-silo topology: the facade throws the domain rejection directly.
            throw new McpException(ex.Message);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition)
        {
            // Remote topology: the gRPC binding already mapped the rejection to a
            // FailedPrecondition status whose detail is the actionable message.
            throw new McpException(ex.Status.Detail);
        }
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
