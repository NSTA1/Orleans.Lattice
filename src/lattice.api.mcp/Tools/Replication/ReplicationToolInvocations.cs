using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter layer between the replication MCP tools and the
/// <see cref="ILatticeReplicationControl"/> facade: one method per tool that maps
/// the tool's arguments onto a facade call and projects the facade result onto
/// the compact MCP DTO. These methods hold no transport, authorization, or
/// fault-translation concern - the fail-closed replication access gate lives in
/// the facade, the caller credential is stamped on the ambient context by the
/// tool delegate before the method runs, and any escaping fault is translated to
/// an actionable <see cref="ModelContextProtocol.McpException"/> at the shared
/// <see cref="CredentialStampingTool"/> invocation seam - so they are directly
/// unit-testable against a fake facade.
/// </summary>
internal static class ReplicationToolInvocations
{
    /// <summary>
    /// Reports the effective replicated-tree set the caller is authorized to
    /// see, reconciling runtime and static enrollment as the facade does.
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
    /// This method is a pure adapter and references only always-loaded types. In
    /// particular it does <b>not</b> name
    /// <c>LatticeReplicationModeChangeRejectedException</c> (a type in the
    /// satellite <c>Orleans.Lattice.Replication</c> assembly): naming it in a
    /// <c>catch</c> clause here would make the JIT throw
    /// <see cref="System.IO.FileNotFoundException"/> while compiling this method
    /// whenever that assembly is absent from the MCP host, masking the real cause
    /// before any gRPC call is dispatched (issue #1352). A rejected in-place
    /// merge-mode change - whether surfaced in-silo as the domain exception or
    /// remotely as a <see cref="Grpc.Core.StatusCode.FailedPrecondition"/>
    /// <see cref="Grpc.Core.RpcException"/> whose detail carries the actionable
    /// message - is converted into a <see cref="ModelContextProtocol.McpException"/>
    /// at the shared <see cref="CredentialStampingTool"/> seam via
    /// <see cref="McpToolFaultTranslator"/>, so the MCP client sees the actionable
    /// guidance instead of the SDK's generic mask (issue #1339, generalised).
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
