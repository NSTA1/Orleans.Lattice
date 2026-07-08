using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Retained-previous-tree redirect primitive for the shard root.
/// <para>
/// A shadow-cutover restore swaps the logical alias to a freshly loaded shadow
/// tree but leaves the previous physical tree in place so the restore can be
/// reverted. Stateless-worker <c>LatticeGrain</c> routing activations cache the
/// logical-&gt;physical alias for the lifetime of the activation and only
/// re-resolve it when a downstream shard signals staleness via
/// <see cref="StaleTreeRoutingException"/>. Because the retained tree keeps
/// answering, a stale activation would otherwise serve pre-restore data
/// forever. Marking the retained tree's shards with a
/// <see cref="RetainedRedirectState"/> makes them throw that signal for
/// logical-alias-routed traffic, so the caller re-resolves and self-heals onto
/// the destination tree - exactly as the online-resize
/// <see cref="ShadowForwardPhase.Rejecting"/> gate does, but without forwarding
/// writes into the frozen revert snapshot.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Hot-path gate invoked from every mutation and read entry point. Throws
    /// <see cref="StaleTreeRoutingException"/> when this shard's tree has been
    /// superseded by a shadow-cutover restore <em>and</em> the current
    /// operation arrived via the logical alias. No-op for direct-physical
    /// access and internal maintenance.
    /// </summary>
    private void ThrowIfRetainedRedirect()
    {
        var rr = state.State.RetainedRedirect;
        if (rr is null) return;

        // Discriminate logical-alias-routed traffic (which must self-heal onto
        // the destination tree) from direct-physical access and maintenance
        // (which must keep reading the retained snapshot). The routing tier
        // stamps the marker with the addressing activation's TreeId:
        //   - absent  => maintenance firing directly on the shard -> no-op
        //   - == the redirected logical tree name => logical-alias traffic
        //     (including the case where the retained physical id equals the
        //     logical name, i.e. the tree was never aliased) -> redirect
        //   - anything else (e.g. the retained tree's own distinct physical
        //     id, used by revert / diagnostics) -> keep reading the snapshot.
        if (RequestContext.Get(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey) is not string routedLogical)
            return;

        var logicalRouted = string.IsNullOrEmpty(rr.LogicalTreeId)
            ? !string.Equals(routedLogical, TreeId, StringComparison.Ordinal)
            : string.Equals(routedLogical, rr.LogicalTreeId, StringComparison.Ordinal);
        if (!logicalRouted) return;

        var logical = string.IsNullOrEmpty(rr.LogicalTreeId) ? routedLogical : rr.LogicalTreeId;
        throw new StaleTreeRoutingException(
            logicalTreeId: logical,
            stalePhysicalTreeId: TreeId,
            destinationPhysicalTreeId: rr.DestinationPhysicalTreeId);
    }

    /// <inheritdoc />
    public async Task MarkRetainedRedirectAsync(string destinationPhysicalTreeId, string operationId, string logicalTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(destinationPhysicalTreeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        if (string.Equals(destinationPhysicalTreeId, TreeId, StringComparison.Ordinal))
            throw new ArgumentException(
                "Destination tree ID must differ from the retained (source) tree ID.",
                nameof(destinationPhysicalTreeId));

        var existing = state.State.RetainedRedirect;
        if (existing is not null
            && string.Equals(existing.OperationId, operationId, StringComparison.Ordinal)
            && string.Equals(existing.DestinationPhysicalTreeId, destinationPhysicalTreeId, StringComparison.Ordinal)
            && string.Equals(existing.LogicalTreeId, logicalTreeId, StringComparison.Ordinal))
        {
            // Idempotent re-mark under the same operation.
            return;
        }

        state.State.RetainedRedirect = new RetainedRedirectState
        {
            DestinationPhysicalTreeId = destinationPhysicalTreeId,
            OperationId = operationId,
            LogicalTreeId = logicalTreeId,
        };
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task ClearRetainedRedirectAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var rr = state.State.RetainedRedirect;
        if (rr is null)
        {
            // Idempotent - nothing to clear.
            return;
        }

        if (!string.Equals(rr.OperationId, operationId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' has a retained redirect from operation '{rr.OperationId}'; "
                + $"refused ClearRetainedRedirectAsync under different operationId '{operationId}'.");
        }

        state.State.RetainedRedirect = null;
        await WriteShardStateAsync();
    }
}
