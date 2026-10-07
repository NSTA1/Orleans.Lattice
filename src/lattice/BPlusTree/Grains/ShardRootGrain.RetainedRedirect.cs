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
        if (state.State.RetainedRedirect is null && state.State.AdditionalRetainedRedirects is null)
            return;

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

        var rr = state.State.AdditionalRetainedRedirects?.GetValueOrDefault(routedLogical)
            ?? (state.State.RetainedRedirect?.LogicalTreeId == routedLogical ? state.State.RetainedRedirect : null)
            ?? state.State.AdditionalRetainedRedirects?.GetValueOrDefault("")
            ?? state.State.RetainedRedirect;
        if (rr is null) return;

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
        var previousForLogical = state.State.AdditionalRetainedRedirects?.GetValueOrDefault(logicalTreeId)
            ?? (existing?.LogicalTreeId == logicalTreeId ? existing : null);
        if (previousForLogical is not null
            && string.Equals(previousForLogical.OperationId, operationId, StringComparison.Ordinal)
            && string.Equals(previousForLogical.DestinationPhysicalTreeId, destinationPhysicalTreeId, StringComparison.Ordinal))
        {
            // Idempotent re-mark under the same operation.
            return;
        }

        var additional = state.State.AdditionalRetainedRedirects;
        var previous = state.State.PreviousRetainedRedirects;
        if (previousForLogical is not null)
        {
            var updatedPrevious = previous is null
                ? new Dictionary<string, RetainedRedirectState>(StringComparer.Ordinal)
                : new Dictionary<string, RetainedRedirectState>(previous, StringComparer.Ordinal);
            updatedPrevious[operationId] = previousForLogical;
            state.State.PreviousRetainedRedirects = updatedPrevious;
        }
        var updatedAdditional = additional is null
            ? new Dictionary<string, RetainedRedirectState>(StringComparer.Ordinal)
            : new Dictionary<string, RetainedRedirectState>(additional, StringComparer.Ordinal);
        if (existing is not null
            && !string.Equals(existing.LogicalTreeId, logicalTreeId, StringComparison.Ordinal))
        {
            updatedAdditional[existing.LogicalTreeId] = existing;
        }
        updatedAdditional.Remove(logicalTreeId);
        state.State.AdditionalRetainedRedirects = updatedAdditional.Count == 0 ? null : updatedAdditional;
        state.State.RetainedRedirect = new RetainedRedirectState
        {
            DestinationPhysicalTreeId = destinationPhysicalTreeId,
            OperationId = operationId,
            LogicalTreeId = logicalTreeId,
        };
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.RetainedRedirect = existing;
            state.State.AdditionalRetainedRedirects = additional;
            state.State.PreviousRetainedRedirects = previous;
            RecycleAfterRetainedRedirectWriteFailure();
            throw;
        }
    }

    /// <inheritdoc />
    public Task ClearRetainedRedirectAsync(string operationId) =>
        ClearRetainedRedirectCoreAsync(operationId, onlyIfOwned: false);

    /// <inheritdoc />
    public Task ClearRetainedRedirectIfOwnedAsync(string operationId) =>
        ClearRetainedRedirectCoreAsync(operationId, onlyIfOwned: true);

    private async Task ClearRetainedRedirectCoreAsync(string operationId, bool onlyIfOwned)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var rr = state.State.RetainedRedirect;
        var additional = state.State.AdditionalRetainedRedirects;
        var previous = state.State.PreviousRetainedRedirects;
        if (rr is null && additional is null)
        {
            // Idempotent - nothing to clear.
            return;
        }

        var clearPrimary = rr is not null && string.Equals(rr.OperationId, operationId, StringComparison.Ordinal);
        var updatedAdditional = additional?.Where(pair => !string.Equals(pair.Value.OperationId, operationId, StringComparison.Ordinal))
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        if (!clearPrimary && updatedAdditional?.Count == additional?.Count)
        {
            if (onlyIfOwned) return;
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' has a retained redirect from operation '{rr?.OperationId}'; "
                + $"refused ClearRetainedRedirectAsync under different operationId '{operationId}'.");
        }

        var restore = onlyIfOwned ? previous?.GetValueOrDefault(operationId) : null;
        if (clearPrimary) state.State.RetainedRedirect = restore;
        else if (restore is not null)
        {
            updatedAdditional ??= new Dictionary<string, RetainedRedirectState>(StringComparer.Ordinal);
            updatedAdditional[restore.LogicalTreeId] = restore;
        }
        if (previous?.ContainsKey(operationId) == true)
        {
            var updatedPrevious = new Dictionary<string, RetainedRedirectState>(previous, StringComparer.Ordinal);
            updatedPrevious.Remove(operationId);
            state.State.PreviousRetainedRedirects = updatedPrevious.Count == 0 ? null : updatedPrevious;
        }
        state.State.AdditionalRetainedRedirects = updatedAdditional is { Count: > 0 } ? updatedAdditional : null;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.RetainedRedirect = rr;
            state.State.AdditionalRetainedRedirects = additional;
            state.State.PreviousRetainedRedirects = previous;
            RecycleAfterRetainedRedirectWriteFailure();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ReleaseRetainedRedirectAsync(string logicalTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(logicalTreeId);
        var rr = state.State.RetainedRedirect;
        var additional = state.State.AdditionalRetainedRedirects;
        var previous = state.State.PreviousRetainedRedirects;
        if (rr is null && additional is null)
        {
            return;
        }

        // A redirect with no recorded logical id applies to every logical tree
        // other than this shard's own id (see ThrowIfRetainedRedirect).
        var redirectsLogical = rr is not null && (string.IsNullOrEmpty(rr.LogicalTreeId)
            ? !string.Equals(logicalTreeId, TreeId, StringComparison.Ordinal)
            : string.Equals(rr.LogicalTreeId, logicalTreeId, StringComparison.Ordinal));
        var releaseLegacy = logicalTreeId != TreeId && additional?.ContainsKey("") == true;
        if (!redirectsLogical && additional?.ContainsKey(logicalTreeId) != true && !releaseLegacy)
        {
            return;
        }

        if (redirectsLogical) state.State.RetainedRedirect = null;
        if (additional is not null)
        {
            var updatedAdditional = new Dictionary<string, RetainedRedirectState>(additional, StringComparer.Ordinal);
            updatedAdditional.Remove(logicalTreeId);
            if (releaseLegacy) updatedAdditional.Remove("");
            state.State.AdditionalRetainedRedirects = updatedAdditional.Count == 0 ? null : updatedAdditional;
        }
        state.State.PreviousRetainedRedirects = previous?.Where(pair => pair.Value.LogicalTreeId != logicalTreeId)
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.RetainedRedirect = rr;
            state.State.AdditionalRetainedRedirects = additional;
            state.State.PreviousRetainedRedirects = previous;
            RecycleAfterRetainedRedirectWriteFailure();
            throw;
        }
    }

    private void RecycleAfterRetainedRedirectWriteFailure()
    {
        // A lost acknowledgement may have persisted the fence and advanced its
        // etag. Retry on a fresh activation, not the restored in-memory snapshot.
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested,
            "Retained redirect persistence failed; reload durable state before retry."));
    }
}
