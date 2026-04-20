using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Online shadow-forwarding primitive for the shard root.
/// <para>
/// When <see cref="ShardRootState.ShadowForward"/> is non-null, every accepted
/// mutation on this shard is mirrored in parallel to the corresponding shard
/// on <c>ShadowForwardState.DestinationPhysicalTreeId</c>. The destination
/// tree is constrained by the coordinator to share this tree's
/// <see cref="ShardMap"/>, so the shadow target is always at the same shard
/// index — <c>{DestinationPhysicalTreeId}/{MyShardIndex}</c>.
/// </para>
/// <para>
/// The three phases <see cref="ShadowForwardPhase.Draining"/>,
/// <see cref="ShadowForwardPhase.Drained"/>, and
/// <see cref="ShadowForwardPhase.Rejecting"/> drive two hot-path behaviours:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <c>Draining</c> / <c>Drained</c>: every accepted mutation is forwarded
/// in parallel via <see cref="Task.WhenAll(Task[])"/>. Correctness relies on
/// LWW commutativity — concurrent forwards and background-drain writes
/// converge to the same final state on the destination shard regardless of
/// interleaving because the highest HLC wins on every key.
/// </description></item>
/// <item><description>
/// <c>Rejecting</c>: every operation (read or write) throws
/// <see cref="StaleTreeRoutingException"/>, signalling the calling
/// <c>LatticeGrain</c> to refresh its cached alias + shard-map snapshot and
/// retry against the destination tree.
/// </description></item>
/// </list>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Hot-path gate invoked from every mutation and read entry point.
    /// Throws <see cref="StaleTreeRoutingException"/> when this shard is in
    /// <see cref="ShadowForwardPhase.Rejecting"/>. No-op otherwise.
    /// </summary>
    private void ThrowIfTreeRejecting()
    {
        var sf = state.State.ShadowForward;
        if (sf is null) return;
        if (sf.Phase != ShadowForwardPhase.Rejecting) return;
        // Prefer the coordinator-stamped logical tree ID when present. The
        // shard's grain key only encodes the physical tree ID, so without
        // this field the diagnostic would misreport a physical ID as the
        // caller's logical name during resize (where they differ).
        var logical = string.IsNullOrEmpty(sf.LogicalTreeId) ? TreeId : sf.LogicalTreeId;
        throw new StaleTreeRoutingException(
            logicalTreeId: logical,
            stalePhysicalTreeId: TreeId,
            destinationPhysicalTreeId: sf.DestinationPhysicalTreeId);
    }

    /// <summary>
    /// Returns a grain reference to the shadow target shard for this shard's
    /// active shadow-forward operation, or <c>null</c> if forwarding is not
    /// active or the destination equals the source (a pathological configuration
    /// that would produce infinite recursion).
    /// </summary>
    private IShardRootGrain? TryGetShadowTarget()
    {
        var sf = state.State.ShadowForward;
        if (sf is null) return null;
        if (sf.Phase != ShadowForwardPhase.Draining
            && sf.Phase != ShadowForwardPhase.Drained) return null;
        if (string.IsNullOrEmpty(sf.DestinationPhysicalTreeId)) return null;
        // Defensive: refuse to forward to ourselves.
        if (string.Equals(sf.DestinationPhysicalTreeId, TreeId, StringComparison.Ordinal))
            return null;
        var targetKey = $"{sf.DestinationPhysicalTreeId}/{MyShardIndex}";
        return grainFactory.GetGrain<IShardRootGrain>(targetKey);
    }

    /// <summary>
    /// Invokes <paramref name="forwardAction"/> against the shadow target if
    /// forwarding is currently active. Returns a completed task otherwise.
    /// Callers wire this into <see cref="Task.WhenAll(Task[])"/> alongside
    /// their local write so the two execute in parallel.
    /// </summary>
    private Task ForwardShadowAsync(Func<IShardRootGrain, Task> forwardAction)
    {
        var target = TryGetShadowTarget();
        return target is null ? Task.CompletedTask : forwardAction(target);
    }

    /// <inheritdoc />
    public async Task BeginShadowForwardAsync(string destinationPhysicalTreeId, string operationId, string logicalTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(destinationPhysicalTreeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        if (string.Equals(destinationPhysicalTreeId, TreeId, StringComparison.Ordinal))
            throw new ArgumentException(
                "Destination tree ID must differ from the source tree ID.",
                nameof(destinationPhysicalTreeId));

        var existing = state.State.ShadowForward;
        if (existing is not null)
        {
            if (!string.Equals(existing.OperationId, operationId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Shard '{context.GrainId.Key}' is already participating in shadow-forward operation '{existing.OperationId}'; refused BeginShadowForwardAsync with different operationId '{operationId}'.");
            }

            if (!string.Equals(existing.DestinationPhysicalTreeId, destinationPhysicalTreeId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Shard '{context.GrainId.Key}' is already forwarding to '{existing.DestinationPhysicalTreeId}'; refused BeginShadowForwardAsync to different destination '{destinationPhysicalTreeId}' under the same operationId.");
            }

            // Idempotent re-entry: any phase for the same destination + operationId returns.
            return;
        }

        state.State.ShadowForward = new ShadowForwardState
        {
            DestinationPhysicalTreeId = destinationPhysicalTreeId,
            Phase = ShadowForwardPhase.Draining,
            OperationId = operationId,
            LogicalTreeId = logicalTreeId,
        };
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task MarkDrainedAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var sf = state.State.ShadowForward;
        if (sf is null)
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' has no active shadow-forward operation; MarkDrainedAsync refused.");
        if (!string.Equals(sf.OperationId, operationId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is participating in shadow-forward operation '{sf.OperationId}'; refused MarkDrainedAsync under different operationId '{operationId}'.");

        if (sf.Phase == ShadowForwardPhase.Drained || sf.Phase == ShadowForwardPhase.Rejecting)
        {
            // Idempotent — already past Draining.
            return;
        }

        sf.Phase = ShadowForwardPhase.Drained;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task EnterRejectingAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var sf = state.State.ShadowForward;
        if (sf is null)
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' has no active shadow-forward operation; EnterRejectingAsync refused.");
        if (!string.Equals(sf.OperationId, operationId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is participating in shadow-forward operation '{sf.OperationId}'; refused EnterRejectingAsync under different operationId '{operationId}'.");

        if (sf.Phase == ShadowForwardPhase.Rejecting)
        {
            return;
        }

        sf.Phase = ShadowForwardPhase.Rejecting;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task ClearShadowForwardAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var sf = state.State.ShadowForward;
        if (sf is null)
        {
            return; // Idempotent — already cleared.
        }
        if (!string.Equals(sf.OperationId, operationId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is participating in shadow-forward operation '{sf.OperationId}'; refused ClearShadowForwardAsync under different operationId '{operationId}'.");

        state.State.ShadowForward = null;
        await state.WriteStateAsync();
    }
}
