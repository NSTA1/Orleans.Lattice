using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;
/// <summary>
/// Online shadow-forwarding primitive for the shard root.
/// <para>
/// When <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.ShadowForward"/> is non-null, the shard's
/// last-writer-wins mutation paths (point, batched and conditional writes,
/// deletes, range deletes, batched merges, and atomic-write terminals) are
/// mirrored in parallel to the shard with the same index on
/// <c>ShadowForwardState.DestinationPhysicalTreeId</c>,
/// <c>{DestinationPhysicalTreeId}/{MyShardIndex}</c>; the typed CRDT delta paths
/// and bulk appends are not mirrored. The target is chosen by index alone. The
/// snapshot coordinator registers the destination tree with this tree's pinned
/// shard count and its routing <see cref="ShardMap"/>
/// (<c>TreeSnapshotGrain.InitiateSnapshotStateAsync</c>), and begins forwarding
/// on every shard that map names as well as the pinned range, so a slot routes
/// to the same physical index on both trees and a mirrored key lands on the
/// shard the destination routes it to - including on a shard an adaptive split
/// allocated above the pinned count.
/// </para>
/// <para>
/// The three phases <see cref="ShadowForwardPhase.Draining"/>,
/// <see cref="ShadowForwardPhase.Drained"/>, and
/// <see cref="ShadowForwardPhase.Rejecting"/> drive two hot-path behaviours:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <c>Draining</c> / <c>Drained</c>: each mirrored mutation (see above) is
/// forwarded in parallel with its local apply, and the destination resolves
/// every key by last-writer-wins (highest HLC). A forwarded write carries no
/// timestamp of its own unless it already runs under an HLC override
/// (<see cref="LatticeHlcOverrideContext"/>), so the destination leaf stamps it
/// from its own clock, while a drained entry keeps its source HLC; a drained
/// entry merged first advances that clock, but one merged after the forward
/// can outrank it, so the order in which the two arrive can decide which
/// version survives.
/// </description></item>
/// <item><description>
/// <c>Rejecting</c>: every operation (read or write) throws
/// <see cref="StaleTreeRoutingException"/>, signalling the calling
/// <c>LatticeGrain</c> to refresh its cached alias + shard-map snapshot and
/// retry against the destination tree. A mutation that passed the gate before
/// the phase was set is still mirrored, because the resize fences the old copy
/// before it moves the alias.
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
        // Rejecting still forwards: the gate refuses every operation that
        // arrives once the phase is set, so the only mutations that get this
        // far are ones that passed the gate before it - an interleaved batch
        // whose turn yielded, or a terminal mid-flight. Those were accepted
        // while this copy was live, and the resize fences the old copy before
        // it moves the alias, so dropping their mirror would leave an
        // acknowledged write on a copy no router reads.
        if (sf.Phase != ShadowForwardPhase.Draining
            && sf.Phase != ShadowForwardPhase.Drained
            && sf.Phase != ShadowForwardPhase.Rejecting) return null;
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
    /// <para>
    /// State is passed by value via <typeparamref name="TState"/> so callers
    /// can use <c>static</c> lambdas (compiler-cached singleton delegates) and
    /// avoid per-call closure allocation. The dispatch is monomorphic at each
    /// call site after generic specialisation.
    /// </para>
    /// </summary>
    private Task ForwardShadowAsync<TState>(TState state, Func<IShardRootGrain, TState, Task> forwardAction)
    {
        var target = TryGetShadowTarget();
        // Bound the outbound forward with the per-tree ShardForwardTimeout so a
        // forward parked against a shard whose ownership is changing during the
        // reshard swap phase cannot pin the foreground write turn indefinitely.
        // The no-forward fast path stays synchronous (Task.CompletedTask) so
        // TrackShadowForward's IsCompleted check still short-circuits.
        return target is null
            ? Task.CompletedTask
            : ForwardWithDeadlineAsync(() => forwardAction(target, state));
    }

    /// <summary>
    /// Bounds <paramref name="forwardCall"/> - a single outbound shard-to-shard
    /// write forward (online-resize shadow forward or adaptive-split migration
    /// forward) - with the per-tree
    /// <see cref="LatticeOptions.ShardForwardTimeout"/> deadline.
    /// <para>
    /// During the reshard swap phase the destination shard's ownership is
    /// changing, and Orleans can reject the outbound forward message and leave
    /// the caller-side <c>await</c> neither completing nor faulting. Without a
    /// ceiling the forwarding turn never returns, the lattice grain's per-shard
    /// fan-out saturates at its in-flight limit, and the whole write pipeline
    /// wedges with no fault and no activation recycle. The deadline abandons
    /// the parked forward (its eventual completion is harmlessly unobserved)
    /// and faults the turn with a <see cref="TimeoutException"/>, which the
    /// existing transient-exception retry envelope on every mutation path
    /// catches and re-runs against refreshed routing once the swap has settled.
    /// </para>
    /// <para>
    /// Abandoning a forward never loses data: convergence on the destination
    /// shard is independently guaranteed by last-writer-wins plus the split
    /// coordinator's authoritative leaf-chain drain (Drain phase and the
    /// Complete-phase final drain), so the entry reaches the destination via
    /// the background sweep even when this per-write forward is dropped.
    /// </para>
    /// <para>
    /// When the configured timeout is <see cref="Timeout.InfiniteTimeSpan"/>
    /// the call is awaited unbounded, restoring the historical behaviour.
    /// </para>
    /// </summary>
    private async Task ForwardWithDeadlineAsync(Func<Task> forwardCall)
    {
        var timeout = (await GetOptionsAsync()).ShardForwardTimeout;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            await forwardCall().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            return;
        }

        using var deadline = new CancellationTokenSource(timeout);
        try
        {
            await forwardCall().WaitAsync(deadline.Token)
                .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
        catch (OperationCanceledException oce) when (deadline.IsCancellationRequested)
        {
            LatticeMetrics.ShardForwardTimeouts.Add(
                1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
                LatticeTenantLabel.ForTree(TreeId));
            throw new TimeoutException(
                $"Outbound shard forward from shard {MyShardIndex} of tree '{TreeId}' "
                + $"exceeded the {timeout} forward deadline "
                + $"({nameof(LatticeOptions.ShardForwardTimeout)}); the destination shard's "
                + "ownership is likely changing during a reshard swap. The forward is "
                + "abandoned and the write will be retried against refreshed routing.", oce);
        }
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
        await WriteShardStateAsync();
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
            // Idempotent - already past Draining.
            return;
        }

        sf.Phase = ShadowForwardPhase.Drained;
        await WriteShardStateAsync();
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
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task ExitRejectingAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var sf = state.State.ShadowForward;
        if (sf is null)
        {
            return;
        }
        if (!string.Equals(sf.OperationId, operationId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is participating in shadow-forward operation '{sf.OperationId}'; refused ExitRejectingAsync under different operationId '{operationId}'.");

        if (sf.Phase != ShadowForwardPhase.Rejecting)
        {
            return;
        }

        sf.Phase = ShadowForwardPhase.Drained;
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task ClearShadowForwardAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var sf = state.State.ShadowForward;
        if (sf is null)
        {
            return; // Idempotent - already cleared.
        }
        if (!string.Equals(sf.OperationId, operationId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is participating in shadow-forward operation '{sf.OperationId}'; refused ClearShadowForwardAsync under different operationId '{operationId}'.");

        state.State.ShadowForward = null;
        await WriteShardStateAsync();
    }
}
