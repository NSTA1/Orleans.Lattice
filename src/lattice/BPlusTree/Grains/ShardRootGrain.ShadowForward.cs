using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;
/// <summary>
/// Online shadow-forwarding primitive for the shard root.
/// <para>
/// When <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.ShadowForward"/> is non-null, the shard's
/// mutation paths (point, batched and conditional writes, deletes, range
/// deletes, batched merges, atomic-write terminals, typed CRDT delta applies and
/// bulk appends) are mirrored to the shard with the same index on
/// <c>ShadowForwardState.DestinationPhysicalTreeId</c>,
/// <c>{DestinationPhysicalTreeId}/{MyShardIndex}</c>. The target is chosen by index alone. The
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
/// before it moves the alias. So is an atomic-write saga's own traffic for a
/// saga bound to this copy - its prepared batch and its terminals - which the
/// gate admits until the copy is purged, so the batch lands whole on both
/// copies (issue #4369).
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
    /// <para>
    /// The forward is addressed to the destination's shard with this shard's
    /// index. Once a resize has completed, a split of the resized copy can move
    /// a slot off that shard, which then refuses the forward; the refusal is
    /// followed to the shard that owns the slot now (issue #4478), see
    /// <see cref="ShadowForwardRefusal"/>. <paramref name="splitPerKey"/> breaks
    /// a refused batch into one forward per entry before it is followed; a
    /// single-key forward passes none. A forward still refused once the hops
    /// run out fails, and so does the mirrored write.
    /// </para>
    /// <para>
    /// <paramref name="closureState"/> marks a forward that is not routed by key:
    /// an atomic-write terminal of a saga bound to this copy. While this copy is
    /// <see cref="ShadowForwardPhase.Rejecting"/> (the resize has swapped, so the
    /// resized copy may have split since) it is delivered to the destination's
    /// shard with this index and to every shard reachable from it through that
    /// copy's split and consolidation records, with the state
    /// <paramref name="closureState"/> derives for the shards other than the
    /// first. Before the swap no migration of the destination can run, so the
    /// shard with this index is the whole closure.
    /// </para>
    /// </summary>
    private Task ForwardShadowAsync<TState>(
        TState forwardState,
        Func<IShardRootGrain, TState, Task> forwardAction,
        Func<TState, IReadOnlyList<TState>>? splitPerKey = null,
        Func<TState, TState>? closureState = null)
    {
        var target = TryGetShadowTarget();
        // Bound the outbound forward with the per-tree ShardForwardTimeout so a
        // forward parked against a shard whose ownership is changing during the
        // reshard swap phase cannot pin the foreground write turn indefinitely.
        // The no-forward fast path stays synchronous (Task.CompletedTask) so
        // TrackShadowForward's IsCompleted check still short-circuits.
        if (target is null) return Task.CompletedTask;

        var sf = state.State.ShadowForward!;
        var destination = sf.DestinationPhysicalTreeId;
        if (closureState is not null && sf.Phase == ShadowForwardPhase.Rejecting)
        {
            return ForwardWithDeadlineAsync(
                () => ForwardOverDestinationClosureAsync(destination, forwardState, forwardAction, closureState));
        }

        return ForwardWithDeadlineAsync(
            () => ForwardFollowingRefusalsAsync(destination, target, forwardState, forwardAction, splitPerKey));
    }

    /// <summary>
    /// Sends a key-routed forward to the destination's shard with this index and
    /// follows a refusal naming the slot's current owner. See
    /// <see cref="ForwardShadowAsync"/>.
    /// </summary>
    private async Task ForwardFollowingRefusalsAsync<TState>(
        string destination,
        IShardRootGrain first,
        TState forwardState,
        Func<IShardRootGrain, TState, Task> forwardAction,
        Func<TState, IReadOnlyList<TState>>? splitPerKey)
    {
        StaleShardRoutingException refusal;
        try
        {
            await forwardAction(first, forwardState);
            return;
        }
        catch (StaleShardRoutingException ex) when (ShadowForwardRefusal.NextShard(ex, MyShardIndex, hopsTaken: 0) is not null)
        {
            refusal = ex;
        }

        if (splitPerKey is null)
        {
            await ChaseShadowForwardAsync(
                destination, refusal.TargetShardIndex, forwardState, forwardAction, hopsTaken: 1);
            return;
        }

        var parts = splitPerKey(forwardState);
        var sends = new Task[parts.Count];
        for (var i = 0; i < parts.Count; i++)
            sends[i] = ChaseShadowForwardAsync(destination, MyShardIndex, parts[i], forwardAction, hopsTaken: 1);
        await Task.WhenAll(sends);
    }

    /// <summary>
    /// Sends one forward to <paramref name="shardIndex"/> of
    /// <paramref name="destination"/>, re-sending it to the shard each refusal
    /// names until it is taken or <see cref="ShadowForwardRefusal.MaxHops"/>
    /// re-sends have been followed, when the last refusal surfaces.
    /// </summary>
    private async Task ChaseShadowForwardAsync<TState>(
        string destination,
        int shardIndex,
        TState forwardState,
        Func<IShardRootGrain, TState, Task> forwardAction,
        int hopsTaken)
    {
        while (true)
        {
            try
            {
                await forwardAction(grainFactory.GetGrain<IShardRootGrain>($"{destination}/{shardIndex}"), forwardState);
                return;
            }
            catch (StaleShardRoutingException ex)
            {
                if (ShadowForwardRefusal.NextShard(ex, shardIndex, hopsTaken) is not { } next) throw;
                shardIndex = next;
                hopsTaken++;
            }
        }
    }

    /// <summary>
    /// Delivers a forward that is not routed by key to the destination's shard
    /// with this index and every shard reachable from it through the
    /// destination's split and consolidation records. Any refusal fails the
    /// whole forward. See <see cref="ForwardShadowAsync"/>.
    /// </summary>
    private async Task ForwardOverDestinationClosureAsync<TState>(
        string destination,
        TState forwardState,
        Func<IShardRootGrain, TState, Task> forwardAction,
        Func<TState, TState> closureState)
    {
        var closure = await TerminalFanOutResolver.ResolveTransitiveAsync(
            grainFactory, destination, [MyShardIndex], CancellationToken.None);
        if (closure.Count <= 1)
        {
            await forwardAction(grainFactory.GetGrain<IShardRootGrain>($"{destination}/{MyShardIndex}"), forwardState);
            return;
        }

        var others = closureState(forwardState);
        var sends = new Task[closure.Count];
        for (var i = 0; i < closure.Count; i++)
        {
            var index = closure[i];
            sends[i] = forwardAction(
                grainFactory.GetGrain<IShardRootGrain>($"{destination}/{index}"),
                index == MyShardIndex ? forwardState : others);
        }
        await Task.WhenAll(sends);
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
        // A forwarded saga prepare can be delivered after the saga decided (an
        // abandoned forward is still in flight, or a duplicate), so mark it for
        // the destination leaf, which then checks the saga's decision before
        // bucketing it (issue #4445). The marker names the logical tree the
        // saga records its decision under: the facade's routed stamp, else this
        // shard's own tree. Scoped to this async method, so the caller's
        // concurrent local write never sees the marker.
        using var forwardedPrepare = LatticeForwardedPrepareContext.BeginScopeIfPrepared(
            RequestContext.Get(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey) is string { Length: > 0 } routedLogical
                ? routedLogical
                : TreeId);
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
    public Task<string?> GetMirrorDestinationAsync()
    {
        var destination = TryGetShadowTarget() is null
            ? null
            : state.State.ShadowForward!.DestinationPhysicalTreeId;
        return Task.FromResult(destination);
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
