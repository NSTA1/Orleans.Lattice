using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable, group-atomic write-fence and shipping-pause primitive. See
/// <see cref="ISagaWriteFenceGrain"/> for the contract and the two-release-point
/// rule it enforces.
/// <para>
/// State is persisted so the fence survives a crash; a periodic reminder drives
/// crash-recovery re-evaluation of the release gates (self-lift the write fence
/// on the cutover deadline, resume shipping on observed global completion). The
/// base <see cref="TtlGrain{TSelf}"/> retention reminder reclaims the grain's
/// state a bounded time after the fence reaches its terminal
/// <see cref="SagaWriteFencePhase.Lifted"/> phase.
/// </para>
/// </summary>
internal sealed class SagaWriteFenceGrain(
    IGrainContext grainContext,
    IReminderRegistry reminderRegistry,
    ILogger<SagaWriteFenceGrain> logger,
    [PersistentState("saga-write-fence", LatticeOptions.StorageProviderName)]
    IPersistentState<SagaWriteFenceState> state,
    IShardCountProvider shardCounts,
    IReplicationTopology topology,
    IGrainFactory grainFactory,
    ISagaCompletionSource completionSource,
    IOptionsMonitor<LatticeOptions> options)
    : TtlGrain<SagaWriteFenceGrain>(grainContext, reminderRegistry, logger), ISagaWriteFenceGrain
{
    /// <summary>Default bounded cutover window sizing the self-lifting fence deadline.</summary>
    private const int DefaultFenceWindowSeconds = 300;

    /// <summary>Reminder that drives crash-recovery gate re-evaluation.</summary>
    private const string PollReminderName = "saga-write-fence-poll";

    /// <inheritdoc />
    protected override string TtlReminderName => "saga-write-fence-ttl";

    /// <summary>This grain's key: the saga id it fences.</summary>
    private string SagaKey => GrainContext.GrainId.Key.ToString()!;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => options.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    public async Task EngageAsync(SagaWriteFenceRequest request)
    {
        ArgumentNullException.ThrowIfNull(request.Trees);
        ArgumentException.ThrowIfNullOrEmpty(request.SagaId);

        var sagaId = SagaKey;
        if (!string.Equals(request.SagaId, sagaId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Request saga id '{request.SagaId}' does not match fence grain key '{sagaId}'.",
                nameof(request));
        }

        if (state.State.Phase == SagaWriteFencePhase.Lifted)
        {
            // A superseding engage after a prior lift starts a fresh cycle.
            state.State.ShippingResumed = false;
            state.State.WritesUnblocked = false;
        }

        var windowSeconds = request.FenceWindowSeconds > 0
            ? request.FenceWindowSeconds
            : DefaultFenceWindowSeconds;
        var deadline = DateTime.UtcNow.AddSeconds(windowSeconds).Ticks;

        // Resolved before any state is touched, so a routing read that fails
        // leaves this activation exactly as it was. The set is persisted below so
        // every lift - the local flip, the deadline self-lift, the
        // global-completion or terminal lift - releases exactly the shards this
        // engage fenced, even after the cutover's alias swap has moved the
        // tree's routing onto the new physical copy.
        var resolved = await ResolveFencedShardKeysAsync(request.Trees);

        // A re-engage of a still-active fence keeps the shards it already fenced:
        // if routing moved in between, the earlier set must still be lifted.
        var stillFenced = string.Equals(state.State.SagaId, sagaId, StringComparison.Ordinal)
            && state.State.Phase is not (SagaWriteFencePhase.None or SagaWriteFencePhase.Lifted)
                ? state.State.FencedShardKeys
                : null;

        state.State.SagaId = sagaId;
        state.State.Trees = [.. request.Trees];
        state.State.Phase = SagaWriteFencePhase.Engaged;
        state.State.FenceDeadlineTicks = deadline;
        state.State.CoordinatorClusterId = request.CoordinatorClusterId;
        state.State.WritesUnblocked = false;
        state.State.ShippingResumed = false;
        state.State.EngagedAtTicks = DateTime.UtcNow.Ticks;
        state.State.FencedShardKeys = stillFenced is { Count: > 0 }
            ? [.. stillFenced.Union(resolved, StringComparer.Ordinal)]
            : resolved;
        await state.WriteStateAsync();

        await EngageWriteFenceAsync(sagaId, deadline);
        await PauseShippingAsync(sagaId);
        await PauseReceiveAsync(sagaId);

        await ArmPollReminderAsync();

        Logger.LogInformation(
            "Write fence engaged for saga '{SagaId}' over {TreeCount} tree(s); deadline {Deadline:o}.",
            sagaId, state.State.Trees.Count, new DateTime(deadline, DateTimeKind.Utc));
    }

    /// <inheritdoc />
    public async Task UnblockWritesAsync()
    {
        if (state.State.Phase is SagaWriteFencePhase.None or SagaWriteFencePhase.Lifted
            || state.State.WritesUnblocked)
        {
            return;
        }

        var sagaId = state.State.SagaId!;
        await LiftWriteFenceAsync(sagaId);
        RecordFenceDurationOnce();
        state.State.WritesUnblocked = true;
        state.State.Phase = SagaWriteFencePhase.WritesUnblocked;
        await state.WriteStateAsync();

        Logger.LogInformation(
            "Local writes unblocked for saga '{SagaId}'; shipping stays paused until global completion.",
            sagaId);
    }

    /// <inheritdoc />
    public async Task LiftAsync()
    {
        if (state.State.Phase is SagaWriteFencePhase.None or SagaWriteFencePhase.Lifted)
        {
            return;
        }

        var sagaId = state.State.SagaId!;
        await LiftWriteFenceAsync(sagaId);
        await ResumeShippingAsync(sagaId);
        await ResumeReceiveAsync(sagaId);

        RecordFenceDurationOnce();
        state.State.WritesUnblocked = true;
        state.State.ShippingResumed = true;
        state.State.Phase = SagaWriteFencePhase.Lifted;
        await state.WriteStateAsync();

        await FinishAsync();

        Logger.LogInformation(
            "Write fence fully lifted for saga '{SagaId}' (terminal decision).", sagaId);
    }

    /// <inheritdoc />
    public async Task<SagaWriteFenceSnapshot> PollResumeAsync()
    {
        await EvaluateGateAsync();
        return Snapshot();
    }

    /// <inheritdoc />
    public Task<SagaWriteFenceSnapshot> GetSnapshotAsync() => Task.FromResult(Snapshot());

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName == PollReminderName)
        {
            await EvaluateGateAsync();
        }
    }

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        state.State.SagaId = null;
        state.State.Trees = [];
        state.State.Phase = SagaWriteFencePhase.None;
        state.State.FenceDeadlineTicks = 0;
        state.State.CoordinatorClusterId = null;
        state.State.WritesUnblocked = false;
        state.State.ShippingResumed = false;
        state.State.EngagedAtTicks = 0;
        state.State.FencedShardKeys = [];
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Re-evaluates both release gates: self-lift the write fence on the cutover
    /// deadline, and resume shipping/receiving on observed global completion.
    /// </summary>
    private async Task EvaluateGateAsync()
    {
        if (state.State.Phase is SagaWriteFencePhase.None or SagaWriteFencePhase.Lifted)
        {
            return;
        }

        var sagaId = state.State.SagaId!;

        // Release point 1: the write fence self-lifts on the bounded cutover
        // deadline so a stranded coordinator never fences writes forever. This
        // deliberately does NOT resume shipping.
        if (!state.State.WritesUnblocked
            && DateTime.UtcNow.Ticks >= state.State.FenceDeadlineTicks)
        {
            await LiftWriteFenceAsync(sagaId);
            RecordFenceDurationOnce();
            state.State.WritesUnblocked = true;
            state.State.Phase = SagaWriteFencePhase.WritesUnblocked;
            await state.WriteStateAsync();
            Logger.LogWarning(
                "Write fence self-lifted for saga '{SagaId}' on cutover deadline; shipping stays paused.",
                sagaId);
        }

        // Release point 2: shipping/receiving resume strictly on observed global
        // completion - every participant has flipped - never on a local flip.
        if (!state.State.ShippingResumed)
        {
            var complete = await completionSource
                .IsSagaCompleteAsync(sagaId, state.State.CoordinatorClusterId ?? string.Empty)
                ;
            if (complete)
            {
                await LiftWriteFenceAsync(sagaId);
                await ResumeShippingAsync(sagaId);
                await ResumeReceiveAsync(sagaId);
                RecordFenceDurationOnce();
                state.State.WritesUnblocked = true;
                state.State.ShippingResumed = true;
                state.State.Phase = SagaWriteFencePhase.Lifted;
                await state.WriteStateAsync();
                await FinishAsync();
                Logger.LogInformation(
                    "Shipping resumed for saga '{SagaId}' on observed global completion.", sagaId);
            }
        }
    }

    private async Task FinishAsync()
    {
        await UnregisterPollReminderAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Records the per-tree write-fence window duration (engage to write-fence
    /// lift) exactly once, on the first lift of the write fence. A no-op when the
    /// write fence was already lifted or no engage timestamp is recorded, so
    /// repeated lift call sites (local flip, self-lift deadline, global-completion
    /// or terminal lift) never double-count.
    /// </summary>
    private void RecordFenceDurationOnce()
    {
        if (state.State.WritesUnblocked || state.State.EngagedAtTicks <= 0)
        {
            return;
        }

        var elapsedMs = (DateTime.UtcNow.Ticks - state.State.EngagedAtTicks) / (double)TimeSpan.TicksPerMillisecond;
        if (elapsedMs < 0)
        {
            elapsedMs = 0;
        }

        foreach (var tree in state.State.Trees)
        {
            LatticeReplicationMetrics.SagaFenceDuration.Record(
                elapsedMs,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
                LatticeTenantLabel.ForTree(tree));
        }
    }

    private SagaWriteFenceSnapshot Snapshot() => new()
    {
        SagaId = state.State.SagaId ?? string.Empty,
        Phase = state.State.Phase,
        Trees = [.. state.State.Trees],
        FenceDeadlineTicks = state.State.FenceDeadlineTicks,
        WritesUnblocked = state.State.WritesUnblocked,
        ShippingResumed = state.State.ShippingResumed,
    };

    // --- fan-out helpers (group-atomic across every tree in the set) ---
    //
    // Every helper below fans one call out over trees x shards (or trees x
    // peers) and every one of those calls is independent: each targets a
    // distinct grain, sets or clears a per-saga flag on it, and reads nothing
    // the others write. Issued one at a time - as they were - the group-atomic
    // step cost one sequential round trip per target, so the wall-clock of
    // engaging a fence grew linearly with the fenced topology while the write
    // fence itself was already blocking writers. Issued in bounded overlapped
    // waves the same calls cost ceil(N / BoundedFanOut.DefaultWidth) round
    // trips. The width is below the router grain's local worker pool, so a
    // wave is serviced by separate local workers rather than queued behind
    // itself, and a wider one would only burst more outbound calls at a single
    // Orleans response deadline. Group atomicity is unaffected: it is defined by every
    // target having acked before the helper returns, which Task.WhenAll inside
    // each wave preserves exactly, not by the order they were asked in.

    /// <summary>
    /// Resolves the shard-root grain key of every shard the fence spans, reading
    /// each tree's live routing with bounded overlap rather than one strictly
    /// sequential lookup per tree.
    /// <para>
    /// The keys come from the routing map, not <c>{tree}/0..ShardCount-1</c>: an
    /// adaptive split moves slots to a physical shard above the pinned count, and
    /// a resize or an earlier restore leaves the tree behind an alias whose
    /// physical id differs from the logical one. The pinned range left the split
    /// target unfenced and fenced a retired copy, so post-cut writers raced the
    /// cutover on exactly the shards that were serving them.
    /// </para>
    /// </summary>
    private async Task<List<string>> ResolveFencedShardKeysAsync(IReadOnlyList<string> trees)
    {
        var shardKeys = new List<string>(trees.Count);

        await foreach (var treeKeys in BoundedFanOut.ReadAheadAsync(
            trees,
            BoundedFanOut.DefaultWidth,
            tree => shardCounts.GetShardRootKeysAsync(tree)))
        {
            shardKeys.AddRange(treeKeys);
        }

        return shardKeys;
    }

    /// <summary>
    /// Builds the <c>{tree}/{peer}</c> key of every shipper the fence spans.
    /// </summary>
    private List<string> ResolveShipperKeys()
    {
        var peers = topology.CurrentPeers;
        var trees = state.State.Trees;
        var shipperKeys = new List<string>(trees.Count * peers.Count);

        foreach (var tree in trees)
        {
            foreach (var peer in peers)
            {
                shipperKeys.Add($"{tree}/{peer}");
            }
        }

        return shipperKeys;
    }

    private Task EngageWriteFenceAsync(string sagaId, long deadline) =>
        BoundedFanOut.ForEachAsync(
            state.State.FencedShardKeys,
            BoundedFanOut.DefaultWidth,
            key => grainFactory.GetGrain<IShardRootGrain>(key).EngageWriteFenceAsync(sagaId, deadline));

    private async Task LiftWriteFenceAsync(string sagaId)
    {
        // State persisted before the engaged shard set was recorded carries an
        // empty list; fall back to the tree's current routing for it.
        var shardKeys = state.State.FencedShardKeys.Count > 0
            ? state.State.FencedShardKeys
            : await ResolveFencedShardKeysAsync(state.State.Trees);
        await BoundedFanOut.ForEachAsync(
            shardKeys,
            BoundedFanOut.DefaultWidth,
            key => grainFactory.GetGrain<IShardRootGrain>(key).LiftWriteFenceAsync(sagaId));
    }

    private Task PauseShippingAsync(string sagaId) =>
        BoundedFanOut.ForEachAsync(
            ResolveShipperKeys(),
            BoundedFanOut.DefaultWidth,
            key => grainFactory.GetGrain<IReplicationShipperGrain>(key)
                .PauseShippingAsync(sagaId, CancellationToken.None));

    private Task ResumeShippingAsync(string sagaId) =>
        BoundedFanOut.ForEachAsync(
            ResolveShipperKeys(),
            BoundedFanOut.DefaultWidth,
            key => grainFactory.GetGrain<IReplicationShipperGrain>(key)
                .ResumeShippingAsync(sagaId, CancellationToken.None));

    private Task PauseReceiveAsync(string sagaId) =>
        BoundedFanOut.ForEachAsync(
            state.State.Trees,
            BoundedFanOut.DefaultWidth,
            tree => grainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).PauseAsync(sagaId));

    private Task ResumeReceiveAsync(string sagaId) =>
        BoundedFanOut.ForEachAsync(
            state.State.Trees,
            BoundedFanOut.DefaultWidth,
            tree => grainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).ResumeAsync(sagaId));

    private async Task ArmPollReminderAsync()
    {
        try
        {
            await ReminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: GrainContext.GrainId,
                reminderName: PollReminderName,
                dueTime: MinimumTtl,
                period: MinimumTtl);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Failed to arm poll reminder for saga '{SagaId}' (non-fatal); on-demand poll still applies.",
                state.State.SagaId);
        }
    }

    private async Task UnregisterPollReminderAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, PollReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Failed to unregister poll reminder for saga '{SagaId}' (non-fatal).",
                state.State.SagaId);
        }
    }
}
