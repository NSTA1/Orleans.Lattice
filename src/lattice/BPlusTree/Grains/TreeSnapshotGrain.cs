using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshots a source tree into a new destination tree, copying all live entries
/// shard-by-shard. Supports offline mode (source tree locked during copy) and
/// online mode (source tree remains available).
/// <para>
/// Follows the same reminder + keepalive + grain-timer pattern used by
/// <see cref="TombstoneCompactionGrain"/> and <see cref="TreeResizeGrain"/>.
/// Progress is persisted per-phase so that a silo restart mid-snapshot can
/// resume without data loss.
/// </para>
/// Key format: <c>{sourceTreeId}</c>.
/// </summary>
internal sealed class TreeSnapshotGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeSnapshotGrain> logger,
    [PersistentState("tree-snapshot", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeSnapshotState> state)
    : CoordinatorGrain<TreeSnapshotGrain>(context, reminderRegistry, logger), ITreeSnapshotGrain
{
    private const int MaxRetriesPerPhase = 1;

    private string SourceTreeId => Context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(SourceTreeId);

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "snapshot-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {SourceTreeId}";

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null)
    {
        await SnapshotWithOperationIdAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren,
            Guid.NewGuid().ToString("N"), SourceTreeId);
    }

    /// <inheritdoc />
    public async Task SnapshotWithOperationIdAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys, int? maxInternalChildren, string operationId, string logicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(destinationTreeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(logicalTreeId);

        if (maxLeafKeys is not null && maxLeafKeys <= 1)
            throw new ArgumentOutOfRangeException(nameof(maxLeafKeys), "Must be greater than 1.");
        if (maxInternalChildren is not null && maxInternalChildren <= 2)
            throw new ArgumentOutOfRangeException(nameof(maxInternalChildren), "Must be greater than 2.");

        if (string.Equals(SourceTreeId, destinationTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Destination tree ID must differ from the source tree ID.", nameof(destinationTreeId));

        if (destinationTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new ArgumentException($"Destination tree ID must not start with the reserved prefix '{LatticeConstants.SystemTreePrefix}'.", nameof(destinationTreeId));

        if (state.State.InProgress)
        {
            // Idempotent if same parameters.
            if (state.State.DestinationTreeId == destinationTreeId &&
                state.State.Mode == mode &&
                state.State.MaxLeafKeys == maxLeafKeys &&
                state.State.MaxInternalChildren == maxInternalChildren)
                return;

            throw new InvalidOperationException(
                $"A snapshot is already in progress for tree '{SourceTreeId}' to destination '{state.State.DestinationTreeId}'.");
        }

        if (state.State.Complete)
        {
            state.State.Complete = false;
        }

        // Resolve the source tree's pinned structural sizing from the registry.
        // The destination tree is created by this grain (see InitiateSnapshotStateAsync)
        // and inherits the source's ShardCount - there is no pre-existing
        // destination to compare against, so no "shard counts must match"
        // check against destOptions.ShardCount is required.
        var sourceResolved = await optionsResolver.ResolveAsync(SourceTreeId);

        // Validate destination tree doesn't already exist.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        if (await registry.ExistsAsync(destinationTreeId))
            throw new InvalidOperationException(
                $"Destination tree '{destinationTreeId}' already exists. Choose a new tree ID.");

        await InitiateSnapshotStateAsync(destinationTreeId, mode, sourceResolved.ShardCount,
            maxLeafKeys, maxInternalChildren, operationId, logicalTreeId);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Persists snapshot intent and registers the destination tree in the registry.
    /// For offline mode, sets <see cref="SnapshotPhase.Lock"/> so that shard marking
    /// is deferred to <see cref="LockSourceShardsAsync"/>. Exposed as <c>internal</c>
    /// for unit testing.
    /// </summary>
    internal async Task InitiateSnapshotStateAsync(string destinationTreeId, SnapshotMode mode,
        int shardCount, int? maxLeafKeys = null, int? maxInternalChildren = null,
        string? operationId = null, string? logicalTreeId = null)
    {
        // Register the destination tree in the registry before any data is written.
        // Always seed the ShardCount pin from the source so the registry
        // resolver has a complete structural pin for the destination tree.
        // MaxLeafKeys / MaxInternalChildren are propagated only when the
        // caller overrode them (resize case); otherwise the registry-grain's
        // seeding fills defaults.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = maxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = shardCount,
        };
        await registry.RegisterAsync(destinationTreeId, entry);

        // Persist intent BEFORE any shard-marking side effects.
        state.State.InProgress = true;
        state.State.Phase = mode switch
        {
            SnapshotMode.Offline => SnapshotPhase.Lock,
            SnapshotMode.Online => SnapshotPhase.ShadowBegin,
            _ => throw new ArgumentOutOfRangeException(nameof(mode)),
        };
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.DestinationTreeId = destinationTreeId;
        state.State.Mode = mode;
        state.State.OperationId = operationId ?? Guid.NewGuid().ToString("N");
        state.State.ShardCount = shardCount;
        state.State.MaxLeafKeys = maxLeafKeys;
        state.State.MaxInternalChildren = maxInternalChildren;
        state.State.Complete = false;
        state.State.LogicalTreeId = logicalTreeId ?? "";
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Marks all source shards as deleted. Called once when the
    /// <see cref="SnapshotPhase.Lock"/> phase is processed (offline mode only).
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task LockSourceShardsAsync()
    {
        var shardCount = state.State.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        state.State.Phase = SnapshotPhase.Copy;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();
    }

    public async Task RunSnapshotPassAsync()
    {
        if (!state.State.InProgress) return;

        if (state.State.Phase == SnapshotPhase.Lock)
        {
            await LockSourceShardsAsync();
        }

        if (state.State.Phase == SnapshotPhase.ShadowBegin)
        {
            await BeginShadowForwardAllShardsAsync();
        }

        if (state.State.Mode == SnapshotMode.Online
            && state.State.Phase == SnapshotPhase.Copy
            && state.State.NextShardIndex < state.State.ShardCount)
        {
            await DrainAllShardsOnlineAsync();
        }
        else
        {
            while (state.State.NextShardIndex < state.State.ShardCount)
            {
                await ProcessCurrentPhaseAsync();
            }
        }

        await CompleteSnapshotAsync();
    }

    /// <summary>
    /// Processes the next phase of the current shard. If all shards are done,
    /// completes the snapshot. Exposed as <c>internal</c> via <c>protected</c>
    /// override for unit testing.
    /// </summary>
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (state.State.Phase == SnapshotPhase.Lock)
        {
            await LockSourceShardsAsync();
            return;
        }

        if (state.State.Phase == SnapshotPhase.ShadowBegin)
        {
            await BeginShadowForwardAllShardsAsync();
            return;
        }

        if (state.State.NextShardIndex >= state.State.ShardCount)
        {
            await CompleteSnapshotAsync();
            return;
        }

        await ProcessCurrentPhaseAsync();
    }

    private async Task ProcessCurrentPhaseAsync()
    {
        var shardIndex = state.State.NextShardIndex;

        try
        {
            switch (state.State.Phase)
            {
                case SnapshotPhase.Copy:
                    await CopyShardAsync(shardIndex);

                    if (state.State.Mode == SnapshotMode.Offline)
                    {
                        state.State.Phase = SnapshotPhase.Unmark;
                    }
                    else
                    {
                        // Online mode: mark this shard drained (shadow-forward
                        // continues until the coordinator transitions to
                        // Rejecting), advance to the next shard.
                        await MarkShardDrainedAsync(shardIndex);
                        state.State.NextShardIndex++;
                        state.State.Phase = SnapshotPhase.Copy;
                    }
                    state.State.ShardRetries = 0;
                    await state.WriteStateAsync();
                    break;

                case SnapshotPhase.Unmark:
                    await UnmarkSourceShardAsync(shardIndex);
                    state.State.NextShardIndex++;
                    state.State.Phase = SnapshotPhase.Copy;
                    state.State.ShardRetries = 0;
                    await state.WriteStateAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Snapshot phase {Phase} failed for shard {ShardIndex} of tree {TreeId}",
                state.State.Phase, shardIndex, SourceTreeId);

            if (state.State.ShardRetries < MaxRetriesPerPhase)
            {
                state.State.ShardRetries++;
                await state.WriteStateAsync();
            }
            else
            {
                throw;
            }
        }
    }

    /// <summary>
    /// Drains live entries from the source shard's leaf chain into the
    /// destination shard. Uses the raw-LwwValue drain path so TTL
    /// (<c>ExpiresAtTicks</c>) and source HLC metadata are preserved on the
    /// destination tree — a snapshot of a key with remaining TTL reappears
    /// on the destination with the same absolute expiry, not a fresh
    /// zero-expiry entry.
    /// <para>
    /// For offline snapshots, the source shards are quiesced via
    /// <see cref="IShardRootGrain.MarkDeletedAsync"/> before drain begins,
    /// so the destination shard is guaranteed empty and we can use the
    /// efficient bottom-up <see cref="IShardRootGrain.BulkLoadRawAsync"/>
    /// path. For online snapshots, shadow-forwarding is active on every
    /// source shard before drain starts, so concurrent writes land on the
    /// destination shard via
    /// <see cref="IShardRootGrain.MergeManyAsync"/> before drain's batch
    /// arrives. We therefore use <see cref="IShardRootGrain.MergeManyAsync"/>
    /// for online mode too — its LWW semantics guarantee convergence
    /// regardless of which write wins the race: whichever carries the
    /// higher HLC is observable in the final destination state.
    /// </para>
    /// </summary>
    private async Task CopyShardAsync(int shardIndex)
    {
        var sourceShardKey = $"{SourceTreeId}/{shardIndex}";
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(sourceShardKey);
        var leafId = await sourceShard.GetLeftmostLeafIdAsync();

        var entries = new List<LwwEntry>();
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var liveRaw = await leaf.GetLiveRawEntriesAsync();
            entries.AddRange(liveRaw);
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (entries.Count == 0) return;

        var destShardKey = $"{state.State.DestinationTreeId}/{shardIndex}";
        var destShard = grainFactory.GetGrain<IShardRootGrain>(destShardKey);

        if (state.State.Mode == SnapshotMode.Online)
        {
            // Online drain: destination shard may already have entries from
            // concurrent shadow-forward writes. Use LWW MergeManyAsync so
            // the two populate streams converge — whichever entry carries
            // the higher HLC wins, per the CRDT invariant.
            var merge = new Dictionary<string, LwwValue<byte[]>>(entries.Count);
            foreach (var e in entries)
                merge[e.Key] = e.ToLwwValue();
            await destShard.MergeManyAsync(merge);
            return;
        }

        // Offline drain: source is locked, destination is guaranteed empty.
        // Use the bottom-up bulk-load path for minimal storage I/O.
        entries.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
        var operationId = $"{state.State.OperationId}-snapshot-{shardIndex}";
        await destShard.BulkLoadRawAsync(operationId, entries);
    }

    private async Task UnmarkSourceShardAsync(int shardIndex)
    {
        var shardKey = $"{SourceTreeId}/{shardIndex}";
        var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);
        await shard.UnmarkDeletedAsync();
    }

    /// <summary>
    /// Begins shadow-forwarding on every source shard. Must complete before
    /// any drain reader starts so that live writes landing during drain are
    /// mirrored to the destination tree. Exposed as <c>internal</c> for unit
    /// testing.
    /// </summary>
    internal async Task BeginShadowForwardAllShardsAsync()
    {
        var opId = state.State.OperationId
            ?? throw new InvalidOperationException(
                $"Snapshot state for tree '{SourceTreeId}' has no OperationId; cannot begin shadow forward.");
        var destinationTreeId = state.State.DestinationTreeId
            ?? throw new InvalidOperationException(
                $"Snapshot state for tree '{SourceTreeId}' has no DestinationTreeId; cannot begin shadow forward.");

        // Fall back to SourceTreeId when no logical name was threaded in —
        // preserves offline/standalone-snapshot behaviour where the source
        // grain key already IS the user-visible name.
        var logicalTreeId = string.IsNullOrEmpty(state.State.LogicalTreeId)
            ? SourceTreeId
            : state.State.LogicalTreeId;

        var shardCount = state.State.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}");
            tasks[i] = shard.BeginShadowForwardAsync(destinationTreeId, opId, logicalTreeId);
        }
        await Task.WhenAll(tasks);

        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Drains every remaining source shard into the destination with bounded
    /// concurrency (<see cref="LatticeOptions.MaxConcurrentDrains"/>). Each
    /// shard is copied then transitioned to
    /// <c>ShadowForwardPhase.Drained</c>. Online-mode only. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task DrainAllShardsOnlineAsync()
    {
        var shardCount = state.State.ShardCount;
        var start = state.State.NextShardIndex;
        var cap = Math.Max(1, Options.MaxConcurrentDrains);

        using var sem = new SemaphoreSlim(cap);
        var tasks = new List<Task>(shardCount - start);
        for (int i = start; i < shardCount; i++)
        {
            var idx = i;
            await sem.WaitAsync();
            tasks.Add(DrainOneShardOnlineAsync(idx, sem));
        }
        await Task.WhenAll(tasks);

        state.State.NextShardIndex = shardCount;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();
    }

    private async Task DrainOneShardOnlineAsync(int shardIndex, SemaphoreSlim sem)
    {
        try
        {
            await CopyShardAsync(shardIndex);
            await MarkShardDrainedAsync(shardIndex);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Transitions a single source shard from
    /// <c>ShadowForwardPhase.Draining</c> to <c>ShadowForwardPhase.Drained</c>.
    /// Online-mode only.
    /// </summary>
    private async Task MarkShardDrainedAsync(int shardIndex)
    {
        var opId = state.State.OperationId
            ?? throw new InvalidOperationException(
                $"Snapshot state for tree '{SourceTreeId}' has no OperationId; cannot mark shard drained.");
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{shardIndex}");
        await shard.MarkDrainedAsync(opId);
    }

    internal async Task CompleteSnapshotAsync()
    {
        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.Phase = SnapshotPhase.Lock;
        await state.WriteStateAsync();

        // Ensure tombstone compaction is active on the destination tree.
        var destCompaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(state.State.DestinationTreeId!);
        await destCompaction.EnsureReminderAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, SourceTreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "snapshot"));

        await PublishSnapshotCompletedAsync();

        await CompleteCoordinatorAsync();
    }

    private async Task PublishSnapshotCompletedAsync()
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, SourceTreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.SnapshotCompleted, SourceTreeId);
        await LatticeEventPublisher.PublishAsync(Context.ActivationServices, opts, evt, Logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <inheritdoc />
    public async Task AbortAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        // Idempotent — nothing to abort.
        if (!state.State.InProgress) return;

        // Refuse to abort a snapshot started under a different operationId.
        // This prevents a stale coordinator from tearing down a newer operation.
        if (!string.Equals(state.State.OperationId, operationId, StringComparison.Ordinal))
            return;

        // Clear all in-flight state so the grain deactivates cleanly. Shadow-
        // forward state on the source shards is the coordinator's responsibility
        // to clear (via ClearShadowForwardAsync); the snapshot grain does not
        // touch it here because the coordinator may want to preserve it across
        // retries.
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.DestinationTreeId = null;
        state.State.OperationId = null;
        state.State.MaxLeafKeys = null;
        state.State.MaxInternalChildren = null;
        state.State.LogicalTreeId = "";
        await state.WriteStateAsync();

        await CompleteCoordinatorAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!state.State.InProgress);
}
