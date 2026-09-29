using System.Diagnostics;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshots a source tree into a new destination tree, copying the live
/// entries of every physical shard the source's routing map names into the
/// destination shard with the same index. Supports offline mode (source
/// tree locked during copy) and online mode (source tree remains available).
/// <para>
/// The destination is registered with the source's pinned shard count, its
/// routing map, and its split allocation high-water mark (see
/// <see cref="InitiateSnapshotStateAsync"/>), so a slot routes to the same
/// physical index on both trees and an index-for-index copy and shadow-forward
/// land every key on the shard that owns it. The shards visited are the union
/// of <c>0</c> to <c>ShardCount - 1</c> and every index the map names
/// (<see cref="RoutedShardIndices"/>), so a shard an adaptive split allocated
/// above the pinned count is copied too (issue 3880). A copied entry is kept
/// only when the map routes it to the shard it was read from: a split leaves a
/// sealed copy of every entry it moved on the shard that gave the slots up,
/// hidden from reads there but returned by the drain's
/// <c>GetLiveRawEntriesAsync</c>, and copying it would resurrect the value
/// the key held when the split moved it.
/// </para>
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

    /// <summary>
    /// The physical tree whose shards the snapshot reads, pinned when it
    /// started (see <see cref="TreeSnapshotState.SourcePhysicalTreeId"/>).
    /// Every source-shard address goes through this rather than
    /// <see cref="SourceTreeId"/>, which after a resize names the retired copy.
    /// </summary>
    private string SourcePhysicalTreeId => string.IsNullOrEmpty(state.State.SourcePhysicalTreeId)
        ? SourceTreeId
        : state.State.SourcePhysicalTreeId;

    /// <summary>
    /// The physical shard indices this snapshot visits, in ascending order (see
    /// <see cref="TreeSnapshotState.ShardIndices"/>). <see cref="TreeSnapshotState.NextShardIndex"/>
    /// is a position in this array.
    /// </summary>
    private int[] CopiedShardIndices =>
        RoutedShardIndices.OrContiguous(state.State.ShardIndices, state.State.ShardCount);

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "snapshot-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {SourceTreeId}";

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null)
    {
        // A standalone snapshot owns the shadow-forward an online copy installs
        // on the source shards, so it releases it on completion; nothing else
        // would, and the source would keep mirroring every write into the
        // (by then independent) destination tree forever.
        await StartSnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren,
            Guid.NewGuid().ToString("N"), SourceTreeId, releasesShadowForwardOnCompletion: true);
    }

    /// <inheritdoc />
    public Task SnapshotWithOperationIdAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys, int? maxInternalChildren, string operationId, string logicalTreeId) =>
        StartSnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren,
            operationId, logicalTreeId, releasesShadowForwardOnCompletion: false);

    private async Task StartSnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys, int? maxInternalChildren, string operationId, string logicalTreeId,
        bool releasesShadowForwardOnCompletion)
    {
        ArgumentNullException.ThrowIfNull(destinationTreeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(logicalTreeId);

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, SourceTreeId, LatticeOperation.Admin);

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
        var registry = grainFactory.GetLatticeRegistry();
        if (await registry.ExistsAsync(destinationTreeId))
            throw new InvalidOperationException(
                $"Destination tree '{destinationTreeId}' already exists. Choose a new tree ID.");

        await InitiateSnapshotStateAsync(destinationTreeId, mode, sourceResolved.ShardCount,
            maxLeafKeys, maxInternalChildren, operationId, logicalTreeId,
            releasesShadowForwardOnCompletion);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Persists snapshot intent and registers the destination tree in the registry.
    /// For offline mode, sets <see cref="SnapshotPhase.Lock"/> so that shard marking
    /// is deferred to <see cref="LockSourceShardsAsync"/>. Exposed as <c>internal</c>
    /// for unit testing.
    /// </summary>
    /// <param name="releasesShadowForwardOnCompletion">
    /// Whether <see cref="CompleteSnapshotAsync"/> releases the online copy's
    /// shadow-forward on the source shards. <see langword="true"/> for a
    /// standalone snapshot; <see langword="false"/> for a coordinator that
    /// manages the shadow-forward itself.
    /// </param>
    internal async Task InitiateSnapshotStateAsync(string destinationTreeId, SnapshotMode mode,
        int shardCount, int? maxLeafKeys = null, int? maxInternalChildren = null,
        string? operationId = null, string? logicalTreeId = null,
        bool releasesShadowForwardOnCompletion = false)
    {
        var registry = grainFactory.GetLatticeRegistry();

        // Capture the source's routing. Splits and reshards write the routing
        // map and the split allocation high-water mark to the LOGICAL tree's
        // entry, never to the physical copy an alias points at, so the logical
        // id is what is read. System trees never carry a custom map, and reading
        // one would be a circular registry call.
        var routingTreeId = string.IsNullOrEmpty(logicalTreeId) ? SourceTreeId : logicalTreeId;
        var sourceEntry = routingTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            ? null
            : await registry.GetEntryAsync(routingTreeId);
        var sourceMap = sourceEntry?.ShardMap;
        var shardIndices = RoutedShardIndices.Resolve(shardCount, sourceMap);

        // Register the destination tree in the registry before any data is written.
        // Always seed the ShardCount pin from the source so the registry
        // resolver has a complete structural pin for the destination tree.
        // MaxLeafKeys / MaxInternalChildren are propagated only when the
        // caller overrode them (resize case); otherwise the registry-grain's
        // seeding fills defaults. The source's routing map and split
        // high-water mark are carried over so every slot routes to the same
        // physical index on both trees - the index-for-index copy and
        // shadow-forward depend on it - and a later split of the destination
        // allocates above every index the copy populated.
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = maxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = shardCount,
            DerivedFrom = releasesShadowForwardOnCompletion ? null : logicalTreeId,
            ShardMap = sourceMap is null
                ? null
                : new ShardMap { Slots = (int[])sourceMap.Slots.Clone(), Version = sourceMap.Version },
            NextShardIndex = sourceEntry?.NextShardIndex,
        };
        await registry.RegisterAsync(destinationTreeId, entry);

        // Pin the physical tree the copy reads. A resized tree's logical id
        // aliases its live data to the resized copy, while the shards under the
        // logical id itself are the retired copy (in its rejecting phase, then
        // soft-deleted and purged), so copying those would snapshot stale or
        // empty data. A resize coordinator addresses this grain by an already
        // resolved physical id, which resolves to itself. System trees never
        // resolve aliases: the registry is itself a system tree.
        var sourcePhysicalTreeId = SourceTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            ? SourceTreeId
            : await registry.ResolveAsync(SourceTreeId);
        if (string.IsNullOrEmpty(sourcePhysicalTreeId)) sourcePhysicalTreeId = SourceTreeId;

        // Snapshot every field the mutation set touches so a failing
        // WriteStateAsync leaves the activation observably equal to what
        // disk (and any future reactivation) see. Without this, the
        // in-memory InProgress / DestinationTreeId / Mode / OperationId
        // would survive the throw and the SnapshotAsync idempotency guard
        // at L73-84 would short-circuit subsequent retries on dirty values -
        // a transient storage failure becoming a permanent "snapshot never
        // started" state until the activation recycles. The cross-grain
        // registry.RegisterAsync above is intentionally not reverted: it
        // is idempotent on the destination key and a retry will succeed
        // (or surface a separate failure) on its own merits.
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevCopyCursorKeyStart = state.State.CopyCursorKey;
        var prevDestinationTreeId = state.State.DestinationTreeId;
        var prevMode = state.State.Mode;
        var prevOperationId = state.State.OperationId;
        var prevShardCount = state.State.ShardCount;
        var prevMaxLeafKeys = state.State.MaxLeafKeys;
        var prevMaxInternalChildren = state.State.MaxInternalChildren;
        var prevComplete = state.State.Complete;
        var prevLogicalTreeId = state.State.LogicalTreeId;
        var prevReleasesShadowForward = state.State.ReleasesShadowForwardOnCompletion;
        var prevSourcePhysicalTreeId = state.State.SourcePhysicalTreeId;
        var prevShardIndices = state.State.ShardIndices;
        var prevSourceShardMap = state.State.SourceShardMap;
        var prevDrainCursors = state.State.DrainCursors;
        var prevDrainedPositions = state.State.DrainedPositions;

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
        state.State.CopyCursorKey = null;
        state.State.DestinationTreeId = destinationTreeId;
        state.State.Mode = mode;
        state.State.OperationId = operationId ?? Guid.NewGuid().ToString("N");
        state.State.ShardCount = shardCount;
        state.State.MaxLeafKeys = maxLeafKeys;
        state.State.MaxInternalChildren = maxInternalChildren;
        state.State.Complete = false;
        state.State.LogicalTreeId = logicalTreeId ?? "";
        state.State.ReleasesShadowForwardOnCompletion = releasesShadowForwardOnCompletion;
        state.State.SourcePhysicalTreeId = sourcePhysicalTreeId;
        state.State.ShardIndices = shardIndices;
        state.State.SourceShardMap = sourceMap;
        state.State.DrainCursors = null;
        state.State.DrainedPositions = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.CopyCursorKey = prevCopyCursorKeyStart;
            state.State.DestinationTreeId = prevDestinationTreeId;
            state.State.Mode = prevMode;
            state.State.OperationId = prevOperationId;
            state.State.ShardCount = prevShardCount;
            state.State.MaxLeafKeys = prevMaxLeafKeys;
            state.State.MaxInternalChildren = prevMaxInternalChildren;
            state.State.Complete = prevComplete;
            state.State.LogicalTreeId = prevLogicalTreeId;
            state.State.ReleasesShadowForwardOnCompletion = prevReleasesShadowForward;
            state.State.SourcePhysicalTreeId = prevSourcePhysicalTreeId;
            state.State.ShardIndices = prevShardIndices;
            state.State.SourceShardMap = prevSourceShardMap;
            state.State.DrainCursors = prevDrainCursors;
            state.State.DrainedPositions = prevDrainedPositions;
            throw;
        }
    }

    /// <summary>
    /// Marks every source shard the copy reads (see
    /// <see cref="TreeSnapshotState.ShardIndices"/>) as deleted. Called once when the
    /// <see cref="SnapshotPhase.Lock"/> phase is processed (offline mode only).
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task LockSourceShardsAsync()
    {
        var shardIndices = CopiedShardIndices;
        var tasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourcePhysicalTreeId}/{shardIndices[i]}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        // Snapshot the two fields the Lock->Copy flip mutates so a failing
        // persist doesn't leak Phase=Copy / ShardRetries=0 ahead of disk.
        // Bundled with the high-priority guarded sites per the same-grain
        // Class B rule: this site self-heals via Phase replay on a
        // subsequent reactivation, but a concurrent reader on the dirty
        // in-memory Phase could observe Copy while disk still says Lock.
        var prevPhase = state.State.Phase;
        var prevShardRetries = state.State.ShardRetries;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.ShardRetries = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.ShardRetries = prevShardRetries;
            throw;
        }
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
            && state.State.NextShardIndex < CopiedShardIndices.Length)
        {
            await DrainAllShardsOnlineAsync();
        }
        else
        {
            while (state.State.NextShardIndex < CopiedShardIndices.Length)
            {
                await ProcessCurrentPhaseAsync();
            }
        }

        await CompleteSnapshotAsync();
    }

    /// <summary>
    /// The longest a single <see cref="RunSnapshotSliceAsync"/> call may keep
    /// advancing before it banks its progress and returns. It sits well below
    /// Orleans' default thirty-second response timeout, so the caller never times
    /// out a slice that is behaving correctly, and it caps a large
    /// <see cref="LatticeOptions.BackgroundDrainMaxDuration"/> - which has no upper
    /// bound of its own - for the same reason (issue 3904).
    /// </summary>
    internal static readonly TimeSpan MaxSnapshotSliceDuration = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The wall-clock length of one snapshot slice:
    /// <see cref="LatticeOptions.BackgroundDrainMaxDuration"/> capped at
    /// <see cref="MaxSnapshotSliceDuration"/>. A zero or negative value, which
    /// disables the per-pass wall clock elsewhere, does not disable the slice: the
    /// slice bound is exactly the guarantee a timer-driven caller relies on, so it
    /// falls back to the cap instead.
    /// </summary>
    internal static TimeSpan SliceDuration(LatticeOptions options)
    {
        var configured = options.BackgroundDrainMaxDuration;
        return configured > TimeSpan.Zero && configured < MaxSnapshotSliceDuration
            ? configured
            : MaxSnapshotSliceDuration;
    }

    private static bool SliceExpired(long sliceStart, TimeSpan sliceDuration) =>
        Stopwatch.GetElapsedTime(sliceStart) >= sliceDuration;

    /// <inheritdoc />
    public async Task<bool> RunSnapshotSliceAsync()
    {
        if (!state.State.InProgress) return true;

        var sliceStart = Stopwatch.GetTimestamp();
        var drainOptions = await optionsResolver.ResolveAsync(SourceTreeId);
        var sliceDuration = SliceDuration(drainOptions);

        // Always take at least one step, so a slice whose prologue already spent
        // the budget still makes progress; then keep stepping until the snapshot
        // completes or the slice expires. Each step is itself bounded: the phase
        // transitions are single fan-outs, and the online copy yields at the
        // slice deadline.
        do
        {
            if (state.State.Mode == SnapshotMode.Online
                && state.State.Phase == SnapshotPhase.Copy
                && state.State.NextShardIndex < CopiedShardIndices.Length)
            {
                await DrainOnlineSliceAsync(drainOptions, sliceStart, sliceDuration);
            }
            else
            {
                await ProcessNextPhaseAsync();
            }
        }
        while (state.State.InProgress && !SliceExpired(sliceStart, sliceDuration));

        return !state.State.InProgress;
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

        if (state.State.NextShardIndex >= CopiedShardIndices.Length)
        {
            await CompleteSnapshotAsync();
            return;
        }

        await ProcessCurrentPhaseAsync();
    }

    private async Task ProcessCurrentPhaseAsync()
    {
        var shardIndex = CopiedShardIndices[state.State.NextShardIndex];

        try
        {
            switch (state.State.Phase)
            {
                case SnapshotPhase.Copy:
                    var cursorBeforeCopy = state.State.CopyCursorKey;
                    var copyBudget = state.State.Mode == SnapshotMode.Online
                        ? LeafWalkBudget.ForBackgroundDrain(await optionsResolver.ResolveAsync(SourceTreeId))
                        : LeafWalkBudget.Unbounded();
                    var (copyComplete, copyResumeFrom) = await CopyShardAsync(
                        shardIndex, cursorBeforeCopy, copyBudget);

                    if (!copyComplete)
                    {
                        // A bounded online pass that yielded. Persist the resume
                        // position and stay on this shard and phase; the next
                        // tick continues from the key. The retry budget is reset
                        // only when the cursor actually moved, so a partial pass
                        // counts as progress rather than as a failed attempt and
                        // a large-but-healthy shard is never retried out.
                        var madeProgress = !string.Equals(
                            copyResumeFrom, cursorBeforeCopy, StringComparison.Ordinal);
                        var prevRetriesPartial = state.State.ShardRetries;
                        state.State.CopyCursorKey = copyResumeFrom;
                        if (madeProgress) state.State.ShardRetries = 0;
                        try
                        {
                            await state.WriteStateAsync();
                        }
                        catch
                        {
                            state.State.CopyCursorKey = cursorBeforeCopy;
                            state.State.ShardRetries = prevRetriesPartial;
                            throw;
                        }
                        break;
                    }

                    if (state.State.Mode == SnapshotMode.Online)
                    {
                        // Online mode: mark this shard drained (shadow-forward
                        // continues until the coordinator transitions to
                        // Rejecting) and advance the head past it - and past any
                        // later shard a concurrent slice already drained - taking
                        // up the new head's banked cursor.
                        await MarkShardDrainedAsync(shardIndex);
                        await CommitDrainProgressAsync(
                            [new ShardDrainOutcome(state.State.NextShardIndex, Drained: true, null, Progressed: true, null)]);
                        break;
                    }

                    // Snapshot the fields the Copy-success flip mutates
                    // so a failing persist doesn't leak Phase=Unmark /
                    // ShardRetries=0 ahead of disk. The outer try/catch below
                    // would otherwise see the dirty in-memory state and
                    // increment ShardRetries from the already-zeroed value.
                    // Bundled with the high-priority guarded sites per the
                    // same-grain Class B rule.
                    var prevPhaseCopy = state.State.Phase;
                    var prevNextShardIndexCopy = state.State.NextShardIndex;
                    var prevShardRetriesCopy = state.State.ShardRetries;
                    var prevCopyCursorKey = state.State.CopyCursorKey;

                    state.State.Phase = SnapshotPhase.Unmark;
                    state.State.ShardRetries = 0;
                    // Each shard owns its own sweep, so the cursor never carries
                    // across a shard advance - a stale key would re-descend into
                    // the wrong shard's keyspace.
                    state.State.CopyCursorKey = null;
                    try
                    {
                        await state.WriteStateAsync();
                    }
                    catch
                    {
                        state.State.Phase = prevPhaseCopy;
                        state.State.NextShardIndex = prevNextShardIndexCopy;
                        state.State.ShardRetries = prevShardRetriesCopy;
                        state.State.CopyCursorKey = prevCopyCursorKey;
                        throw;
                    }
                    break;

                case SnapshotPhase.Unmark:
                    await UnmarkSourceShardAsync(shardIndex);

                    // Same shape as the Copy-success branch: snapshot the
                    // three fields the Unmark-success flip mutates so a
                    // failing persist doesn't leak Phase=Copy /
                    // NextShardIndex+1 / ShardRetries=0 ahead of disk.
                    var prevPhaseUnmark = state.State.Phase;
                    var prevNextShardIndexUnmark = state.State.NextShardIndex;
                    var prevShardRetriesUnmark = state.State.ShardRetries;
                    var prevCopyCursorUnmark = state.State.CopyCursorKey;

                    state.State.NextShardIndex++;
                    state.State.Phase = SnapshotPhase.Copy;
                    state.State.ShardRetries = 0;
                    state.State.CopyCursorKey = null;
                    try
                    {
                        await state.WriteStateAsync();
                    }
                    catch
                    {
                        state.State.Phase = prevPhaseUnmark;
                        state.State.NextShardIndex = prevNextShardIndexUnmark;
                        state.State.ShardRetries = prevShardRetriesUnmark;
                        state.State.CopyCursorKey = prevCopyCursorUnmark;
                        throw;
                    }
                    break;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Snapshot phase {Phase} failed for shard {ShardIndex} of tree {TreeId}",
                state.State.Phase, shardIndex, SourceTreeId);

            if (state.State.ShardRetries < MaxRetriesPerPhase)
            {
                // Snapshot the retry counter so a failing persist of the
                // retry-bump doesn't leak ShardRetries++ ahead of disk - on
                // reactivation the budget check would observe the dirty
                // counter while disk holds the pre-bump value, double-burning
                // retries in lock-step with reactivation.
                var prevShardRetries = state.State.ShardRetries;
                state.State.ShardRetries++;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.ShardRetries = prevShardRetries;
                    throw;
                }
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
    /// destination tree - a snapshot of a key with remaining TTL reappears
    /// on the destination with the same absolute expiry, not a fresh
    /// zero-expiry entry.
    /// <para>
    /// For offline snapshots, the source shards are quiesced via
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MarkDeletedAsync"/> before drain begins,
    /// so the destination shard is guaranteed empty and we can use the
    /// efficient bottom-up <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.BulkLoadRawAsync"/>
    /// path. For online snapshots, shadow-forwarding is active on every copied
    /// source shard before drain starts, so concurrent writes can reach the
    /// destination shard - each through its own forwarded call - before the
    /// drain's batch arrives. We therefore merge with
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>
    /// for online mode too - its LWW semantics keep whichever version of a key
    /// carries the higher HLC. A forwarded write is stamped by the destination
    /// leaf rather than carrying the source's HLC (see
    /// <see cref="ShardRootGrain"/>'s shadow-forward notes), so which version
    /// that is can depend on whether the drained entry or the forward arrived
    /// first.
    /// </para>
    /// <para>
    /// <b>The online copy is work-bounded and resumable; the offline copy is
    /// deliberately atomic</b> (issue 1973).
    /// </para>
    /// <para>
    /// <i>Online.</i> One pass visits at most
    /// <see cref="LatticeOptions.BackgroundDrainLeavesPerPass"/> source leaves,
    /// merges what it read, and persists the key the next pass re-descends
    /// onto. A pass boundary makes nothing observable that a shard boundary did
    /// not already: the destination tree is populated shard by shard across
    /// timer ticks anyway, shadow-forwarding mirrors concurrent writes onto it
    /// throughout, and every copied entry carries its source HLC, so a
    /// partially copied destination is a state this mode has always been able
    /// to present. An online snapshot
    /// is a converging mirror, not a point-in-time image, so there is no
    /// instant of consistency for a bound to break.
    /// </para>
    /// <para>
    /// <i>Offline.</i> <b>DELIBERATELY NOT WORK-BOUNDED.</b> The offline copy
    /// assembles the destination shard bottom-up through
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.BulkLoadRawAsync"/>,
    /// which by contract refuses a shard that already has a root node and needs
    /// the complete sorted entry set in one call. There is therefore no
    /// intermediate state for a cursor to name: a bounded pass could only
    /// resume by abandoning the bulk-load path for per-pass merges, which would
    /// trade a single bottom-up build for repeated top-down inserts on every
    /// offline snapshot and restore. The source is quiesced by
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MarkDeletedAsync"/>
    /// before the copy begins, so nothing it reads can change while it runs.
    /// Made attributable through <see cref="AtomicLeafWalk"/> instead.
    /// </para>
    /// </summary>
    /// <returns>
    /// Whether the source shard's whole leaf chain has been copied, and the key
    /// the next pass resumes from when it has not. The offline path always
    /// reports the copy complete.
    /// </returns>
    private async Task<(bool CopyComplete, string? ResumeFromInclusive)> CopyShardAsync(
        int shardIndex,
        string? resumeFromInclusive,
        LeafWalkBudget budget)
    {
        var sourceShardKey = $"{SourcePhysicalTreeId}/{shardIndex}";
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(sourceShardKey);
        var offline = state.State.Mode != SnapshotMode.Online;

        var atomicWalk = offline ? new AtomicLeafWalk("SnapshotOfflineCopyShardAsync") : default;

        // The offline path never resumes and is never bounded: it must read the
        // source's whole chain so the bulk load receives the complete sorted
        // set. Forcing both here rather than trusting the caller means a stray
        // cursor or budget can never turn a bulk load into a partial one, which
        // would be silent data loss on the destination.
        var walk = await BoundedLeafWalk.StartAsync(
            grainFactory,
            sourceShard,
            offline ? null : resumeFromInclusive,
            offline ? LeafWalkBudget.Unbounded() : budget);

        // Keep only the entries the source's routing map sends to this shard.
        // An adaptive split leaves a sealed copy of every entry it moved on the
        // shard that gave the slots up; reads there hide it, but this raw drain
        // does not, and because the destination routes by the same map those
        // entries would otherwise resurrect the values the keys held when the
        // split moved them (issue 3880). A source on its default routing holds
        // no such copies, so it skips the per-entry hash.
        var sourceMap = state.State.SourceShardMap;
        var entries = new List<LwwEntry>();
        while (walk.HasLeaf)
        {
            var liveRaw = await walk.CurrentLeaf.GetLiveRawEntriesAsync();
            if (sourceMap is null)
            {
                entries.AddRange(liveRaw);
            }
            else
            {
                foreach (var entry in liveRaw)
                {
                    if (sourceMap.Resolve(entry.Key) == shardIndex) entries.Add(entry);
                }
            }
            if (!await walk.MoveNextAsync()) break;
        }

        if (offline)
        {
            atomicWalk.RecordLeavesVisited(walk.LeavesVisited);
            atomicWalk.ReportIfSlow(Logger, Context.GrainId);

            if (entries.Count == 0) return (true, null);

            // Offline drain: source is locked, destination is guaranteed empty.
            // Use the bottom-up bulk-load path for minimal storage I/O.
            entries.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
            var operationId = $"{state.State.OperationId}-snapshot-{shardIndex}";
            var offlineDest = grainFactory.GetGrain<IShardRootGrain>($"{state.State.DestinationTreeId}/{shardIndex}");
            await offlineDest.BulkLoadRawAsync(operationId, entries);
            return (true, null);
        }

        // Online drain: destination shard may already have entries from
        // concurrent shadow-forward writes. Use LWW MergeManyAsync so
        // the two populate streams converge - whichever entry carries
        // the higher HLC wins, per the CRDT invariant.
        //
        // The merge is issued before the cursor is returned, so the position
        // the caller persists is never ahead of the entries the destination has
        // accepted; a cursor past an unmerged batch would drop those entries
        // permanently, because the next pass resumes beyond them.
        if (entries.Count > 0)
        {
            var destShard = grainFactory.GetGrain<IShardRootGrain>($"{state.State.DestinationTreeId}/{shardIndex}");
            var merge = new Dictionary<string, LwwValue<byte[]>>(entries.Count);
            foreach (var e in entries)
                merge[e.Key] = e.ToLwwValue();
            await destShard.MergeManyAsync(merge);
        }

        return (walk.Completed, walk.ResumeFromInclusive);
    }

    /// <summary>
    /// The outcome of one shard's share of a concurrent online drain slice.
    /// </summary>
    /// <param name="Position">The shard's position in <see cref="CopiedShardIndices"/>.</param>
    /// <param name="Drained">Whether the shard's whole leaf chain was copied and the shard marked drained.</param>
    /// <param name="ResumeFrom">The key the shard's next pass resumes from when it is not drained; <see langword="null"/> to start at its leftmost leaf.</param>
    /// <param name="Progressed">Whether the shard's copy moved forward in this slice.</param>
    /// <param name="Fault">The failure that stopped the shard's copy, if any; its progress up to the fault is still banked.</param>
    private readonly record struct ShardDrainOutcome(
        int Position, bool Drained, string? ResumeFrom, bool Progressed, Exception? Fault);

    /// <summary>
    /// The resume key of the shard at <paramref name="position"/>: the head
    /// shard's lives in <see cref="TreeSnapshotState.CopyCursorKey"/>, every
    /// later one's in <see cref="TreeSnapshotState.DrainCursors"/>.
    /// </summary>
    private string? CursorFor(int position) =>
        position == state.State.NextShardIndex
            ? state.State.CopyCursorKey
            : state.State.DrainCursors is { } cursors && cursors.TryGetValue(position, out var key) ? key : null;

    /// <summary>
    /// Copies one shard for as many bounded passes as fit in the current slice.
    /// Every pass shares the slice's deadline, so the copy stops at the first
    /// leaf past it rather than being granted a fresh wall-clock allowance per
    /// pass. A fault is returned, not thrown, so the progress every other shard
    /// made in the same slice is still banked before it surfaces.
    /// </summary>
    private async Task<ShardDrainOutcome> DrainShardForSliceAsync(
        int position, int shardIndex, string? cursor, LatticeOptions options,
        long sliceStart, TimeSpan sliceDuration, SemaphoreSlim sem)
    {
        var progressed = false;
        try
        {
            while (true)
            {
                var budget = new LeafWalkBudget(options.BackgroundDrainLeavesPerPass, sliceDuration, sliceStart);
                var (complete, resumeFrom) = await CopyShardAsync(shardIndex, cursor, budget);
                if (complete)
                {
                    await MarkShardDrainedAsync(shardIndex);
                    return new ShardDrainOutcome(position, Drained: true, null, Progressed: true, null);
                }

                progressed |= !string.Equals(resumeFrom, cursor, StringComparison.Ordinal);
                cursor = resumeFrom;
                if (SliceExpired(sliceStart, sliceDuration))
                    return new ShardDrainOutcome(position, Drained: false, cursor, progressed, null);
            }
        }
        catch (Exception ex)
        {
            return new ShardDrainOutcome(position, Drained: false, cursor, progressed, ex);
        }
        finally
        {
            sem.Release();
        }
    }

    private async Task UnmarkSourceShardAsync(int shardIndex)
    {
        var shardKey = $"{SourcePhysicalTreeId}/{shardIndex}";
        var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);
        await shard.UnmarkDeletedAsync();
    }

    /// <summary>
    /// Begins shadow-forwarding on every source shard the copy reads (see
    /// <see cref="TreeSnapshotState.ShardIndices"/>). Must complete before
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

        // Fall back to SourceTreeId when no logical name was threaded in -
        // preserves offline/standalone-snapshot behaviour where the source
        // grain key already IS the user-visible name.
        var logicalTreeId = string.IsNullOrEmpty(state.State.LogicalTreeId)
            ? SourceTreeId
            : state.State.LogicalTreeId;

        var shardIndices = CopiedShardIndices;
        var tasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourcePhysicalTreeId}/{shardIndices[i]}");
            tasks[i] = shard.BeginShadowForwardAsync(destinationTreeId, opId, logicalTreeId);
        }
        await Task.WhenAll(tasks);

        // Snapshot the three fields the ShadowBegin->Copy flip mutates so
        // a failing persist doesn't leak Phase=Copy / NextShardIndex=0 /
        // ShardRetries=0 ahead of disk. Bundled with the high-priority
        // guarded sites per the same-grain Class B rule: cross-grain
        // BeginShadowForwardAsync side effects on the source shards are
        // deliberately not reverted (they are idempotent on the
        // operationId + destinationTreeId tuple).
        var prevPhase = state.State.Phase;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            throw;
        }
    }

    /// <summary>
    /// Drains every remaining source shard into the destination, running
    /// wall-clock-bounded slices of the concurrent drain back to back until
    /// every shard is drained. Online-mode only. Unbounded in wall clock by
    /// design: it backs the run-to-completion <see cref="RunSnapshotPassAsync"/>.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task DrainAllShardsOnlineAsync()
    {
        var drainOptions = await optionsResolver.ResolveAsync(SourceTreeId);
        var sliceDuration = SliceDuration(drainOptions);
        while (state.State.NextShardIndex < CopiedShardIndices.Length)
        {
            await DrainOnlineSliceAsync(drainOptions, Stopwatch.GetTimestamp(), sliceDuration);
        }
    }

    /// <summary>
    /// Runs one wall-clock-bounded slice of the online drain: copies up to
    /// <see cref="LatticeOptions.MaxConcurrentDrains"/> source shards at once,
    /// each for as many bounded passes as fit before the slice deadline, then
    /// persists every shard's progress in one write (issue 3904). No new shard
    /// is started once the slice has expired, and a shard still copying when it
    /// expires stops at its next leaf and banks its resume key, so the slice
    /// returns the turn instead of holding it until the whole tree is copied.
    /// </summary>
    private async Task DrainOnlineSliceAsync(LatticeOptions drainOptions, long sliceStart, TimeSpan sliceDuration)
    {
        var shardIndices = CopiedShardIndices;
        var cap = Math.Max(1, Options.MaxConcurrentDrains);

        using var sem = new SemaphoreSlim(cap);
        var tasks = new List<Task<ShardDrainOutcome>>(Math.Min(cap, shardIndices.Length));
        for (int position = state.State.NextShardIndex; position < shardIndices.Length; position++)
        {
            // The head is never skipped: it is the one position a slice must
            // always be able to advance, so even a stray record naming it cannot
            // leave the drain spinning without launching any work.
            if (position != state.State.NextShardIndex
                && state.State.DrainedPositions?.Contains(position) == true) continue;

            await sem.WaitAsync();

            // The first shard always starts, so a slice whose budget was spent
            // before it got here still makes progress.
            if (tasks.Count > 0 && SliceExpired(sliceStart, sliceDuration))
            {
                sem.Release();
                break;
            }

            tasks.Add(DrainShardForSliceAsync(position, shardIndices[position], CursorFor(position),
                drainOptions, sliceStart, sliceDuration, sem));
        }

        var outcomes = await Task.WhenAll(tasks);
        await CommitDrainProgressAsync(outcomes);

        foreach (var outcome in outcomes)
        {
            if (outcome.Fault is { } fault) ExceptionDispatchInfo.Capture(fault).Throw();
        }
    }

    /// <summary>
    /// Persists the progress a set of online-drain outcomes represents: drained
    /// shards are recorded, the head advances past every leading drained shard
    /// and takes up the new head's banked cursor, and every partly copied
    /// shard's resume key is banked. The in-memory state is left untouched if
    /// the write fails, so a retry resumes from what disk actually holds.
    /// </summary>
    private async Task CommitDrainProgressAsync(IReadOnlyList<ShardDrainOutcome> outcomes)
    {
        var total = CopiedShardIndices.Length;
        var head = state.State.NextShardIndex;
        var headCursor = state.State.CopyCursorKey;
        var cursors = state.State.DrainCursors is { } existingCursors ? new Dictionary<int, string>(existingCursors) : null;
        var drained = state.State.DrainedPositions is { } existingDrained ? new HashSet<int>(existingDrained) : null;
        var progressed = false;
        var headDrained = false;

        foreach (var outcome in outcomes)
        {
            progressed |= outcome.Progressed;
            if (outcome.Drained)
            {
                cursors?.Remove(outcome.Position);
                if (outcome.Position == head) headDrained = true;
                else (drained ??= new HashSet<int>()).Add(outcome.Position);
            }
            else if (outcome.Position == head)
            {
                headCursor = outcome.ResumeFrom;
            }
            else if (outcome.ResumeFrom is { } key)
            {
                (cursors ??= new Dictionary<int, string>())[outcome.Position] = key;
            }
            else
            {
                cursors?.Remove(outcome.Position);
            }
        }

        var next = head;
        if (headDrained)
        {
            next++;
            while (next < total && drained is not null && drained.Remove(next)) next++;
            headCursor = cursors is not null && cursors.Remove(next, out var nextCursor) ? nextCursor : null;
        }

        if (cursors is { Count: 0 }) cursors = null;
        if (drained is { Count: 0 }) drained = null;

        var prevNextShardIndex = state.State.NextShardIndex;
        var prevCopyCursorKey = state.State.CopyCursorKey;
        var prevShardRetries = state.State.ShardRetries;
        var prevDrainCursors = state.State.DrainCursors;
        var prevDrainedPositions = state.State.DrainedPositions;

        state.State.NextShardIndex = next;
        state.State.CopyCursorKey = headCursor;
        state.State.DrainCursors = cursors;
        state.State.DrainedPositions = drained;
        if (progressed) state.State.ShardRetries = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.CopyCursorKey = prevCopyCursorKey;
            state.State.ShardRetries = prevShardRetries;
            state.State.DrainCursors = prevDrainCursors;
            state.State.DrainedPositions = prevDrainedPositions;
            throw;
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
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourcePhysicalTreeId}/{shardIndex}");
        await shard.MarkDrainedAsync(opId);
    }

    internal async Task CompleteSnapshotAsync()
    {
        // A standalone online snapshot releases the shadow-forward it put on
        // the source shards BEFORE the completion flip is persisted. The order
        // is what makes it crash-safe: a crash after the release but before
        // the persist re-enters here on reactivation, and the release is
        // idempotent (ClearShadowForwardAsync on an already-cleared shard is a
        // no-op), whereas persisting first would lose the obligation. Leaving
        // it in place kept every later source write mirroring into the
        // destination, refused every later online snapshot or resize of the
        // source (a shard takes part in one shadow-forward operation at a
        // time), and failed source writes outright once the destination tree
        // was deleted.
        if (state.State.Mode == SnapshotMode.Online && state.State.ReleasesShadowForwardOnCompletion)
        {
            await ReleaseSourceShadowForwardAsync();
        }

        // Snapshot every field the completion flip mutates. Without this,
        // a failing WriteStateAsync would leave InProgress=false /
        // Complete=true / Phase=Lock in memory while disk still says the
        // snapshot is running. IsIdleAsync (defined as `!InProgress`) would
        // then lie to callers; the keepalive reminder would still tick and
        // re-enter RunSnapshotPassAsync which now short-circuits at its
        // !InProgress guard - the snapshot halts on this activation while
        // disk-loaded reactivations would resume.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhase = state.State.Phase;
        var prevReleasesShadowForward = state.State.ReleasesShadowForwardOnCompletion;
        var prevShardIndices = state.State.ShardIndices;
        var prevSourceShardMap = state.State.SourceShardMap;
        var prevDrainCursors = state.State.DrainCursors;
        var prevDrainedPositions = state.State.DrainedPositions;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.ReleasesShadowForwardOnCompletion = false;
        state.State.ShardIndices = null;
        state.State.SourceShardMap = null;
        state.State.DrainCursors = null;
        state.State.DrainedPositions = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.Phase = prevPhase;
            state.State.ReleasesShadowForwardOnCompletion = prevReleasesShadowForward;
            state.State.ShardIndices = prevShardIndices;
            state.State.SourceShardMap = prevSourceShardMap;
            state.State.DrainCursors = prevDrainCursors;
            state.State.DrainedPositions = prevDrainedPositions;
            throw;
        }

        // Ensure tombstone compaction is active on the destination tree.
        var destCompaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(state.State.DestinationTreeId!);
        await destCompaction.EnsureReminderAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, LogicalMetricsTreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "snapshot"),
            LatticeTenantLabel.ForTree(SourceTreeId));

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

    /// <summary>
    /// Clears the shadow-forward this snapshot installed on every source shard
    /// (<see cref="BeginShadowForwardAllShardsAsync"/> covers the same
    /// <see cref="TreeSnapshotState.ShardIndices"/> under the same operation id),
    /// so writes to the source stop reaching the destination once the copy is
    /// complete. Idempotent per shard.
    /// </summary>
    private async Task ReleaseSourceShadowForwardAsync()
    {
        var opId = state.State.OperationId;
        if (string.IsNullOrEmpty(opId)) return;

        var shardIndices = CopiedShardIndices;
        var tasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourcePhysicalTreeId}/{shardIndices[i]}");
            tasks[i] = shard.ClearShadowForwardAsync(opId);
        }
        await Task.WhenAll(tasks);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <inheritdoc />
    public async Task AbortAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, SourceTreeId, LatticeOperation.Admin);

        // Idempotent - nothing to abort.
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
        // Snapshot every field the abort-clear mutates so a failing
        // WriteStateAsync doesn't leak InProgress=false / OperationId=null
        // ahead of disk. Without this, the L488 idempotency guard
        // `if (!state.State.InProgress) return` would short-circuit every
        // subsequent abort retry, and the L492
        // `if (!state.State.OperationId.Equals(operationId)) return` guard
        // on a dirty in-memory OperationId=null would silently no-op every
        // abort from any caller - a transient storage failure permanently
        // blocking abort recovery until activation recycles.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhase = state.State.Phase;
        var prevDestinationTreeId = state.State.DestinationTreeId;
        var prevOperationId = state.State.OperationId;
        var prevMaxLeafKeys = state.State.MaxLeafKeys;
        var prevMaxInternalChildren = state.State.MaxInternalChildren;
        var prevLogicalTreeId = state.State.LogicalTreeId;
        var prevReleasesShadowForward = state.State.ReleasesShadowForwardOnCompletion;
        var prevShardIndices = state.State.ShardIndices;
        var prevSourceShardMap = state.State.SourceShardMap;
        var prevDrainCursors = state.State.DrainCursors;
        var prevDrainedPositions = state.State.DrainedPositions;

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
        state.State.ReleasesShadowForwardOnCompletion = false;
        state.State.ShardIndices = null;
        state.State.SourceShardMap = null;
        state.State.DrainCursors = null;
        state.State.DrainedPositions = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.Phase = prevPhase;
            state.State.DestinationTreeId = prevDestinationTreeId;
            state.State.OperationId = prevOperationId;
            state.State.MaxLeafKeys = prevMaxLeafKeys;
            state.State.MaxInternalChildren = prevMaxInternalChildren;
            state.State.LogicalTreeId = prevLogicalTreeId;
            state.State.ReleasesShadowForwardOnCompletion = prevReleasesShadowForward;
            state.State.ShardIndices = prevShardIndices;
            state.State.SourceShardMap = prevSourceShardMap;
            state.State.DrainCursors = prevDrainCursors;
            state.State.DrainedPositions = prevDrainedPositions;
            throw;
        }

        await CompleteCoordinatorAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!state.State.InProgress);
}
