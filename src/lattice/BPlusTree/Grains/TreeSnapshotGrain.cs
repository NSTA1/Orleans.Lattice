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
            throw;
        }
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

                    // Snapshot the fields the Copy-success flip mutates
                    // so a failing persist doesn't leak Phase=Unmark/Copy /
                    // NextShardIndex+1 / ShardRetries=0 ahead of disk. The
                    // outer try/catch below would otherwise see the dirty
                    // in-memory state, increment ShardRetries from the
                    // already-zeroed value, and on a subsequent re-entry
                    // skip a shard (NextShardIndex was advanced) while disk
                    // still pointed at this one. Bundled with the
                    // high-priority guarded sites per the same-grain Class B
                    // rule.
                    var prevPhaseCopy = state.State.Phase;
                    var prevNextShardIndexCopy = state.State.NextShardIndex;
                    var prevShardRetriesCopy = state.State.ShardRetries;
                    var prevCopyCursorKey = state.State.CopyCursorKey;

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
    /// path. For online snapshots, shadow-forwarding is active on every
    /// source shard before drain starts, so concurrent writes land on the
    /// destination shard via
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/> before drain's batch
    /// arrives. We therefore use <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>
    /// for online mode too - its LWW semantics guarantee convergence
    /// regardless of which write wins the race: whichever carries the
    /// higher HLC is observable in the final destination state.
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
    /// throughout, and every entry carries its source HLC, so a partially
    /// copied destination is a state this mode has always been able to present
    /// and every ordering converges to the same LWW result. An online snapshot
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
        var sourceShardKey = $"{SourceTreeId}/{shardIndex}";
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

        var entries = new List<LwwEntry>();
        while (walk.HasLeaf)
        {
            var liveRaw = await walk.CurrentLeaf.GetLiveRawEntriesAsync();
            entries.AddRange(liveRaw);
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
    /// Copies one shard through to the end of its leaf chain, running as many
    /// bounded passes as it takes and carrying the resume key in a local rather
    /// than in persisted state.
    /// <para>
    /// Used by the concurrent online drain, where several shards are in flight
    /// at once and so cannot share the single persisted
    /// <see cref="TreeSnapshotState.CopyCursorKey"/> - one shard's cursor would
    /// overwrite another's. Bounding each pass still holds peak memory to a
    /// batch of leaves rather than a whole shard.
    /// </para>
    /// <para>
    /// A fresh budget is built for every pass. A <see cref="LeafWalkBudget"/>
    /// fixes its wall-clock deadline at construction, so reusing one across
    /// passes would leave every pass after the first already past its deadline
    /// and yielding at the first leaf it could resume from.
    /// </para>
    /// </summary>
    private async Task CopyShardToEndAsync(int shardIndex, LatticeOptions options)
    {
        string? cursor = null;
        while (true)
        {
            var (complete, resumeFrom) = await CopyShardAsync(
                shardIndex, cursor, LeafWalkBudget.ForBackgroundDrain(options));
            if (complete) return;
            cursor = resumeFrom;
        }
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

        // Fall back to SourceTreeId when no logical name was threaded in -
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
        // Resolve options once; each pass builds its own budget from them, so
        // the leaf cap and the wall-clock net apply per pass rather than
        // across the whole concurrent drain.
        var drainOptions = await optionsResolver.ResolveAsync(SourceTreeId);
        for (int i = start; i < shardCount; i++)
        {
            var idx = i;
            await sem.WaitAsync();
            tasks.Add(DrainOneShardOnlineAsync(idx, sem, drainOptions));
        }
        await Task.WhenAll(tasks);

        // Snapshot the two fields the bulk-cursor advance mutates so a
        // failing persist doesn't leak NextShardIndex=shardCount /
        // ShardRetries=0 ahead of disk. The DrainOneShardOnlineAsync
        // side effects above are deliberately not reverted (each shard's
        // MarkDrainedAsync transition is idempotent on the operationId).
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        state.State.NextShardIndex = shardCount;
        state.State.ShardRetries = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            throw;
        }
    }

    private async Task DrainOneShardOnlineAsync(int shardIndex, SemaphoreSlim sem, LatticeOptions options)
    {
        try
        {
            await CopyShardToEndAsync(shardIndex, options);
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

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.Phase = SnapshotPhase.Lock;
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
            throw;
        }

        // Ensure tombstone compaction is active on the destination tree.
        var destCompaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(state.State.DestinationTreeId!);
        await destCompaction.EnsureReminderAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, SourceTreeId),
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
            throw;
        }

        await CompleteCoordinatorAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!state.State.InProgress);
}
