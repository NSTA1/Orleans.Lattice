using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bottom-up bulk-load and streaming bulk-append operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
    private static readonly Task<BulkOperationStatus>[] BulkOperationStatusTasks =
    [
        Task.FromResult(BulkOperationStatus.NotRunning),
        Task.FromResult(BulkOperationStatus.Running),
        Task.FromResult(BulkOperationStatus.Completed),
    ];

    /// <summary>
    /// The bulk load running on this activation, read by
    /// <see cref="GetBulkOperationStatusAsync"/> while the load's own turn is suspended
    /// (#5812). The shard root is not reentrant, so at most one load runs at a time.
    /// </summary>
    private string? _runningBulkOperationId;

    /// <summary>
    /// A bulk load that published its root and then faulted in its switch's tail, so
    /// its persisted completion is not the whole of it (review S1 of #5815). It reads
    /// as not running, so its caller re-issues it, and the re-issue's completed-id
    /// short-circuit closes the switch. Activation memory only: a fresh activation
    /// closes the switch by the idle close it arms instead.
    /// </summary>
    private string? _faultedBulkOperationId;

    /// <inheritdoc />
    public Task<BulkOperationStatus> GetBulkOperationStatusAsync(string operationId)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        // The load records itself complete when it publishes its root, before its
        // switch's tail has relocated the replaced root and closed by fence. So Running
        // wins, or a caller could proceed or cut over mid-tail; and a load whose tail
        // faulted reads as not running, or nobody would re-issue it to close the switch.
        BulkOperationStatus status;
        if (string.Equals(_runningBulkOperationId, operationId, StringComparison.Ordinal))
        {
            status = BulkOperationStatus.Running;
        }
        else if (string.Equals(_faultedBulkOperationId, operationId, StringComparison.Ordinal))
        {
            status = BulkOperationStatus.NotRunning;
        }
        else
        {
            status = state.State.LastCompletedBulkOperationId == operationId
                ? BulkOperationStatus.Completed
                : BulkOperationStatus.NotRunning;
        }

        return BulkOperationStatusTasks[(int)status];
    }

    public async Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        _runningBulkOperationId = operationId;
        try
        {
            await BulkLoadCoreAsync(operationId, sortedEntries);
            ClearFaultedBulkLoad(operationId);
        }
        catch
        {
            NoteBulkLoadFaulted(operationId);
            throw;
        }
        finally
        {
            _runningBulkOperationId = null;
        }
    }

    /// <inheritdoc />
    public async Task BulkLoadRawAsync(string operationId, List<LwwEntry> sortedEntries)
    {
        _runningBulkOperationId = operationId;
        try
        {
            await BulkLoadRawCoreAsync(operationId, sortedEntries);
            ClearFaultedBulkLoad(operationId);
        }
        catch
        {
            NoteBulkLoadFaulted(operationId);
            throw;
        }
        finally
        {
            _runningBulkOperationId = null;
        }
    }

    private void ClearFaultedBulkLoad(string operationId)
    {
        if (string.Equals(_faultedBulkOperationId, operationId, StringComparison.Ordinal))
        {
            _faultedBulkOperationId = null;
        }
    }

    /// <summary>
    /// Review S1 of #5815: a load that faulted after it published its root left its
    /// switch open with its completion persisted. It is remembered so its status reads
    /// as not running and its caller re-issues it, and the idle close is armed so the
    /// switch is closed even when nobody does. A load that faulted before it published
    /// reads as not running already. Runs only on the failure path.
    /// </summary>
    private void NoteBulkLoadFaulted(string operationId)
    {
        if (state.State.LastCompletedBulkOperationId != operationId)
        {
            return;
        }

        _faultedBulkOperationId = operationId;
        if (HasHandOffStorage())
        {
            ArmBulkIdleClose();
        }
    }

    /// <summary>
    /// The completed-id short-circuit of a same-operation re-issue: closes the switch the
    /// load left open. When this activation remembers the load as faulted after it
    /// published, the publish's own persist may be the step that failed, so the shard-root
    /// state is persisted before the re-issue reports success (#5829). A fresh activation
    /// read its completion from storage, so it writes nothing here.
    /// </summary>
    private async Task FinishCompletedBulkLoadAsync(string operationId)
    {
        if (string.Equals(_faultedBulkOperationId, operationId, StringComparison.Ordinal))
        {
            await WriteShardStateAsync();
        }

        await FinishBulkSwitchAsync(operationId, [], relocated: false, closeNow: true);
    }

    private async Task BulkLoadCoreAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        ThrowIfRetired();
        await AdmitPurgedCopyForBulkWriteAsync();
        if (state.State.LastCompletedBulkOperationId == operationId)
        {
            // A crash after the load published its root and before its switch closed
            // leaves the marker open and the hint stored. The replaced root is no
            // longer known, so the leaves keep reading from the log's start, which
            // reaches any row it held; the switch is closed without the hint.
            await FinishCompletedBulkLoadAsync(operationId);
            return;
        }

        var seededRoot = await EnsureEmptyForBulkLoadAsync(nameof(BulkLoadAsync), operationId);

        if (sortedEntries.Count == 0) return;

        // Stamp every entry's version in one sequential pass up front. The
        // clock is a strictly increasing sequence, so it cannot be ticked from
        // inside the overlapped wave below; ticking it here yields exactly the
        // versions the serial loop assigned, entry for entry.
        var clocks = new HybridLogicalClock[sortedEntries.Count];
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < clocks.Length; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            clocks[i] = clock;
        }

        await RunBulkLoadAsync(operationId, seededRoot, "bulk", sortedEntries.Count, i => sortedEntries[i].Key, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var kv = sortedEntries[start + j];
                batch[kv.Key] = LwwValue<byte[]>.Create(kv.Value, clocks[start + j]);
            }

            return batch;
        });
    }

    private async Task BulkLoadRawCoreAsync(
        string operationId,
        List<LwwEntry> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        ThrowIfRetired();
        await AdmitPurgedCopyForBulkWriteAsync();
        if (state.State.LastCompletedBulkOperationId == operationId)
        {
            // A crash after the load published its root and before its switch closed
            // leaves the marker open and the hint stored. The replaced root is no
            // longer known, so the leaves keep reading from the log's start, which
            // reaches any row it held; the switch is closed without the hint.
            await FinishCompletedBulkLoadAsync(operationId);
            return;
        }

        var seededRoot = await EnsureEmptyForBulkLoadAsync(nameof(BulkLoadRawAsync), operationId);

        if (sortedEntries.Count == 0) return;

        // Identical B+ tree assembly to BulkLoadAsync - the only difference is
        // that every entry's LwwValue (HLC version AND ExpiresAtTicks /
        // TTL) flows through verbatim instead of being re-stamped with a fresh
        // zero-based clock. Used by snapshot / restore (TreeSnapshotGrain) so
        // TTL metadata survives the transfer end-to-end.
        await RunBulkLoadAsync(operationId, seededRoot, "bulkraw", sortedEntries.Count, i => sortedEntries[i].Key, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var e = sortedEntries[start + j];
                batch[e.Key] = e.ToLwwValue();
            }

            return batch;
        });
    }

    /// <summary>
    /// The bottom-up build shared by <see cref="BulkLoadAsync"/> and
    /// <see cref="BulkLoadRawAsync"/>, once the shard has been found empty.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Switched.</b> A shard that has a root - the empty seeded leaf - opens its
    /// bulk switch before the first leaf is born (the birth hint and the marker), seeds
    /// under it with the bulk-seed stamp class, publishes, relocates whatever a batch
    /// write stored on the replaced root (issue #5642), and closes by fence.
    /// </para>
    /// <para>
    /// <b>Never written (#5812, B2).</b> A shard with no root has no leaf, so it holds
    /// no row for a switch to protect and no write in flight to drain. It opens no
    /// marker, captures no hint and runs no fence: its leaves are seeded unstamped,
    /// which every partition admits and reports, and they read from the log's start as
    /// a fresh shard's leaves already do. The skip is decided at publish (review Q2):
    /// with the root-seed gate held, the publish checks that the shard still has no
    /// root and swaps its own in, in one synchronous segment, so a batch write that
    /// seeds a root afterwards finds the loaded one, and one that seeded a root first
    /// sends the load down the switched path: the switch is opened then, without a
    /// hint, before the root is published, and the seeded root's rows are relocated
    /// and fenced as they always are. A failed publish persist keeps the published root
    /// in memory rather than reverting to none.
    /// </para>
    /// </remarks>
    private async Task RunBulkLoadAsync(
        string operationId,
        GrainId? seededRoot,
        string planPrefix,
        int entryCount,
        Func<int, string> keyAt,
        Func<int, int, Dictionary<string, LwwValue<byte[]>>> buildBatch)
    {
        var switched = seededRoot is not null || state.State.RootNodeId is not null;
        var seedRouting = WalRoutingStamp.Unstamped;
        if (switched)
        {
            // Epic #5605, member 3: the switch's birth hint and its marker, stored before
            // the first leaf is created; the seed is stamped only after the marker is
            // durably open (review C1).
            await BeginBulkSwitchAsync(operationId, null);
            seedRouting = await BulkSeedRoutingAsync(operationId);
        }

        RecordWrite(entryCount);

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        var (leafIds, separators) = PlanBulkLoadLeaves(shardKey, operationId, planPrefix, maxLeafKeys, entryCount, keyAt);
        await SeedBulkLoadLeavesAsync(leafIds, separators, maxLeafKeys, entryCount, seedRouting, buildBatch);

        var (root, rootIsLeaf) = await BuildBulkLoadRootAsync(operationId, leafIds, separators, maxChildren);
        if (!switched)
        {
            if (await TryPublishNeverWrittenAsync(operationId, root, rootIsLeaf))
            {
                return;
            }

            // A batch write seeded a root while the load ran: fall back to the switched
            // path, opening the marker before the root is published. The leaves were
            // born before this open, so no hint is stored for them.
            await BeginBulkSwitchAsync(operationId, null, withHint: false);
        }

        var replaced = PublishBulkLoadRoot(operationId, root, rootIsLeaf);
        await WriteShardStateAsync();
        var readFrontier = await RetireReplacedRootAsync(seededRoot, replaced);

        // Every row the shard held before the switch is now in the loaded tree, so
        // the leaves may read from the birth hint, capped at the replaced root's
        // frontier (issue #5766); with no frontier, or no hint, they read from the
        // log's start. The switch closes by fence.
        await FinishBulkSwitchAsync(operationId, leafIds, relocated: readFrontier is not null, closeNow: true, readFrontier);
    }

    /// <summary>
    /// The never-written publish (#5812, B2; review Q2): with the root-seed gate held,
    /// checks that the shard still has no root and publishes <paramref name="root"/> in
    /// the same synchronous segment. Returns <see langword="false"/>, publishing
    /// nothing, when a batch write seeded a root first. Holding the gate means a root
    /// seed in flight finishes first, and one that starts afterwards finds this root.
    /// The persist follows outside the gate; if it fails, the published root is kept in
    /// memory, so a batch write admitted into it is not orphaned, and the same
    /// operation's retry finds it complete.
    /// </summary>
    private async Task<bool> TryPublishNeverWrittenAsync(string operationId, GrainId root, bool rootIsLeaf)
    {
        await _ensureRootGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            if (state.State.RootNodeId is not null)
            {
                return false;
            }

            PublishBulkLoadRoot(operationId, root, rootIsLeaf);
        }
        finally
        {
            _ensureRootGate.Release();
        }

        await WriteShardStateAsync();
        return true;
    }

    /// <summary>
    /// The routing a seed under <paramref name="operationId"/>'s switch carries: the
    /// shard root's routing generation, read after the switch's marker opened, and only
    /// while that marker is open (review C1). Otherwise - the gate closed, the marker not
    /// open (a gate that opened after the switch began with it closed), or a generation
    /// that cannot be raised - <see cref="WalRoutingStamp.Unstamped"/>, which seeds
    /// unstamped and reported.
    /// </summary>
    private async Task<WalRoutingStamp> BulkSeedRoutingAsync(string operationId)
    {
        await EnsureRoutingGenerationAsync();
        var routing = AdmissionRoutingStamp();
        return routing.IsSet
            && HasHandOffStorage()
            && (await LoadHandOffMarkerAsync()).Open.ContainsKey(BulkHandOffToken(operationId))
            ? routing
            : WalRoutingStamp.Unstamped;
    }

    /// <summary>
    /// The purge tombstone's gate for the bulk entry points, which seed their own
    /// root rather than going through <see cref="PrepareForWriteAsync"/>: a bulk
    /// write to a purged copy proceeds only when the registry names the copy live
    /// again, seeding (and lifting the tombstone) as any write does
    /// (issue #4503). A no-op on a shard that was never purged. A write stamped
    /// with the registration it was issued under (<see cref="BulkWriteRegistrationLineage"/>)
    /// is admitted only while the registry still names the copy under that
    /// registration, so a load whose caller gave up cannot land in a later
    /// registration of the same id (issue #5813).
    /// </summary>
    private async Task AdmitPurgedCopyForBulkWriteAsync()
    {
        if (!state.State.IsPurged) return;
        await PrepareForPurgedCopyAsync(
            forWrite: true,
            purgedAnswersEmpty: false,
            BulkWriteRegistrationLineage.For(TreeId));
    }

    /// <summary>
    /// Refuses a bulk load unless the shard holds no data, returning the empty
    /// root leaf to retire once the load has published its own root.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A shard with no root is empty. So is one whose root is the single leaf
    /// <see cref="EnsureRootAsync"/> seeds, as long as that leaf holds no entry at
    /// all (issue #4251). Any read seeds that leaf - the emptiness probe a reshard
    /// or resize runs first among them - so testing for "no root" alone refused a
    /// tree that had never held a key. The test is made here, against the shard's
    /// contents, rather than by keeping the probe from seeding, because every read
    /// seeds and the probe is only one of them.
    /// </para>
    /// <para>
    /// The leaf must hold no tombstone either, live or pending. A tombstone means
    /// the shard held data that was deleted, and a bulk load stamps its entries
    /// from a zero clock, so such a shard keeps being refused exactly as before.
    /// An internal root, or a promotion or graft in flight, is data by
    /// construction and is refused without asking a leaf.
    /// </para>
    /// </remarks>
    /// <param name="operation">The bulk-load operation name for the refusal message.</param>
    /// <param name="operationId">The load's operation id.</param>
    /// <returns>
    /// The empty seeded root leaf the load replaces, or <see langword="null"/>
    /// when the shard has no root.
    /// </returns>
    /// <remarks>
    /// One root leaf that holds rows is accepted (#5812, review of the member B design):
    /// one under this same operation's bulk switch, still open. An earlier attempt of
    /// the operation seeded its leaves under that switch and stopped before it
    /// published. The seeded root's range meets the marker, so it is held on the
    /// switch's close heads and replays the aborted seeds by key range, as a 10.2.0
    /// reader does. The retry then replaces that root and relocates its rows into the
    /// loaded tree last-writer-wins, as it relocates a batch write's, and the
    /// operation's own rows carry the same stamps on both sides. The marker is read on
    /// this path only.
    /// </remarks>
    private async Task<GrainId?> EnsureEmptyForBulkLoadAsync(string operation, string operationId)
    {
        if (state.State.RootNodeId is not { } rootId) return null;

        if (RootIsLeafTyped
            && state.State.PendingPromotion is null
            && state.State.PendingBulkGraft is null)
        {
            var stats = await grainFactory.GetGrain<IBPlusLeafGrain>(rootId).GetStatsAsync();
            if (stats.LiveKeys == 0 && stats.Tombstones == 0) return rootId;
            if (HasHandOffStorage()
                && (await LoadHandOffMarkerAsync()).Open.ContainsKey(BulkHandOffToken(operationId)))
            {
                return rootId;
            }
        }

        throw new InvalidOperationException($"{operation} requires an empty shard. This shard already has data.");
    }

    /// <summary>
    /// Retires the root a bulk load replaced, so it does not linger unreachable
    /// with its materialiser pins holding the tree's WAL, after relocating any row
    /// it holds into the loaded tree (issue #5642).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The load checks that the shard is empty once, before its first await, but
    /// a batch write is <c>[AlwaysInterleave]</c> and runs inside the load's
    /// awaits. It routes to the root still published, the empty seeded leaf, or
    /// seeds one itself on a shard that had no root, and may divide it. Point
    /// writes cannot interleave with the load's serial turn. Once the load has
    /// published its root, every batch write admitted before that is waited out
    /// (<see cref="DrainBatchWritesAdmittedBeforeAsync"/>), so no write can reach
    /// the replaced tree any more. Every
    /// leaf of that replaced tree is then read, and any row it holds,
    /// tombstones included, is merged last-writer-wins into the loaded tree by
    /// routing, exactly as a merge of the same rows would be, before the leaf is
    /// cleared. The batch write's stamp then resolves against the bulk entry for
    /// the same key as it does on every replica. Without it the acknowledged row
    /// was unreadable until a covering bulk leaf replayed it from the WAL.
    /// </para>
    /// <para>
    /// Runs only after the new root is persisted, so a failure before it leaves
    /// the shard on its seeded root. A crash in between leaves an unreachable
    /// leaf whose rows its covering bulk leaf replays and whose pins the WAL GC's
    /// orphan sweep retires.
    /// </para>
    /// <para>
    /// The clear goes through the shard root's owed-clear record rather than
    /// being awaited directly (issue #4383). By now the bulk operation is
    /// recorded as complete, so a retry of the same operation returns early and
    /// would never come back here; a clear that failed - its snapshot rows being
    /// the part most able to - would otherwise be stranded with nothing left
    /// that names the leaf. Recorded as owed, it is retried by the next reclaim
    /// or orphan-repair pass and swept by a purge.
    /// </para>
    /// </remarks>
    private async Task<long[]?> RetireReplacedRootAsync(GrainId? seededRoot, ReplacedBulkRoot replaced)
    {
        var rootId = replaced.RootId ?? seededRoot;
        if (rootId is not { } root || state.State.RootNodeId == root) return null;

        // The loaded root is published, so a batch write admitted from here on
        // routes into the loaded tree. Wait out the ones admitted before, which may
        // still reach the replaced root, so the read below sees every row any of
        // them stores there and nothing lands between that read and the clear.
        await DrainBatchWritesAdmittedBeforeAsync();

        var leafIds = await CollectReplacedLeavesAsync(root, replaced.RootId is null || replaced.WasLeaf);
        long[]? frontier = null;
        var frontierKnown = true;
        foreach (var leafId in leafIds)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var (rows, leafFrontier) = await ReadReplacedLeafAsync(leaf);
            if (leafFrontier is null)
                frontierKnown = false;
            else if (frontier is null)
                frontier = leafFrontier;
            else
                MinPerPartitionInPlace(frontier, leafFrontier);

            if (rows is { Count: > 0 })
            {
                // Epic #5605: no phase check admitted these rows, so they carry no
                // routing generation and are appended unstamped (S3).
                await MergeRoutedAsync(rows, isCrossShardMigration: false, WalRoutingStamp.Unstamped);
                RecordBulkRelocation(rows.Count, LatticeMetrics.BulkRelocationReplacedRoot);
            }

            await ClearRemovedLeafAsync(leafId, "replaced bulk-load root");
        }

        return frontierKnown ? frontier : null;
    }

    /// <summary>
    /// Reads a leaf of the root a bulk load replaced: its rows, and, while every silo
    /// serves <see cref="IBPlusLeafGrain.CopyRowsForBulkLoadAsync"/>, its replay
    /// frontier read before them, in one call (issue #5766). An older silo answers that
    /// method with a definite unknown-method fault, or the gate is closed, and the rows
    /// are read as 10.2.0 reads them, with no frontier, so the load applies no hint.
    /// </summary>
    private async Task<(Dictionary<string, LwwValue<byte[]>>? Rows, long[]? Frontier)> ReadReplacedLeafAsync(IBPlusLeafGrain leaf)
    {
        if (BulkGraftSeamsAvailable)
        {
            try
            {
                var copied = await leaf.CopyRowsForBulkLoadAsync();
                Dictionary<string, LwwValue<byte[]>>? copiedRows = null;
                if (copied.Rows.Count > 0)
                {
                    copiedRows = new Dictionary<string, LwwValue<byte[]>>(copied.Rows.Count, StringComparer.Ordinal);
                    foreach (var row in copied.Rows)
                        copiedRows[row.Key] = row.ToLwwValue();
                }

                return (copiedRows, copied.ReadFrontier);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
                // An older silo joined and hosts the leaf before the gate saw it.
            }
        }

        var stats = await leaf.GetStatsAsync();
        if (stats.LiveKeys == 0 && stats.Tombstones == 0)
            return (null, null);

        var delta = await leaf.GetDeltaSinceAsync(new VersionVector());
        return delta.Entries.Count > 0
            ? (new Dictionary<string, LwwValue<byte[]>>(delta.Entries, StringComparer.Ordinal), null)
            : (null, null);
    }

    /// <summary>
    /// Lowers each partition of <paramref name="minimum"/> to <paramref name="frontier"/>'s,
    /// in place, so the minimum over a replaced root's leaves allocates nothing beyond the
    /// first leaf's own array. A partition <paramref name="frontier"/> lacks reads as -1,
    /// no frontier; one <paramref name="minimum"/> lacks already reads as -1 where the hint
    /// is capped.
    /// </summary>
    internal static void MinPerPartitionInPlace(long[] minimum, long[] frontier)
    {
        for (var p = 0; p < minimum.Length; p++)
        {
            minimum[p] = Math.Min(minimum[p], p < frontier.Length ? frontier[p] : -1);
        }
    }

    /// <summary>
    /// The leaves of the tree rooted at <paramref name="root"/>, the root a bulk
    /// load replaced: the root itself when it is a leaf, otherwise its leftmost
    /// leaf and every leaf on the chain to its right. That chain is not linked to
    /// the loaded leaves, which the load chains among themselves.
    /// </summary>
    private async Task<List<GrainId>> CollectReplacedLeavesAsync(GrainId root, bool rootIsLeaf)
    {
        if (rootIsLeaf)
            return [root];

        var nodeId = root;
        GrainId leftmost;
        while (true)
        {
            var routing = await grainFactory.GetGrain<IBPlusInternalGrain>(nodeId).GetRoutingTableAsync();
            if (routing.ChildIds is not { Length: > 0 })
                return [];
            if (routing.ChildrenAreLeaves)
            {
                leftmost = routing.ChildIds[0];
                break;
            }

            nodeId = routing.ChildIds[0];
        }

        var leaves = new List<GrainId>();
        GrainId? next = leftmost;
        while (next is { } id && leaves.Count < MaxReplacedRootLeaves)
        {
            leaves.Add(id);
            next = await grainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return leaves;
    }

    /// <summary>
    /// Bound on the leaves <see cref="CollectReplacedLeavesAsync"/> walks. The tree
    /// a bulk load replaces holds only what batch writes added during the load,
    /// so it is small; the bound keeps a corrupt chain from looping.
    /// </summary>
    private const int MaxReplacedRootLeaves = 10_000;

    /// <summary>
    /// The root a bulk load replaced when it published its own (issue #5642):
    /// its id, or <see langword="null"/> when the shard had none, and whether it
    /// was a leaf.
    /// </summary>
    private readonly record struct ReplacedBulkRoot(GrainId? RootId, bool WasLeaf);
    /// <summary>
    /// Computes every bulk-load leaf's deterministic identity and promoted
    /// separator up front, without issuing a single grain call.
    /// </summary>
    /// <remarks>
    /// Materialising the whole chain first is what makes
    /// <see cref="SeedBulkLoadLeavesAsync"/> safe to overlap: a leaf's sibling
    /// pointers are then known before any leaf is touched, so each leaf can
    /// stamp both of its own links at birth instead of the chain being wired up
    /// one leaf behind the loop. The ids are a pure function of the shard key,
    /// the operation id and the leaf ordinal, exactly as before, so a resumed
    /// bulk load addresses the same grains.
    /// </remarks>
    private (List<GrainId> LeafIds, List<string?> Separators) PlanBulkLoadLeaves(
        string shardKey,
        string operationId,
        string segment,
        int maxLeafKeys,
        int entryCount,
        Func<int, string> keyAt)
    {
        var leafCount = (entryCount + maxLeafKeys - 1) / maxLeafKeys;
        var leafIds = new List<GrainId>(leafCount);
        var separators = new List<string?>(leafCount);

        for (int i = 0, leafIndex = 0; i < entryCount; i += maxLeafKeys, leafIndex++)
        {
            var deterministicId = DeterministicGuid($"{shardKey}/{segment}/{operationId}/leaf/{leafIndex}");
            leafIds.Add(grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId).GetGrainId());
            separators.Add(leafIndex == 0 ? null : keyAt(i));
        }

        return (leafIds, separators);
    }

    /// <summary>
    /// Seeds and fills every planned bulk-load leaf in bounded overlapped waves.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Two calls per leaf, not five.</b> The serial version issued
    /// <c>SetTreeIdAsync</c>, <c>SetShardIndexAsync</c>, <c>MergeEntriesAsync</c>
    /// and then wired the sibling chain with a <c>SetNextSiblingAsync</c> on the
    /// previous leaf and a <c>SetPrevSiblingAsync</c> on this one - five gated,
    /// separately-persisted round trips per leaf, awaited one at a time. The
    /// first four collapse into the single <see cref="SiblingInitialization"/>
    /// batch that the leaf-split donor already uses, which acquires the split
    /// gate once and persists once for the whole batch. Because
    /// <see cref="PlanBulkLoadLeaves"/> has already computed the chain, each leaf
    /// stamps <i>both</i> of its links itself, so no leaf is touched by another
    /// leaf's unit of work.
    /// </para>
    /// <para>
    /// <b>Why the waves are safe.</b> Every leaf is a distinct, freshly created
    /// grain, and after the collapse no unit writes to a leaf other than its own,
    /// so the units are mutually independent and their completion order is
    /// immaterial. Nothing can observe an intermediate state either: the shard's
    /// root is published only by <see cref="PublishBulkLoadRoot"/>, after
    /// this method has returned, and <c>BulkLoadAsync</c> refuses to run against
    /// a shard that already has a root. Within a unit the two calls stay ordered,
    /// because the birth seam must seed the durable materialiser block pin before
    /// <c>MergeEntriesAsync</c> makes the leaf's data reachable in the WAL - the
    /// same ordering the serial version had.
    /// </para>
    /// <para>
    /// The batch for a leaf is built inside its unit rather than up front, so the
    /// live batch set stays O(<see cref="BoundedFanOut.DefaultWidth"/>) instead of
    /// holding one dictionary per leaf for the whole load.
    /// </para>
    /// <para>
    /// <b>Each leaf is born declaring the range it owns</b>, from its own
    /// separator to the next leaf's (open at the chain's two ends). The WAL
    /// materialiser replays into a leaf every record its declared range covers,
    /// and a leaf declaring no range covers its whole shard: these leaves used to
    /// be born that way, so a leaf that deactivated re-materialised every later key
    /// of its shard when it next activated - duplicate rows that over-counted, and
    /// a leaf chain that regressed, so range scans suppressed whole leaves (issue
    /// #5460).
    /// </para>
    /// </remarks>
    private Task SeedBulkLoadLeavesAsync(
        List<GrainId> leafIds,
        List<string?> separators,
        int maxLeafKeys,
        int entryCount,
        WalRoutingStamp seedRouting,
        Func<int, int, Dictionary<string, LwwValue<byte[]>>> buildBatch)
    {
        var slots = new int[leafIds.Count];
        for (int i = 0; i < slots.Length; i++)
        {
            slots[i] = i;
        }

        var treeId = TreeId;
        var shardIndex = MyShardIndex;

        return BoundedFanOut.ForEachAsync(
            slots,
            BoundedFanOut.DefaultWidth,
            async slot =>
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[slot]);

                // A bulk load creates every planned leaf, so each first row write
                // carries a create intent naming its leaf (issue #4654).
                using var createIntent = LatticeNewLeafIntentContext.BeginScope(leafIds[slot]);
                await leaf.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = treeId,
                    ShardIndex = shardIndex,
                    LowKeyInclusive = separators[slot],
                    HighKeyExclusive = slot + 1 < leafIds.Count ? separators[slot + 1] : null,
                    PrevSibling = slot > 0 ? leafIds[slot - 1] : null,
                    NextSibling = slot + 1 < leafIds.Count ? leafIds[slot + 1] : null,
                });

                var start = slot * maxLeafKeys;
                var count = Math.Min(maxLeafKeys, entryCount - start);
                var leafBatch = buildBatch(start, count);
                await WalFenceSeamRetry.RunAsync(
                    () => MergeBulkSeedAsync(leaf, leafBatch, seedRouting), WalFenceSeamRetry.CallerShardCoordinator, TreeId);
            });
    }

    /// <summary>
    /// Seeds <paramref name="batch"/> into a leaf the bulk operation has not yet
    /// published (#5812), so the seed claims no append epoch for it. A leaf hosted by
    /// an older build answers the seed method as unknown, and only then is the batch
    /// merged as before, which claims as it always has.
    /// </summary>
    /// <remarks>
    /// A partition that had not loaded the shard's generation loads it when the seed
    /// re-presents it. Concurrent seeds re-presenting the same generation can race, and
    /// the loser is refused at a threshold equal to the generation itself, which is still
    /// current: the seed is re-sent, as a stamped write is re-admitted after a stale
    /// generation (member 2). A refusal above the seed's generation means the switch
    /// closed under it, which fails the seed and the operation. A re-sent seed re-merges
    /// the same rows, which is idempotent.
    /// </remarks>
    private async Task MergeBulkSeedAsync(IBPlusLeafGrain leaf, Dictionary<string, LwwValue<byte[]>> batch, WalRoutingStamp seedRouting)
    {
        WalFenceSeamRetry? retry = null;
        while (true)
        {
            try
            {
                await leaf.MergeBulkSeedAsync(batch, seedRouting);
                return;
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
                await leaf.MergeEntriesAsync(batch);
                return;
            }
            catch (WalRoutingGenerationStaleException stale) when (
                stale.Generation == seedRouting.Generation && stale.Refusal <= seedRouting.Generation)
            {
                retry = await WalFenceSeamRetry.NextAsync(retry, WalFenceSeamRetry.CallerShardCoordinator, TreeId);
            }
        }
    }

    /// <summary>
    /// Shared bottom-up internal-node assembly used by both
    /// <see cref="BulkLoadAsync"/> and <see cref="BulkLoadRawAsync"/>. Builds
    /// internal nodes from <paramref name="leafIds"/> / <paramref name="separators"/>
    /// and returns the root, unpublished: nothing reaches it until
    /// <see cref="PublishBulkLoadRoot"/> swaps it in.
    /// </summary>
    private async Task<(GrainId Root, bool IsLeaf)> BuildBulkLoadRootAsync(
        string operationId,
        List<GrainId> leafIds,
        List<string?> separators,
        int maxChildren)
    {
        if (leafIds.Count == 1)
        {
            return (leafIds[0], true);
        }

        var shardKey = context.GrainId.Key.ToString()!;
        var currentLevel = new List<(string? separator, GrainId id)>(leafIds.Count);
        for (int i = 0; i < leafIds.Count; i++)
            currentLevel.Add((separators[i], leafIds[i]));

        bool childrenAreLeaves = true;
        int level = 0;

        while (currentLevel.Count > 1)
        {
            var nodeCount = (currentLevel.Count + maxChildren - 1) / maxChildren;
            var nextLevel = new List<(string? separator, GrainId id)>(nodeCount);
            var plans = new List<(GrainId Id, List<string?> Seps, List<GrainId> Ids)>(nodeCount);
            int nodeIndex = 0;

            for (int i = 0; i < currentLevel.Count; i += maxChildren)
            {
                int end = Math.Min(i + maxChildren, currentLevel.Count);
                var chunkSize = end - i;

                var promotedSeparator = currentLevel[i].separator;

                var seps = new List<string?>(chunkSize) { null };
                var ids = new List<GrainId>(chunkSize) { currentLevel[i].id };
                for (int j = i + 1; j < end; j++)
                {
                    seps.Add(currentLevel[j].separator);
                    ids.Add(currentLevel[j].id);
                }

                var deterministicId = DeterministicGuid($"{shardKey}/bulk/{operationId}/internal/{level}/{nodeIndex++}");
                var nodeId = grainFactory.GetGrain<IBPlusInternalGrain>(deterministicId).GetGrainId();

                plans.Add((nodeId, seps, ids));
                nextLevel.Add((promotedSeparator, nodeId));
            }

            // One level's nodes are siblings: each is a distinct, freshly created
            // grain initialised from a disjoint slice of the level below, and none
            // reads another, so the call that builds a node is independent of
            // every other node's. Issued one node at a time a level cost N
            // sequential round trips; issued in bounded waves it costs
            // ceil(N / BoundedFanOut.DefaultWidth). The levels themselves stay
            // strictly ordered - a level's ids must exist before the level above
            // can name them as children - and the tree is unreachable until the
            // root is published below, so nothing observes a half-built level.
            var levelChildrenAreLeaves = childrenAreLeaves;
            var levelTreeId = TreeId;
            await BoundedFanOut.ForEachAsync(
                plans,
                BoundedFanOut.DefaultWidth,
                async plan =>
                {
                    var node = grainFactory.GetGrain<IBPlusInternalGrain>(plan.Id);
                    await node.InitializeWithChildrenForTreeAsync(
                        levelTreeId, plan.Seps, plan.Ids, levelChildrenAreLeaves);
                });

            currentLevel = nextLevel;
            childrenAreLeaves = false;
            level++;
        }

        return (currentLevel[0].id, false);
    }

    /// <summary>
    /// Publishes a built bulk-load root in memory and records <paramref name="operationId"/>
    /// as complete, returning the root it replaced, read in the same synchronous step,
    /// so a root a concurrent batch write seeded or divided while the load ran is the one
    /// reported (issue #5642). The caller persists.
    /// </summary>
    private ReplacedBulkRoot PublishBulkLoadRoot(string operationId, GrainId root, bool rootIsLeaf)
    {
        var replaced = new ReplacedBulkRoot(state.State.RootNodeId, state.State.RootIsLeaf);
        state.State.RootNodeId = root;
        state.State.RootIsLeaf = rootIsLeaf;
        state.State.LastCompletedBulkOperationId = operationId;
        return replaced;
    }

    public async Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        ThrowIfRetired();
        await AdmitPurgedCopyForBulkWriteAsync();

        // Issue #4618: an online resize mirrors the rows a bulk append stored
        // here to its destination. A retry of an append that already completed
        // (or whose graft it resumes) mirrors by reading its rows back, so a
        // crash between the apply and its mirror is covered by the caller's
        // same-operation retry.
        if (state.State.LastCompletedBulkOperationId == operationId)
        {
            await MirrorAppliedRowsAsync(MirroredKeys(sortedEntries), joinCrdt: false);
            return;
        }

        RecordWrite(sortedEntries.Count);

        if (state.State.PendingBulkGraft is not null)
        {
            if (state.State.PendingBulkGraft.OperationId == operationId)
            {
                await CompleteBulkGraftAsync();
                await MirrorAppliedRowsAsync(MirroredKeys(sortedEntries), joinCrdt: false);
                return;
            }
            await CompleteBulkGraftAsync();
        }

        if (sortedEntries.Count == 0) return;

        // The stamped rows, kept only while a resize mirror is active.
        var mirrored = TryGetShadowTarget() is null
            ? null
            : new Dictionary<string, LwwValue<byte[]>>(sortedEntries.Count, StringComparer.Ordinal);

        // A bulk load writes data, so its seed may register a tree that has no
        // row (issue #4219); see PrepareForWriteAsync.
        await EnsureRootAsync(forWrite: true);
        await ResumePendingPromotionAsync();

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var clock = HybridLogicalClock.Zero;

        // Decided by node TYPE so a corrupt RootIsLeaf flag over an internal
        // root (issue 899) descends to the rightmost leaf rather than
        // blind-casting the internal root to IBPlusLeafGrain.
        GrainId rightmostLeafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : await TraverseToRightmostLeafAsync();

        var rightmostLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(rightmostLeafId);

        // GetAppendTailAsync and NarrowHighKeyAsync are new leaf methods, so while an
        // older silo may host the leaf the append takes the 10.2.0 path: fill by count,
        // graft without narrowing, relocate nothing. See BulkGraftSeamsAvailable.
        var (graftSeams, existingCount, rightEdge) = await ReadAppendTailAsync(rightmostLeaf);

        // Entries the right edge cannot take: at or below the shard's highest key,
        // or below the rightmost leaf's own low bound. Either they were grafted by
        // an earlier delivery of this batch (issue #5460) or the batch sorts below
        // rows the shard already holds (issue #5610). See ResolveBelowEdgeAsync.
        var lowBound = RootIsLeafTyped || !graftSeams
            ? null
            : (await rightmostLeaf.GetKeyRangeAndNextSiblingAsync()).KeyRange.LowKeyInclusive;
        var belowEdge = CountBelowEdge(sortedEntries, rightEdge, lowBound);
        int idx = 0;
        if (belowEdge > 0)
        {
            bool merged;
            (idx, clock, merged) = await ResolveBelowEdgeAsync(sortedEntries, belowEdge, clock, mirrored);
            if (merged && idx < sortedEntries.Count)
            {
                // The merge may have divided the rightmost leaf, so the right edge
                // is found again before the append fills it.
                rightmostLeafId = RootIsLeafTyped
                    ? state.State.RootNodeId!.Value
                    : await TraverseToRightmostLeafAsync();
                rightmostLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(rightmostLeafId);
                (_, existingCount, _) = await ReadAppendTailAsync(rightmostLeaf);
            }
        }

        int space = maxLeafKeys - existingCount;

        if (space > 0 && idx < sortedEntries.Count)
        {
            int take = Math.Min(space, sortedEntries.Count - idx);
            var batch = new Dictionary<string, LwwValue<byte[]>>(take);
            for (int i = 0; i < take; i++, idx++)
            {
                clock = HybridLogicalClock.Tick(clock);
                batch[sortedEntries[idx].Key] = LwwValue<byte[]>.Create(sortedEntries[idx].Value, clock);
            }
            await WalFenceSeamRetry.RunAsync(
                () => rightmostLeaf.MergeEntriesAsync(batch), WalFenceSeamRetry.CallerShardCoordinator, TreeId);
            CollectMirroredRows(mirrored, batch);
        }

        if (idx >= sortedEntries.Count)
        {
            state.State.LastCompletedBulkOperationId = operationId;
            await WriteShardStateAsync();
            await MirrorBulkAppendAsync(mirrored, sortedEntries);
            return;
        }

        // Epic #5605, member 3: the switch's birth hint and its marker, stored before
        // the first appended leaf is created. The marker covers the appended range
        // only; the old rightmost leaf is narrowed to its low bound.
        await BeginBulkSwitchAsync(operationId, sortedEntries[idx].Key);

        // #5812: the appended leaves are seeded under the open switch with the bulk-seed
        // class, so the seed claims no append epoch for a leaf not yet grafted.
        var seedRouting = await BulkSeedRoutingAsync(operationId);

        var graftEntries = new List<GraftEntry>();
        var newLeafCount = 1 + (sortedEntries.Count - idx - 1) / maxLeafKeys;
        var newLeafIds = new GrainId[newLeafCount];
        for (var i = 0; i < newLeafCount; i++)
        {
            newLeafIds[i] = grainFactory.GetGrain<IBPlusLeafGrain>(
                DeterministicGuid($"{shardKey}/append/{operationId}/leaf/{i}")).GetGrainId();
        }
        int leafIndex = 0;

        while (idx < sortedEntries.Count)
        {
            int take = Math.Min(maxLeafKeys, sortedEntries.Count - idx);
            var separator = sortedEntries[idx].Key;

            var batch = new Dictionary<string, LwwValue<byte[]>>(take);
            for (int i = 0; i < take; i++, idx++)
            {
                clock = HybridLogicalClock.Tick(clock);
                batch[sortedEntries[idx].Key] = LwwValue<byte[]>.Create(sortedEntries[idx].Value, clock);
            }

            var newId = newLeafIds[leafIndex];
            var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(newId);

            // The append creates this leaf, so its first row write carries a
            // create intent naming it (issue #4654). The leaf is born declaring
            // the range it owns, up to the next appended leaf's separator and open
            // at the tail (issue #5460; see SeedBulkLoadLeavesAsync).
            using (LatticeNewLeafIntentContext.BeginScope(newId))
            {
                await newLeaf.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = TreeId,
                    ShardIndex = MyShardIndex,
                    LowKeyInclusive = separator,
                    HighKeyExclusive = idx < sortedEntries.Count ? sortedEntries[idx].Key : null,
                    PrevSibling = leafIndex > 0 ? newLeafIds[leafIndex - 1] : null,
                    NextSibling = leafIndex + 1 < newLeafCount ? newLeafIds[leafIndex + 1] : null,
                });
            }
            await WalFenceSeamRetry.RunAsync(
                () => MergeBulkSeedAsync(newLeaf, batch, seedRouting), WalFenceSeamRetry.CallerShardCoordinator, TreeId);
            CollectMirroredRows(mirrored, batch);

            graftEntries.Add(new GraftEntry { SeparatorKey = separator, LeafId = newId });
            leafIndex++;
        }

        state.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = operationId,
            ExistingRightmostLeafId = rightmostLeafId,
            NewLeaves = graftEntries,
            RootWasLeaf = state.State.RootIsLeaf,
        };
        await WriteShardStateAsync();

        await CompleteBulkGraftAsync();
        await MirrorBulkAppendAsync(mirrored, sortedEntries);
    }

    /// <summary>
    /// Counts the leading entries of <paramref name="sortedEntries"/> the shard's
    /// right edge cannot take: those at or below <paramref name="rightEdge"/>, the
    /// highest live key of the rightmost leaf, or below <paramref name="lowBound"/>,
    /// that leaf's declared low bound.
    /// </summary>
    private static int CountBelowEdge(
        List<KeyValuePair<string, byte[]>> sortedEntries,
        string? rightEdge,
        string? lowBound)
    {
        var count = 0;
        while (count < sortedEntries.Count)
        {
            var key = sortedEntries[count].Key;
            var atOrBelowEdge = rightEdge is not null && string.CompareOrdinal(key, rightEdge) <= 0;
            var belowLow = lowBound is not null && string.CompareOrdinal(key, lowBound) < 0;
            if (!atOrBelowEdge && !belowLow)
                break;
            count++;
        }

        return count;
    }

    /// <summary>
    /// Resolves the first <paramref name="belowEdge"/> entries, which the shard's
    /// right edge cannot take, and returns the index the append continues from,
    /// the clock after stamping, and whether any entry was merged.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A bulk append grafts at the shard's right edge, so it can graft only keys
    /// above every key the shard already holds. Two things put entries below it.
    /// A resumed bulk-load stream re-sends every unacknowledged chunk, including
    /// one a later chunk has already superseded here, and a retry after a fault
    /// part-way through an append finds the rightmost leaf already filled (issue
    /// #5460). Or the batch really does sort below rows the shard holds: a point
    /// write landed above the maximum between two chunks, or an import resumed
    /// into a tree that has since taken writes (issue #5610).
    /// </para>
    /// <para>
    /// <b>A re-delivery appends nothing.</b> When the shard holds every one of
    /// these entries with the identical value (live, no expiry, the same bytes),
    /// the batch is a re-delivery and they are skipped, so nothing is grafted
    /// twice. Key presence alone cannot tell a re-delivery from a refresh that
    /// overwrites the keys with new values, so it is not the test. The rows are
    /// mirrored to an online resize's destination, as a same-operation retry's
    /// rows are, so a crash between an apply and its mirror stays covered.
    /// </para>
    /// <para>
    /// <b>Anything else is merged, never refused.</b> When any of them is missing,
    /// deleted, or held with another value or an expiry, the batch is not a
    /// re-delivery, and every one of them is stamped and merged last-writer-wins
    /// into the leaf the current routing names, as a merge of the same rows would
    /// be, with the split-shadow forward such a merge makes. A
    /// key the shard already holds therefore resolves by its stamp: bulk entries
    /// are stamped at the wall time of the append, so an entry appended after a
    /// point write wins over it, on this shard exactly as on every replica and
    /// view that applies both records, as 10.2.0 resolved it there. The two
    /// treatments are never mixed within a batch. A chunk that succeeded in 10.2.0
    /// still succeeds.
    /// </para>
    /// </remarks>
    private async Task<(int Next, HybridLogicalClock Clock, bool Merged)> ResolveBelowEdgeAsync(
        List<KeyValuePair<string, byte[]>> sortedEntries,
        int belowEdge,
        HybridLogicalClock clock,
        Dictionary<string, LwwValue<byte[]>>? mirrored)
    {
        var keys = new List<string>(belowEdge);
        for (var i = 0; i < belowEdge; i++)
            keys.Add(sortedEntries[i].Key);

        var rows = await AppliedRowsAsync(keys);
        var redelivery = true;
        for (var i = 0; i < belowEdge && redelivery; i++)
        {
            redelivery = rows.TryGetValue(sortedEntries[i].Key, out var held)
                && HoldsIdentically(held, sortedEntries[i].Value);
        }

        if (redelivery)
        {
            await MirrorRowsAsync(rows, joinCrdt: false);
            return (belowEdge, clock, false);
        }

        var batch = new Dictionary<string, LwwValue<byte[]>>(belowEdge, StringComparer.Ordinal);
        for (var i = 0; i < belowEdge; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            batch[sortedEntries[i].Key] = LwwValue<byte[]>.Create(sortedEntries[i].Value, clock);
        }

        // A row stored for a split's moved slot reaches the split destination
        // before the merge returns, as MergeManyAsync forwards it (issue #4522).
        // MergeManyAsync's other check, refusing a slot this shard is handing off,
        // is deliberately not made: BulkAppendChunkAsync does not re-route a
        // refused shard, so the caller would see a refusal for a chunk 10.2.0
        // accepted; the above-edge graft checks no slot ownership either.
        // Epic #5605: no phase check, so no routing generation; appended unstamped (S3).
        await MergeRoutedAsync(batch, isCrossShardMigration: false, WalRoutingStamp.Unstamped);
        await ForwardMergedRowsToSplitShadowIfNeededAsync(batch.Keys);
        CollectMirroredRows(mirrored, batch);
        RecordBulkRelocation(belowEdge, LatticeMetrics.BulkRelocationBelowEdge);
        return (belowEdge, clock, true);
    }

    /// <summary>
    /// Whether <paramref name="held"/> is exactly the entry a bulk append of
    /// <paramref name="value"/> would have stored: live, with no expiry, and the
    /// same bytes. Only such a row marks a re-delivery; anything else, a value
    /// overwritten since, a deletion, or a refresh carrying new bytes, is merged
    /// (design #5604 change 4).
    /// </summary>
    private static bool HoldsIdentically(in LwwValue<byte[]> held, byte[] value) =>
        !held.IsTombstone
        && held.ExpiresAtTicks == 0
        && held.Value is { } bytes
        && bytes.AsSpan().SequenceEqual(value);

    /// <summary>The single write seam for <see cref="LatticeMetrics.BulkRelocatedRows"/>.</summary>
    private void RecordBulkRelocation(long rows, KeyValuePair<string, object?> source) =>
        LatticeMetrics.BulkRelocatedRows.Add(
            rows,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
            source,
            LatticeTenantLabel.ForTree(TreeId));
    private static void CollectMirroredRows(
        Dictionary<string, LwwValue<byte[]>>? mirrored,
        Dictionary<string, LwwValue<byte[]>> batch)
    {
        if (mirrored is null)
            return;

        foreach (var (key, row) in batch)
            mirrored[key] = row;
    }

    /// <summary>
    /// Mirrors a bulk append's stamped rows to an online resize's destination
    /// (issue #4618). Rows are mirrored only once the append has completed here,
    /// so the destination never holds a row this copy has not stored. A mirror
    /// that became active while the append ran reads the rows back.
    /// </summary>
    private Task MirrorBulkAppendAsync(
        Dictionary<string, LwwValue<byte[]>>? mirrored,
        List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        if (mirrored is not null)
            return MirrorRowsAsync(mirrored, joinCrdt: false);

        return TryGetShadowTarget() is null
            ? Task.CompletedTask
            : MirrorAppliedRowsAsync(DistinctKeys(sortedEntries), joinCrdt: false);
    }

    /// <summary>
    /// Completes (or resumes) a bulk-append graft whose intent has been persisted.
    /// </summary>
    private async Task CompleteBulkGraftAsync()
    {
        var graft = state.State.PendingBulkGraft!;

        var existingLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.ExistingRightmostLeafId);
        var firstNewLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.NewLeaves[0].LeafId);

        // The rightmost leaf stops declaring the keyspace the appended leaves now
        // own, and links the first of them, in one persist (issues #5460, #5654).
        // Narrowing only, so a resumed graft re-runs it harmlessly. The rows it
        // held in that keyspace come back as a copy, are relocated into the
        // appended leaves, and only then dropped from it (issue #5610), so a
        // relocation that fails part-way is handed the same rows when the graft is
        // resumed. A resume narrows whenever the leaf was already narrowed, even
        // with the gate closed, so it never abandons rows a first attempt stranded.
        var readFrontier = await NarrowAndRelocateAsync(graft, existingLeaf);

        // Re-asserts the link for the 10.2.0 path and for a graft resumed after the
        // narrow; a no-op persist-wise once the narrow linked it.
        await existingLeaf.SetNextSiblingAsync(graft.NewLeaves[0].LeafId);
        await firstNewLeaf.SetPrevSiblingAsync(graft.ExistingRightmostLeafId);

        // Hoisted out of the per-entry loop: a single Stack<GrainId> is reused
        // across every entry in graft.NewLeaves via Clear() (which preserves
        // the backing array). This eliminates ~152 B of per-entry transient
        // allocation (24 B Stack header + ~128 B GrainId[4] backing on first
        // Push) on the steady-state non-RootIsLeaf branch. The Stack is
        // unused on the RootIsLeaf early-continue branch.
        var path = new Stack<GrainId>();

        foreach (var entry in graft.NewLeaves)
        {
            if (state.State.RootIsLeaf)
            {
                // Rare branch: only the very first entry of the very first
                // graft on a flat-tree shard hits this; PromoteRootAsync
                // persists the SplitResult via state.PendingPromotion, so
                // the allocation is required here.
                var splitResult = new SplitResult
                {
                    PromotedKey = entry.SeparatorKey,
                    NewSiblingId = entry.LeafId,
                    ChildIsLeaf = true,
                };
                await PromoteRootAsync(splitResult);
                continue;
            }

            path.Clear();
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var node = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
                path.Push(currentId);

                // One call per level: the rightmost child and the leaf-level flag
                // come back together instead of as two round trips.
                var (rightmostChild, childrenAreLeaves) = await node.GetRightmostChildWithMetadataAsync();
                if (childrenAreLeaves)
                    break;

                currentId = rightmostChild;
            }

            // Track the pending promoted (key, child) as locals instead of
            // boxing them in a SplitResult instance. The vast majority of
            // entries trigger a leaf split that the parent internal absorbs
            // without re-splitting, so pendingHasValue flips false on the
            // first null return from AcceptSplitAsync. The class allocation
            // is only paid in the very rare case where the root itself
            // needs to split (caught by the `if (pendingHasValue)` tail).
            //
            // pendingChildIsLeaf tracks the node-type of pendingChild as it
            // bubbles up: it starts as `true` (the graft-supplied leaf id)
            // and flips to `false` as soon as an internal-parent's
            // AcceptSplitAsync returns a non-null bubble (whose NewSiblingId
            // is the freshly-split internal sibling). This value is stamped
            // into the final PromoteRootAsync's SplitResult so the
            // CompletePromotionAsync seam can dispatch SeedChildParentAsync
            // through IBPlusLeafGrain or IBPlusInternalGrain without
            // re-reading the (potentially raced) RootIsLeaf flag.
            var pendingKey = entry.SeparatorKey;
            var pendingChild = entry.LeafId;
            var pendingChildIsLeaf = true;
            var pendingHasValue = true;
            var levelsAccepted = 0;
            while (pendingHasValue && path.Count > 0)
            {
                var parentId = path.Pop();
                var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
                var bubble = await parent.AcceptSplitAsync(pendingKey, pendingChild);
                InvalidateRoutingTable(parentId);
                levelsAccepted++;
                if (bubble is null)
                {
                    pendingHasValue = false;
                }
                else if (bubble.Additional is not null || bubble.Forwarded)
                {
                    // More than one division at this level (a parent that
                    // completed an interrupted split on accepting): link every
                    // one by descent rather than carrying only the first up
                    // this path and dropping the rest (issue #3523).
                    await LinkSplitAsync(bubble, levelsAccepted);
                    pendingHasValue = false;
                }
                else
                {
                    pendingKey = bubble.PromotedKey;
                    pendingChild = bubble.NewSiblingId;
                    pendingChildIsLeaf = bubble.ChildIsLeaf;
                }
            }

            if (pendingHasValue)
            {
                // Every level on the path divided, so the division is of the
                // root: a sibling levelsAccepted internal levels tall, which the
                // link wraps under a new root (or, were the root deeper by now,
                // links at the parent a descent finds).
                await LinkSplitAsync(
                    new SplitResult
                    {
                        PromotedKey = pendingKey,
                        NewSiblingId = pendingChild,
                        ChildIsLeaf = pendingChildIsLeaf,
                    },
                    levelsAccepted);
            }
        }

        // The appended leaves are routable. Their read position becomes the birth hint
        // only when the relocation above moved every row they own whose records predate
        // it; the switch is left for the run's fence (epic #5605, member 3).
        var newLeafIds = new GrainId[graft.NewLeaves.Count];
        for (var i = 0; i < newLeafIds.Length; i++)
            newLeafIds[i] = graft.NewLeaves[i].LeafId;
        await FinishBulkSwitchAsync(graft.OperationId, newLeafIds, relocated: readFrontier is not null, closeNow: false, readFrontier);

        state.State.PendingBulkGraft = null;
        state.State.LastCompletedBulkOperationId = graft.OperationId;
        await WriteShardStateAsync();
    }

    /// <summary>
    /// Whether every silo that may host a leaf serves the bulk-graft leaf methods
    /// <c>GetAppendTailAsync</c> and <c>NarrowHighKeyAsync</c>. A 10.2.0 silo has
    /// neither, so a call to a leaf it hosts fails with an unknown-method fault and
    /// the append, which succeeded in 10.2.0, would fail. The append-fence gate
    /// (epic #5605) is open only while every active silo runs a build that has them,
    /// and closes by itself when an older silo joins, so while it is closed the
    /// append takes the 10.2.0 path. A host with no gate registered runs a single
    /// build.
    /// </summary>
    private bool BulkGraftSeamsAvailable =>
        context.ActivationServices?.GetService(typeof(IWalAppendFenceGate)) is not IWalAppendFenceGate gate
        || gate.IsOpen;

    /// <summary>
    /// Narrows the old rightmost leaf to the first appended separator and links
    /// the first appended leaf in one persist, relocates the rows it held at or
    /// above that separator into the appended leaves, and only then drops exactly
    /// those rows from it (issues #5610, #5654). Copy, deliver, drop: a relocation
    /// that fails, or a shard root that dies before it, leaves the rows on the old
    /// leaf for the resumed graft to be handed again.
    /// <para>
    /// Returns the read position the appended leaves may start from no higher than: the
    /// old leaf's replay frontier when this call narrowed it and every row it held at
    /// or above the separator was relocated into the appended leaves. Otherwise
    /// <see langword="null"/>, and the birth hint is not applied:
    /// <list type="bullet">
    /// <item>a row left on the old leaf has its records below the hint, and the appended
    /// leaves reach it only by replaying from the log's start (review N2 of #5646);</item>
    /// <item>a record of the range that another author appended below the hint and the
    /// old leaf never applied - another shard's range-delete walk, or a duplicate
    /// activation's append - is applied by no leaf unless the appended leaves start at
    /// or below the old leaf's frontier (issue #5752), and a leaf narrowed by an earlier
    /// attempt reports none.</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task<long[]?> NarrowAndRelocateAsync(PendingBulkGraft graft, IBPlusLeafGrain existingLeaf)
    {
        var separator = graft.NewLeaves[0].SeparatorKey;
        if (!BulkGraftSeamsAvailable)
        {
            // The gate is closed, but a first attempt with it open may have
            // narrowed the leaf already; its stranded rows must still be moved.
            var high = (await existingLeaf.GetKeyRangeAndNextSiblingAsync()).KeyRange.HighKeyExclusive;
            if (high is null || string.CompareOrdinal(high, separator) > 0)
                return null;
        }

        LeafNarrowResult? narrowed;
        try
        {
            narrowed = await existingLeaf.NarrowHighKeyAsync(separator, graft.NewLeaves[0].LeafId, graft.NewLeaves);
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
            // An older silo joined and hosts the leaf before the gate saw it: the
            // 10.2.0 path, which links without narrowing.
            return null;
        }

        if (narrowed is null)
        {
            // Review B1 of #5702: the leaf could not drain the writes it admitted
            // before the narrow within its deadline, so it narrowed nothing. The
            // 10.2.0 path: link without narrowing, and apply no hint.
            return null;
        }

        var stranded = narrowed.Rows;
        if (stranded.Count == 0)
            return narrowed.ReadFrontier;

        await RelocateIntoGraftedLeavesAsync(graft, stranded);

        var relocated = new Dictionary<string, HybridLogicalClock>(stranded.Count, StringComparer.Ordinal);
        foreach (var row in stranded)
            relocated[row.Key] = row.Timestamp;
        try
        {
            await existingLeaf.DropRelocatedRowsAsync(relocated);
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
            // The rows are delivered and lie outside the leaf's declared span, so
            // no read reaches them there and its replay does not reload them; only
            // the in-memory copy on an older silo's activation is left behind.
        }

        return narrowed.ReadFrontier;
    }

    /// <summary>
    /// Reads the rightmost leaf's live count and highest live key in one call, or,
    /// while an older silo may host the leaf, only its count (the 10.2.0 path). An
    /// older silo that joins before the gate sees it answers the new method with a
    /// definite unknown-method fault, which also takes the 10.2.0 path.
    /// </summary>
    private async Task<(bool Seams, int LiveCount, string? RightEdge)> ReadAppendTailAsync(IBPlusLeafGrain leaf)
    {
        if (BulkGraftSeamsAvailable)
        {
            try
            {
                var (count, edge) = await leaf.GetAppendTailAsync();
                return (true, count, edge);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
            }
        }

        return (false, await leaf.CountAsync(), null);
    }

    /// <summary>
    /// Merges the rows the old rightmost leaf held at or above the first appended
    /// separator into the appended leaf whose range declares each one, preserving
    /// their stamps (issue #5610). A row and a bulk entry for the same key resolve
    /// last-writer-wins on their stamps, as on every replica, so the acknowledged
    /// row is never lost and a key only the old leaf held stays readable. Re-run
    /// by a resumed graft against whatever the narrow hands back, so it is
    /// idempotent.
    /// </summary>
    private async Task RelocateIntoGraftedLeavesAsync(PendingBulkGraft graft, List<LwwEntry> rows)
    {
        var byLeaf = new Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>();
        foreach (var row in rows)
        {
            // The appended leaves are ordered by separator, and every row sorts at
            // or above the first, so the owner is the last leaf whose separator is
            // at or below the row's key.
            var owner = graft.NewLeaves[0].LeafId;
            for (var i = 1; i < graft.NewLeaves.Count; i++)
            {
                if (string.CompareOrdinal(row.Key, graft.NewLeaves[i].SeparatorKey) < 0)
                    break;
                owner = graft.NewLeaves[i].LeafId;
            }

            if (!byLeaf.TryGetValue(owner, out var batch))
                byLeaf[owner] = batch = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
            batch[row.Key] = row.ToLwwValue();
        }

        // #5812: relocated under the graft's still-open switch, as its seed was, so the
        // relocation claims no append epoch for an appended leaf either.
        var seedRouting = await BulkSeedRoutingAsync(graft.OperationId);
        foreach (var (leafId, batch) in byLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            await WalFenceSeamRetry.RunAsync(
                () => MergeBulkSeedAsync(leaf, batch, seedRouting), WalFenceSeamRetry.CallerShardCoordinator, TreeId);
        }

        RecordBulkRelocation(rows.Count, LatticeMetrics.BulkRelocationRightmostLeaf);
    }

    /// <summary>
    /// If a previous bulk-append graft was interrupted, resume it now.
    /// </summary>
    private async Task ResumePendingBulkGraftAsync()
    {
        if (state.State.PendingBulkGraft is null) return;
        await CompleteBulkGraftAsync();
    }
}
