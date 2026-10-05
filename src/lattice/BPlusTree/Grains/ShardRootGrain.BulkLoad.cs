using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bottom-up bulk-load and streaming bulk-append operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
    public async Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        ThrowIfRetired();
        await AdmitPurgedCopyForBulkWriteAsync();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        var seededRoot = await EnsureEmptyForBulkLoadAsync(nameof(BulkLoadAsync));

        if (sortedEntries.Count == 0) return;

        RecordWrite(sortedEntries.Count);

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        var (leafIds, separators) = PlanBulkLoadLeaves(
            shardKey, operationId, "bulk", maxLeafKeys, sortedEntries.Count, i => sortedEntries[i].Key);

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

        await SeedBulkLoadLeavesAsync(leafIds, maxLeafKeys, sortedEntries.Count, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var kv = sortedEntries[start + j];
                batch[kv.Key] = LwwValue<byte[]>.Create(kv.Value, clocks[start + j]);
            }

            return batch;
        });

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
        await RetireSeededRootLeafAsync(seededRoot);
    }

    /// <inheritdoc />
    public async Task BulkLoadRawAsync(
        string operationId,
        List<LwwEntry> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        ThrowIfRetired();
        await AdmitPurgedCopyForBulkWriteAsync();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        var seededRoot = await EnsureEmptyForBulkLoadAsync(nameof(BulkLoadRawAsync));

        if (sortedEntries.Count == 0) return;

        RecordWrite(sortedEntries.Count);

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        // Identical B+ tree assembly to BulkLoadAsync - the only difference is
        // that every entry's LwwValue (HLC version AND ExpiresAtTicks /
        // TTL) flows through verbatim instead of being re-stamped with a fresh
        // zero-based clock. Used by snapshot / restore (TreeSnapshotGrain) so
        // TTL metadata survives the transfer end-to-end.
        var (leafIds, separators) = PlanBulkLoadLeaves(
            shardKey, operationId, "bulkraw", maxLeafKeys, sortedEntries.Count, i => sortedEntries[i].Key);

        await SeedBulkLoadLeavesAsync(leafIds, maxLeafKeys, sortedEntries.Count, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var e = sortedEntries[start + j];
                batch[e.Key] = e.ToLwwValue();
            }

            return batch;
        });

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
        await RetireSeededRootLeafAsync(seededRoot);
    }

    /// <summary>
    /// The purge tombstone's gate for the bulk entry points, which seed their own
    /// root rather than going through <see cref="PrepareForWriteAsync"/>: a bulk
    /// write to a purged copy proceeds only when the registry names the copy live
    /// again, seeding (and lifting the tombstone) as any write does
    /// (issue #4503). A no-op on a shard that was never purged.
    /// </summary>
    private async Task AdmitPurgedCopyForBulkWriteAsync()
    {
        if (!state.State.IsPurged) return;
        await PrepareForPurgedCopyAsync(forWrite: true, purgedAnswersEmpty: false);
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
    /// <returns>
    /// The empty seeded root leaf the load replaces, or <see langword="null"/>
    /// when the shard has no root.
    /// </returns>
    private async Task<GrainId?> EnsureEmptyForBulkLoadAsync(string operation)
    {
        if (state.State.RootNodeId is not { } rootId) return null;

        if (RootIsLeafTyped
            && state.State.PendingPromotion is null
            && state.State.PendingBulkGraft is null)
        {
            var stats = await grainFactory.GetGrain<IBPlusLeafGrain>(rootId).GetStatsAsync();
            if (stats.LiveKeys == 0 && stats.Tombstones == 0) return rootId;
        }

        throw new InvalidOperationException($"{operation} requires an empty shard. This shard already has data.");
    }

    /// <summary>
    /// Retires the empty seeded root leaf a bulk load replaced, so it does not
    /// linger unreachable with its materialiser pins holding the tree's WAL.
    /// </summary>
    /// <remarks>
    /// Runs only after the new root is persisted, so a failure before it leaves
    /// the shard on its seeded root, and the leaf is cleared only while it still
    /// holds nothing. A crash in between leaves an empty unreachable leaf whose
    /// pins the WAL GC's orphan sweep retires.
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
    private async Task RetireSeededRootLeafAsync(GrainId? seededRoot)
    {
        if (seededRoot is not { } leafId || state.State.RootNodeId == leafId) return;

        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var stats = await leaf.GetStatsAsync();
        if (stats.LiveKeys == 0 && stats.Tombstones == 0)
            await ClearRemovedLeafAsync(leafId, "replaced seeded root");
    }

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
    /// root is published only by <see cref="FinalizeBulkLoadTreeAsync"/>, after
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
    /// </remarks>
    private Task SeedBulkLoadLeavesAsync(
        List<GrainId> leafIds,
        int maxLeafKeys,
        int entryCount,
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
                await leaf.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = treeId,
                    ShardIndex = shardIndex,
                    // Bulk-load leaves carry no ownership range, exactly as the
                    // serial version left them - it never called SetKeyRangeAsync.
                    LowKeyInclusive = null,
                    HighKeyExclusive = null,
                    PrevSibling = slot > 0 ? leafIds[slot - 1] : null,
                    NextSibling = slot + 1 < leafIds.Count ? leafIds[slot + 1] : null,
                });

                var start = slot * maxLeafKeys;
                var count = Math.Min(maxLeafKeys, entryCount - start);
                await leaf.MergeEntriesAsync(buildBatch(start, count));
            });
    }

    /// <summary>
    /// Shared bottom-up internal-node assembly used by both
    /// <see cref="BulkLoadAsync"/> and <see cref="BulkLoadRawAsync"/>. Builds
    /// internal nodes from <paramref name="leafIds"/> / <paramref name="separators"/>,
    /// persists the root, and records <paramref name="operationId"/> as complete.
    /// </summary>
    private async Task FinalizeBulkLoadTreeAsync(
        string operationId,
        List<GrainId> leafIds,
        List<string?> separators,
        int maxChildren)
    {
        if (leafIds.Count == 1)
        {
            state.State.RootNodeId = leafIds[0];
            state.State.RootIsLeaf = true;
            state.State.LastCompletedBulkOperationId = operationId;
            await WriteShardStateAsync();
            return;
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
            // reads another, so the two calls that build a node are independent of
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
                    await node.SetTreeIdAsync(levelTreeId);
                    await node.InitializeWithChildrenAsync(plan.Seps, plan.Ids, levelChildrenAreLeaves);
                });

            currentLevel = nextLevel;
            childrenAreLeaves = false;
            level++;
        }

        state.State.RootNodeId = currentLevel[0].id;
        state.State.RootIsLeaf = false;
        state.State.LastCompletedBulkOperationId = operationId;
        await WriteShardStateAsync();
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
            await MirrorAppliedRowsAsync(DistinctKeys(sortedEntries), joinCrdt: false);
            return;
        }

        RecordWrite(sortedEntries.Count);

        if (state.State.PendingBulkGraft is not null)
        {
            if (state.State.PendingBulkGraft.OperationId == operationId)
            {
                await CompleteBulkGraftAsync();
                await MirrorAppliedRowsAsync(DistinctKeys(sortedEntries), joinCrdt: false);
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
        var existingKeys = await rightmostLeaf.GetKeysAsync();
        int space = maxLeafKeys - existingKeys.Count;

        int idx = 0;
        if (space > 0)
        {
            int take = Math.Min(space, sortedEntries.Count);
            var batch = new Dictionary<string, LwwValue<byte[]>>(take);
            for (int i = 0; i < take; i++, idx++)
            {
                clock = HybridLogicalClock.Tick(clock);
                batch[sortedEntries[idx].Key] = LwwValue<byte[]>.Create(sortedEntries[idx].Value, clock);
            }
            await rightmostLeaf.MergeEntriesAsync(batch);
            CollectMirroredRows(mirrored, batch);
        }

        if (idx >= sortedEntries.Count)
        {
            state.State.LastCompletedBulkOperationId = operationId;
            await WriteShardStateAsync();
            await MirrorBulkAppendAsync(mirrored, sortedEntries);
            return;
        }

        var graftEntries = new List<GraftEntry>();
        GrainId? prevNewLeafId = null;
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

            var deterministicId = DeterministicGuid($"{shardKey}/append/{operationId}/leaf/{leafIndex++}");
            var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
            var newId = newLeaf.GetGrainId();
            await newLeaf.SetTreeIdAsync(TreeId);
            await newLeaf.SetShardIndexAsync(MyShardIndex);
            await newLeaf.MergeEntriesAsync(batch);
            CollectMirroredRows(mirrored, batch);

            if (prevNewLeafId is not null)
            {
                var prevLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(prevNewLeafId.Value);
                await prevLeaf.SetNextSiblingAsync(newId);
                await newLeaf.SetPrevSiblingAsync(prevNewLeafId.Value);
            }

            graftEntries.Add(new GraftEntry { SeparatorKey = separator, LeafId = newId });
            prevNewLeafId = newId;
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

                if (await node.AreChildrenLeavesAsync())
                    break;

                currentId = await node.GetRightmostChildAsync();
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

        state.State.PendingBulkGraft = null;
        state.State.LastCompletedBulkOperationId = graft.OperationId;
        await WriteShardStateAsync();
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
