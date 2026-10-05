using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Lifecycle operations: soft-delete, recovery, and purge.
/// </summary>
internal sealed partial class ShardRootGrain
{
    public async Task MarkDeletedAsync()
    {
        if (state.State.IsDeleted) return;

        // Snapshot the pre-mutation IsDeleted. Without this revert, the
        // idempotency guard above short-circuits every retry from this
        // activation - turning a transient storage failure into a permanent
        // split-brain (Class B "persisted / in-memory divergence on write
        // failure, idempotency-guarded" anti-pattern).
        var isDeletedSnapshot = state.State.IsDeleted;
        state.State.IsDeleted = true;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            throw;
        }
    }

    public Task<bool> IsDeletedAsync() => Task.FromResult(state.State.IsDeleted);

    /// <inheritdoc />
    public async Task WarmUpAsync()
    {
        // Drive the shard root through PrepareForOperationAsync so the
        // first hot-path write does not pay shard-root activation, state
        // hydration, OR root materialization. PrepareForOperationAsync is
        // idempotent: on a populated shard it sync-completes (the
        // RootNodeId-not-null fast path); on a brand-new empty shard it
        // runs EnsureRootAsync, which is exactly the path the first
        // traffic write would take. The resulting root leaf id is
        // deterministic-from-shard-key, so warm-up creates no extra
        // grains beyond what the first write would have created itself -
        // it just moves that work to startup time.
        await PrepareForOperationAsync();

        // Pre-activate this shard's current root node. For an empty
        // bench tree this is the deterministic root leaf that
        // EnsureRootAsync just produced. A read-only ping on that
        // grain forces its placement-directory entry, grain-storage
        // ReadStateAsync, and OnActivateAsync to run while the silo is
        // idle. For a populated tree with RootIsLeaf=false, we ping the
        // root internal node instead; that absorbs the first internal-
        // node first-touch on the routing path. We intentionally do NOT
        // walk deeper - traversal warmup of the full subtree would be
        // O(nodes) RPCs and is out of scope for the lightweight startup
        // probe.
        var rootId = state.State.RootNodeId!.Value;
        if (RootIsLeafTyped)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootId);
            // CountAsync is a cheap read-only probe on IBPlusLeafGrain
            // (returns the live key count from in-memory state).
            await leaf.CountAsync();
        }
        else
        {
            var internalNode = grainFactory.GetGrain<IBPlusInternalGrain>(rootId);
            // AreChildrenLeavesAsync is read-only and trivial - it
            // returns a single bool from the routing snapshot.
            await internalNode.AreChildrenLeavesAsync();
        }

        // Leaf-cache pre-warm (issue #332). On by default; off when
        // LatticeOptions.LeafCachePreWarmCount is 0. Ranks this shard's
        // persisted leaf-access Markov chain by long-run read probability and
        // primes that many LeafCacheGrain activations on this silo - the same
        // silo that will serve the reads, because this grain is the only caller
        // of the stateless-worker cache. Strictly best-effort: every failure is
        // swallowed inside, so pre-warm can never fail warm-up.
        await PreWarmLeafCachesAsync();
    }

    /// <inheritdoc />
    public Task ForceDeactivateAsync()
    {
        // Test-only deactivation seam, mirroring
        // BPlusLeafGrain.ForceDeactivateAsync. Wraps the protected
        // Grain.DeactivateOnIdle() extension so integration tests can drive a
        // real deactivate/reactivate cycle - and therefore a real
        // OnDeactivateAsync leaf-access-model flush through the real Orleans
        // serializer and storage provider - without waiting on the silo's
        // idle-collection scheduler. The runtime schedules the deactivation
        // after the current grain turn completes, so the caller must poll or
        // briefly wait before observing the fresh activation; blocking here
        // would deadlock, because OnDeactivateAsync can only run once this
        // turn ends. The serial guard drains point writes, but not SetManyAsync:
        // fence both and defer the runtime request until batch writes drain too.
        // With no writes in flight the runtime request is still synchronous.
        RequestDeactivationFencingPointWrites();
        return Task.CompletedTask;
    }

    public async Task UnmarkDeletedAsync()
    {
        if (!state.State.IsDeleted) return;

        // See MarkDeletedAsync for the snapshot/restore rationale.
        var isDeletedSnapshot = state.State.IsDeleted;
        state.State.IsDeleted = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            throw;
        }
    }

    public async Task PurgeAsync()
    {
        // First, because nothing else names these leaves: they were taken out
        // of the tree by a reclaim or an orphan repair whose clear failed, so
        // they are on neither the chain nor any routing table, and clearing the
        // shard row below would drop the only record that they still hold state
        // (issue #2207). A failure propagates with the record intact, so the
        // tree-deletion retry comes back to them.
        await ClearPendingLeavesForPurgeAsync();

        await ClearTopologyAsync();

        // Leave a purge tombstone rather than no row (issue #4503): a router that
        // still caches this physical copy must keep being refused, as it was in
        // the soft-delete window, instead of meeting a fresh empty shard that
        // answers its read as empty and accepts - and loses - its write. A system
        // tree is never routed through an alias, so it keeps the empty row.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            await state.ClearStateAsync();
            return;
        }

        var purged = state.State;
        state.State = new ShardRootState { IsPurged = true };
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State = purged;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<bool> IsRetiredAsync() => Task.FromResult(state.State.IsRetired);

    /// <inheritdoc />
    public async Task RetireAsync()
    {
        EnsureInternalOrigin(LatticeOperation.Admin);

        if (!state.State.IsRetired)
        {
            // Fail closed: each of these means this shard may still be the
            // authoritative owner of live data, so its storage is not ours to
            // release. The consolidation coordinator only calls this after its
            // fold committed, where none of them can hold.
            if (state.State.SplitInProgress is { } sip)
                throw new InvalidOperationException(
                    $"Shard {MyShardIndex} of tree '{TreeId}' cannot be retired while a migration to shard {sip.ShadowTargetShardIndex} is in progress (phase {sip.Phase}).");
            if (state.State.ShadowForward is not null || state.State.RetainedRedirect is not null)
                throw new InvalidOperationException(
                    $"Shard {MyShardIndex} of tree '{TreeId}' cannot be retired while an online resize is forwarding or redirecting it.");
            if (state.State.MovedAwaySlots.Count == 0 || state.State.MovedAwayVirtualShardCount is null)
                throw new InvalidOperationException(
                    $"Shard {MyShardIndex} of tree '{TreeId}' cannot be retired: it records no moved-away slots, so a caller holding an older shard map would be served from it instead of redirected.");

            // Persisted BEFORE the storage walk. From here the shard refuses
            // routed traffic and returns empty range-read pages, so nothing can
            // observe - or re-grow - the half-cleared topology below, and a crash
            // part way through is finished by re-issuing this call.
            state.State.IsRetired = true;
            try
            {
                await WriteShardStateAsync();
            }
            catch
            {
                state.State.IsRetired = false;
                throw;
            }
        }

        await ReleaseRetiredStorageAsync();

        logger.LogInformation(
            "Retired shard {ShardIndex} of tree {TreeId}: its leaves, internal nodes and WAL materialiser pins were released; its moved-away fence of {SlotCount} slot(s) is kept.",
            MyShardIndex,
            TreeId,
            state.State.MovedAwaySlots.Count);

        // Drop the activation's in-memory routing and leaf caches with it.
        RequestDeactivationFencingPointWrites();
    }

    /// <inheritdoc />
    public async Task ReviveAsync(int[] ownedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(ownedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");
        EnsureInternalOrigin(LatticeOperation.Admin);

        if (!state.State.IsRetired)
        {
            return;
        }

        // Finish an interrupted retirement first, so a revived shard never
        // resumes serving a half-cleared topology.
        await ReleaseRetiredStorageAsync();

        // Lift the fence only where the new map sends traffic here. A slot this
        // shard gave away and does not get back keeps redirecting a caller whose
        // map predates the one being published. A fence recorded under a
        // different slot count cannot be read against this map, so it goes.
        var previousSlots = state.State.MovedAwaySlots;
        var previousVsc = state.State.MovedAwayVirtualShardCount;
        var kept = new Dictionary<int, int>(previousSlots);
        if (previousVsc == virtualShardCount)
        {
            foreach (var slot in ownedSlots) kept.Remove(slot);
        }
        else
        {
            kept.Clear();
        }

        state.State.IsRetired = false;
        state.State.MovedAwaySlots = kept;
        state.State.MovedAwayVirtualShardCount = kept.Count == 0 ? null : previousVsc;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.IsRetired = true;
            state.State.MovedAwaySlots = previousSlots;
            state.State.MovedAwayVirtualShardCount = previousVsc;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task FenceMovedSlotsAsync(int[] slots, int[] newOwners, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(slots);
        ArgumentNullException.ThrowIfNull(newOwners);
        if (slots.Length != newOwners.Length)
        {
            throw new ArgumentException(
                $"FenceMovedSlotsAsync requires one new owner per slot; got {slots.Length} slot(s) and {newOwners.Length} owner(s).",
                nameof(newOwners));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualShardCount);
        for (var i = 0; i < slots.Length; i++)
        {
            if ((uint)slots[i] >= (uint)virtualShardCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(slots),
                    slots[i],
                    $"Slot must be in [0, {virtualShardCount}).");
            }

            ArgumentOutOfRangeException.ThrowIfNegative(newOwners[i], nameof(newOwners));
        }

        EnsureInternalOrigin(LatticeOperation.Admin);
        if (slots.Length == 0)
        {
            return;
        }

        var previousSlots = state.State.MovedAwaySlots;
        var previousVsc = state.State.MovedAwayVirtualShardCount;

        // A fence recorded under another virtual slot count cannot be read
        // against this map, so it is replaced rather than merged.
        var sameSpace = previousVsc == virtualShardCount && previousSlots is not null;
        var next = sameSpace
            ? new Dictionary<int, int>(previousSlots!)
            : new Dictionary<int, int>(slots.Length);
        var changed = !sameSpace;
        for (var i = 0; i < slots.Length; i++)
        {
            if (!next.TryGetValue(slots[i], out var owner) || owner != newOwners[i])
            {
                next[slots[i]] = newOwners[i];
                changed = true;
            }
        }

        if (changed)
        {
            state.State.MovedAwaySlots = next;
            state.State.MovedAwayVirtualShardCount = virtualShardCount;
            try
            {
                await WriteShardStateAsync();
            }
            catch
            {
                state.State.MovedAwaySlots = previousSlots;
                state.State.MovedAwayVirtualShardCount = previousVsc;
                throw;
            }
        }

        var sorted = (int[])slots.Clone();
        Array.Sort(sorted);
        await MarkLeavesMovedAwayAsync(sorted, virtualShardCount);
    }

    /// <summary>
    /// Clears every leaf and internal node of a retired shard and rewrites its
    /// record as an empty routing tombstone. Idempotent: a shard already reduced
    /// to its tombstone clears nothing and writes once.
    /// </summary>
    private async Task ReleaseRetiredStorageAsync()
    {
        // A routing mutation for the whole walk: an interleaved optimistic read
        // that overlaps it fails its epoch check and retries serially, queueing
        // behind this call and then meeting the retired gate.
        BeginRoutingMutation();
        try
        {
            await ClearPendingLeavesForPurgeAsync();
            await ClearTopologyAsync();

            // Keep the moved-away fence, the delete flag and the registration;
            // drop everything that described the cleared topology.
            state.State.RootNodeId = null;
            state.State.RootIsLeaf = false;
            state.State.PendingPromotion = null;
            state.State.PendingPromotionRootWasLeaf = false;
            state.State.PendingBulkGraft = null;
            state.State.DirtyLeavesSinceLastCompaction.Clear();
            state.State.LeafAccessModel = null;
            state.State.StrandedScanLeafId = null;
            state.State.StrandedScanRecoveries = 0;
            state.State.PendingChildLinks.Clear();
            state.State.PendingLeafClears.Clear();
            await WriteShardStateAsync();
        }
        finally
        {
            EndRoutingMutation();
        }

        logger.LogInformation(
            "Retired shard {ShardIndex} of tree {TreeId}: its leaves, internal nodes and WAL materialiser pins were released; its moved-away fence of {SlotCount} slot(s) is kept.",
            MyShardIndex,
            TreeId,
            state.State.MovedAwaySlots.Count);

        // Drop the activation's in-memory routing and leaf caches with it.
        RequestDeactivationFencingPointWrites();
    }

    /// <summary>
    /// Clears every leaf and internal node this shard routes to. Shared by
    /// <see cref="PurgeAsync"/> and <see cref="RetireAsync"/>; leaves the shard
    /// root's own record untouched, which each caller then clears or rewrites.
    /// </summary>
    private async Task ClearTopologyAsync()
    {
        if (state.State.RootNodeId is null)
        {
            return;
        }

        // Record, before the first leaf is cleared, that this shard's leaves are
        // being cleared on purpose (issue #4654). A purge that dies part-way leaves
        // routed leaves with no state row, which recovery can then re-create empty
        // without mistaking a leaf whose row was lost for one the purge cleared.
        if (!state.State.LeafClearsBegun)
        {
            state.State.LeafClearsBegun = true;
            try
            {
                await WriteShardStateAsync();
            }
            catch
            {
                state.State.LeafClearsBegun = false;
                throw;
            }
        }

        GrainId? leafId;
        // Decide leaf-vs-internal by node TYPE so a corrupt RootIsLeaf flag
        // over an internal root (issue 899) still purges the internal subtree
        // and the leaf chain rather than treating the internal root as a leaf.
        var rootIsLeafTyped = RootIsLeafTyped;
        if (rootIsLeafTyped)
        {
            leafId = state.State.RootNodeId;
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var internalNodeIds = new List<GrainId>();
        List<GrainId>? routedLeafIds = null;
        if (!rootIsLeafTyped)
        {
            routedLeafIds = new List<GrainId>();
            await CollectInternalNodeIds(state.State.RootNodeId!.Value, internalNodeIds, routedLeafIds);
        }

        // DELIBERATELY NOT WORK-BOUNDED (issue 1956). Do not apply
        // LeafWalkBudget here. Two reasons: the tree is already offline by
        // contract when this runs (DeleteTreeAsync makes every subsequent read
        // and write throw), so no live traffic is waiting on this shard; and
        // the walk is destructive, so it cannot resume by key the way a read
        // walk can - once a leaf's state is cleared its sibling pointer is gone,
        // which is why nextId is read before the clear. Bounded by observability.
        var walk = new AtomicLeafWalk("PurgeShardAsync");
        var clearedLeafIds = routedLeafIds is null ? null : new HashSet<GrainId>();
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var nextId = await leaf.GetNextSiblingAsync();
            await leaf.ClearGrainStateAsync();
            clearedLeafIds?.Add(leafId.Value);
            walk.RecordLeafVisited();
            leafId = nextId;
        }

        walk.ReportIfSlow(logger, context.GrainId);

        // The chain walk alone is not enough on a RETRIED purge (issue #2207).
        // A purge that failed part-way has already cleared the head of the
        // chain, and a cleared leaf has no sibling pointer, so the retry's walk
        // stops at the first leaf and every routed leaf beyond the failure
        // would keep its state after the shard row that routes to it is gone.
        // The internal nodes are cleared only after this, so on the retry they
        // still name every routed leaf; clear the ones the walk did not reach.
        // Re-clearing a leaf is idempotent, so the set only saves grain calls.
        if (routedLeafIds is not null)
        {
            foreach (var routedLeafId in routedLeafIds)
            {
                if (clearedLeafIds!.Add(routedLeafId))
                {
                    await grainFactory.GetGrain<IBPlusLeafGrain>(routedLeafId).ClearGrainStateAsync();
                }
            }
        }

        await ClearInternalNodesAsync(internalNodeIds);
    }

    /// <summary>
    /// Clears every collected internal node, in bounded overlapped waves.
    /// </summary>
    /// <remarks>
    /// Unlike the leaf chain - which must stay serial because a leaf's sibling
    /// pointer has to be read before its state is cleared - the internal-node set
    /// is fully materialised by <see cref="CollectInternalNodeIds"/> before the
    /// first clear is issued, so the clears are independent of one another and of
    /// the traversal that produced them, and their completion order is
    /// immaterial. Issued one at a time they turn a purge of an I-node tree into
    /// I sequential round trips; issued in bounded waves they cost
    /// ceil(I / <see cref="BoundedFanOut.DefaultWidth"/>) instead. The purge runs
    /// against a tree that is already offline by contract, so nothing observes an
    /// intermediate state of this sweep either way.
    /// </remarks>
    private Task ClearInternalNodesAsync(List<GrainId> internalNodeIds) =>
        BoundedFanOut.ForEachAsync(
            internalNodeIds,
            BoundedFanOut.DefaultWidth,
            id => grainFactory.GetGrain<IBPlusInternalGrain>(id).ClearGrainStateAsync());

    /// <summary>
    /// Collects every internal node id beneath <paramref name="rootNodeId"/>
    /// (inclusive) so <see cref="ClearInternalNodesAsync"/> can sweep them, and
    /// every leaf id the bottom internal level routes to into
    /// <paramref name="routedLeafIds"/>, so a purge can clear routed leaves its
    /// chain walk did not reach.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>One call per node, not two.</b> The walk used to ask each node
    /// <c>AreChildrenLeavesAsync</c> and then <c>GetChildIdsAsync</c> - two
    /// round trips to learn two fields of the same state.
    /// <c>GetRoutingTableAsync</c> returns both in one snapshot, so a walk over
    /// I nodes issues I calls rather than 2I.
    /// </para>
    /// <para>
    /// <b>Level-parallel, not serial.</b> The old walk was a depth-first stack
    /// that awaited each node before it knew the next one to visit, so the whole
    /// pre-walk was strictly sequential - and it is what feeds the sweep that
    /// already runs in bounded overlapped waves, so the sweep was fast and the
    /// walk that finds its input was not. A level's nodes are known in full once
    /// the level above has been read, and reading a node is a pure query, so a
    /// level's reads are independent and overlap safely.
    /// <see cref="BoundedFanOut.ReadAheadAsync"/> keeps them in input order and
    /// holds only <see cref="BoundedFanOut.DefaultWidth"/> reads in flight, so a
    /// wide level does not burst one call per node. The levels themselves stay
    /// ordered, since a level's ids are only known from the level above.
    /// </para>
    /// <para>
    /// The collected order changes from depth-first to breadth-first. The set is
    /// identical, and its only consumer clears the nodes in a fan-out whose
    /// completion order is already undefined, so nothing depends on the
    /// traversal order.
    /// </para>
    /// </remarks>
    private async Task CollectInternalNodeIds(
        GrainId rootNodeId,
        List<GrainId> collected,
        List<GrainId> routedLeafIds)
    {
        var level = new List<GrainId> { rootNodeId };

        while (level.Count > 0)
        {
            collected.AddRange(level);

            var next = new List<GrainId>();
            await foreach (var routing in BoundedFanOut.ReadAheadAsync(
                level,
                BoundedFanOut.DefaultWidth,
                id => grainFactory.GetGrain<IBPlusInternalGrain>(id).GetRoutingTableAsync()))
            {
                if (routing.ChildrenAreLeaves)
                {
                    // Already in hand from the same snapshot, so recording the
                    // routed leaves costs no extra call.
                    routedLeafIds.AddRange(routing.ChildIds);
                    continue;
                }

                next.AddRange(routing.ChildIds);
            }

            level = next;
        }
    }

    /// <summary>
    /// Upper bound on the number of nodes a single
    /// <see cref="ReseedNodeBindingsAsync"/> pass will re-assert. The repair
    /// runs inside one grain call, and an unbounded node walk in one grain call
    /// is precisely what stranded the topology in the first place - a
    /// <c>PurgeTreeAsync</c> that blew the grain-call timeout part-way through
    /// its own walk. A recovery that timed out would be no better than the
    /// unbound leaf it is trying to repair, so the walk is capped and the
    /// overrun is reported rather than allowed to run long.
    /// </summary>
    private const int MaxReseedNodes = 4096;

    /// <summary>
    /// Maximum re-assert calls in flight at once inside
    /// <see cref="ReseedNodeBindingsAsync"/>. Bounded for the reason
    /// <see cref="BoundedFanOut"/> documents - a burst of one call per node all
    /// racing a single Orleans response deadline degrades into deadline
    /// failures rather than latency - but wide enough that the repair does not
    /// walk the budget one strictly sequential round trip at a time.
    /// </summary>
    private const int ReseedFanOutWidth = 16;

    /// <inheritdoc />
    public async Task ReseedNodeBindingsAsync()
    {
        // No topology to re-assert: EnsureRootAsync seeds a fresh root leaf,
        // binding included, on the first operation after recovery.
        if (state.State.RootNodeId is null) return;

        // Walk unconditionally rather than probing one node and inferring the
        // rest. An earlier revision of this repair probed the leftmost leaf on
        // the theory that PurgeAsync clears it first, so a bound leftmost leaf
        // proved the whole shard was intact. That inference does not hold: a
        // split inherits the donor's binding verbatim, so an unbound leaf mints
        // an unbound sibling anywhere in the key range while the leftmost leaf
        // stays perfectly bound. Recovery is a rare operator action, so paying
        // the full walk to be correct is the right trade.
        var leafIds = new List<GrainId>();
        var internalIds = new List<GrainId>();
        var recreateClearedLeaves = state.State.LeafClearsBegun;
        bool truncated;
        try
        {
            truncated = await CollectBindingTargetsAsync(RootIsLeafTyped, leafIds, internalIds);
        }
        catch (Exception ex)
        {
            // Best-effort: a topology whose internal root was itself cleared has
            // nothing to descend, and a node's silo may be momentarily
            // unreachable. Recovery succeeds today without this repair and an
            // unbound node still surfaces loudly and typed on the write path
            // (where it is now also self-repairing), so degrading to "no repair"
            // is strictly no worse than the status quo, whereas throwing would
            // make recovery newly fragile.
            logger.LogWarning(
                ex,
                "Could not walk shard {ShardIndex} of tree {TreeId} to re-assert node bindings after recovery; "
                + "a node left unbound by an interrupted purge would keep rejecting typed CRDT writes to its key "
                + "range until the write path re-binds it.",
                MyShardIndex,
                TreeId);
            return;
        }

        // Leaves first: an unbound leaf is what actually fails the write path,
        // whereas an unbound internal node only degrades its per-tree options
        // lookup. Both setters are idempotent and short-circuit inside the
        // callee, so a node that is already bound costs one RPC and no storage
        // write - which is what makes an unconditional walk affordable.
        await BoundedFanOut.RunAsync(leafIds.Count, ReseedFanOutWidth, async slot =>
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[slot]);

            // A routed leaf with no state row is re-created empty only when a purge
            // of this shard began clearing its leaves: their data was discarded on
            // purpose. Otherwise it cannot be told apart from a leaf whose row was
            // lost (issue #4654), so it carries no create intent, refuses the
            // binding, and stays failed closed; recovery of the rest goes on.
            using var createIntent = recreateClearedLeaves ? LatticeNewLeafIntentContext.BeginScope(leafIds[slot]) : null;
            try
            {
                await leaf.SetTreeIdAsync(TreeId);
                await leaf.SetShardIndexAsync(MyShardIndex);
            }
            catch (LeafStateRowLostException ex)
            {
                logger.LogWarning(
                    ex,
                    "Leaf {LeafId} of shard {ShardIndex} of tree {TreeId} has no state row and no purge of the shard "
                    + "began, so recovery did not re-create it: it may be a leaf whose row was lost, and re-creating it "
                    + "empty would report its keys absent. Its key range fails closed until the leaf, or the tree, is "
                    + "restored from a backup.",
                    leafIds[slot],
                    MyShardIndex,
                    TreeId);
            }
        });

        await BoundedFanOut.RunAsync(internalIds.Count, ReseedFanOutWidth, slot =>
            grainFactory.GetGrain<IBPlusInternalGrain>(internalIds[slot]).SetTreeIdAsync(TreeId));

        if (truncated)
        {
            logger.LogWarning(
                "Re-asserted node bindings on the first {NodeCount} nodes of shard {ShardIndex} of tree {TreeId} "
                + "after recovery, but the shard has more than the {MaxReseedNodes}-node repair budget; "
                + "keys routed to the nodes beyond it are re-bound by the write path on their next typed CRDT write; "
                + "a node beyond it with no state row fails closed (issue #4654).",
                leafIds.Count + internalIds.Count,
                MyShardIndex,
                TreeId,
                MaxReseedNodes);
        }

        // The purge's leaves are re-created; the shard is live again, so a leaf
        // that loses its row from here on is a lost leaf, not a purged one.
        if (recreateClearedLeaves)
        {
            state.State.LeafClearsBegun = false;
            try
            {
                await WriteShardStateAsync();
            }
            catch
            {
                state.State.LeafClearsBegun = true;
                throw;
            }
        }
    }

    /// <summary>
    /// Collects every node this shard still routes to, splitting them into
    /// leaves and internal nodes, and returns whether the
    /// <see cref="MaxReseedNodes"/> budget truncated the walk.
    /// <para>
    /// Descends through the internal nodes rather than following the leaf
    /// sibling chain: the chain is exactly what an interrupted purge severs
    /// (clearing a leaf wipes its sibling pointers), whereas an internal node
    /// keeps its child ids until the purge reaches the internal sweep - which
    /// only starts once every leaf has already been cleared. Descending
    /// therefore reaches precisely the leaves routing can still deliver a write
    /// to, which is the set that has to be bound for the tree to be writable.
    /// </para>
    /// </summary>
    private async Task<bool> CollectBindingTargetsAsync(
        bool rootIsLeafTyped,
        List<GrainId> leafIds,
        List<GrainId> internalIds)
    {
        var rootNodeId = state.State.RootNodeId!.Value;
        if (rootIsLeafTyped)
        {
            leafIds.Add(rootNodeId);
            return false;
        }

        var stack = new Stack<GrainId>();
        stack.Push(rootNodeId);

        while (stack.Count > 0)
        {
            if (leafIds.Count + internalIds.Count >= MaxReseedNodes)
                return true;

            var nodeId = stack.Pop();
            internalIds.Add(nodeId);

            var node = grainFactory.GetGrain<IBPlusInternalGrain>(nodeId);
            // A node the purge already cleared reports no children, so the walk
            // simply stops there: everything below it is unreachable by routing
            // too, and re-binding an id nothing can route to buys nothing.
            var childrenAreLeaves = await node.AreChildrenLeavesAsync();
            var children = await node.GetChildIdsAsync();
            if (childrenAreLeaves)
            {
                leafIds.AddRange(children);
                continue;
            }

            for (int i = children.Count - 1; i >= 0; i--)
            {
                stack.Push(children[i]);
            }
        }

        return false;
    }
}
