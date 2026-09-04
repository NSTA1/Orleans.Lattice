using System.Runtime.InteropServices;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-tooling partial for <see cref="Orleans.Lattice.BPlusTree.Grains.ShardRootGrain"/>. Fans the
/// leaf-projection rebuild and materialiser-lag queries across this
/// shard's leaf chain, mirroring the existing chain-walk used by
/// <see cref="ShardRootGrain.GetDiagnosticsAsync"/>.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task RebuildShardProjectionAsync(CancellationToken cancellationToken)
    {
        // Retained for wire compatibility with a caller from an older build
        // that has not adopted the bounded protocol. Drives the bounded walk to
        // completion inside this one call.
        string? cursor = null;
        while (true)
        {
            var page = await RebuildShardProjectionBoundedAsync(cursor, cancellationToken);
            if (page.ResumeFromInclusive is not { } next) return;
            cursor = next;
        }
    }

    /// <inheritdoc />
    // The stall ceiling can abandon this walk mid-chain, which is safe because
    // RebuildProjectionFromWalAsync is idempotent - it deactivates the leaf so
    // the projection is rebuilt lazily on its next activation, and rebuilding an
    // already-rebuilt leaf is a no-op. An abandoned continuation therefore
    // completes harmlessly unobserved, and an operator retries from the cursor
    // of the last page that returned. The pre-rebuild ordering below is what
    // keeps the walk resumable at all (issue 1972) and is unaffected by the
    // ceiling, which never interposes inside the loop.
    public Task<ShardProjectionRebuildPage> RebuildShardProjectionBoundedAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken)
    {
        var scan = BeginScanPage(nameof(RebuildShardProjectionBoundedAsync));
        return GuardScanPageAsync(
            scan,
            RebuildShardProjectionBoundedCoreAsync(resumeFromInclusive, cancellationToken, scan));
    }

    private async Task<ShardProjectionRebuildPage> RebuildShardProjectionBoundedCoreAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken,
        ScanPageWalk scan)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        scan.Phase = ScanPagePhase.Descent;
        var startLeafId = await ResolveWalkStartLeafAsync(resumeFromInclusive);
        if (startLeafId is null)
        {
            // Empty shard - no leaves to rebuild. Returning here matches
            // the diagnostics-walk semantics for a shard whose root has
            // never been assigned.
            return default;
        }

        // Hand-rolled rather than routed through BoundedLeafWalk, which is the
        // shared implementation every other bounded chain walk uses. The reason
        // is ordering: BoundedLeafWalk reads the sibling pointer, and the key
        // range it resumes from, AFTER the caller's per-leaf work. That is
        // right for a read walk and wrong here, because
        // RebuildProjectionFromWalAsync deactivates the leaf - so both reads
        // would land on a deactivated grain and force an immediate WAL replay
        // purely to obtain a cursor, turning the deliberately lazy rebuild into
        // an inline one. Both reads therefore happen BEFORE the rebuild. The
        // rules are otherwise the shared ones: the same LeafWalkBudget, a key
        // cursor rather than a leaf id, and no stopping anywhere the walk
        // cannot name a resume position (issues 1955, 1972, 1973).
        scan.Phase = ScanPagePhase.LeafWalk;
        var leafId = startLeafId.Value;
        var rebuilt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());

            // Read the next-sibling pointer BEFORE driving the rebuild
            // because RebuildProjectionFromWalAsync deactivates the
            // grain; any subsequent call on the same grain handle would
            // simply reactivate it (wasting an activation) and we
            // already have everything we need from the pre-rebuild
            // state to continue the chain walk.
            var next = await leaf.GetNextSiblingAsync();

            scan.Budget.RecordLeafVisited();

            // Decide whether this is a yield point while the leaf is still
            // activated, so the resume key can be read before the rebuild for
            // the same reason the sibling pointer is. resultsCollected is 1
            // because the unit of progress here is a leaf rebuilt: passing an
            // aggregate count would disarm the bound on any run of leaves that
            // reported nothing (issue 1992). The budget's duration component
            // therefore excludes the current leaf's rebuild, which costs at
            // most one extra leaf per batch.
            string? resumeFrom = null;
            if (next is not null && scan.Budget.ShouldYield())
            {
                resumeFrom = await TryResolveResumeKeyAsync(leaf, resumeFromInclusive, null);
            }

            await leaf.RebuildProjectionFromWalAsync();
            rebuilt++;

            if (next is null)
            {
                // Chain complete: no resume position, because there is nothing
                // left to resume.
                return new ShardProjectionRebuildPage { LeavesRebuilt = rebuilt };
            }

            if (resumeFrom is not null)
            {
                return new ShardProjectionRebuildPage
                {
                    LeavesRebuilt = rebuilt,
                    ResumeFromInclusive = resumeFrom,
                };
            }

            // Either the budget is unspent, or it is spent at a leaf that
            // declares no usable high bound - in which case the walk keeps
            // going rather than stop somewhere it cannot resume from, which
            // would silently leave the rest of the shard unrebuilt.
            leafId = next.Value;
        }
    }

    /// <inheritdoc />
    public async Task<long> GetShardMaterialiserLagAsync(CancellationToken cancellationToken)
    {
        // Retained for wire compatibility with a caller from an older build
        // that has not adopted the bounded protocol. Drives the bounded walk to
        // completion inside this one call.
        var page = await GetShardMaterialiserLagBoundedAsync(null, cancellationToken);
        var heads = page.WalHeadOffsets;
        var minCheckpoint = page.MinCheckpointOffset;

        var cursor = page.ResumeFromInclusive;
        while (cursor is not null)
        {
            page = await GetShardMaterialiserLagBoundedAsync(cursor, cancellationToken);
            if (page.MinCheckpointOffset < minCheckpoint)
                minCheckpoint = page.MinCheckpointOffset;
            cursor = page.ResumeFromInclusive;
        }

        return ReduceMaterialiserLag(heads, minCheckpoint);
    }

    /// <summary>
    /// Reduces captured per-partition WAL heads and the chain-wide minimum
    /// projection checkpoint to a single shard lag figure.
    /// <para>
    /// A <paramref name="minCheckpoint"/> still at <see cref="long.MaxValue"/>
    /// means the walk visited no leaf at all, so no projection state exists and
    /// the heads themselves are the lag - the empty-shard answer.
    /// </para>
    /// </summary>
    internal static long ReduceMaterialiserLag(long[] walHeadOffsets, long minCheckpoint)
    {
        if (walHeadOffsets is null || walHeadOffsets.Length == 0) return 0;

        long total = 0;
        if (minCheckpoint == long.MaxValue)
        {
            foreach (var head in walHeadOffsets) total += head;
            return total;
        }

        foreach (var head in walHeadOffsets)
        {
            var lag = head - minCheckpoint;
            if (lag > 0) total += lag;
        }
        return total;
    }

    /// <inheritdoc />
    public Task<ShardMaterialiserLagPage> GetShardMaterialiserLagBoundedAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken)
    {
        var scan = BeginScanPage(nameof(GetShardMaterialiserLagBoundedAsync));
        return GuardScanPageAsync(
            scan,
            GetShardMaterialiserLagBoundedCoreAsync(resumeFromInclusive, cancellationToken, scan));
    }

    private async Task<ShardMaterialiserLagPage> GetShardMaterialiserLagBoundedCoreAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken,
        ScanPageWalk scan)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // WAL head is shared across every leaf in this shard's chain
        // but - under multi-partition WAL - is partitioned across
        // multiple ILeafReplayCoordinatorGrain activations. We capture
        // the head per partition and compute the per-shard lag as the
        // SUM of per-partition lags: each leaf's projection checkpoint
        // is also per-partition, and the operator-facing "how far
        // behind is this shard" number must reflect total replay work
        // outstanding across every partition (not just the deepest
        // one). Under the default single-partition shape the sum
        // collapses to the legacy scalar lag.
        //
        // Captured on the first batch only. Lag is head - checkpoint, so
        // re-reading a fresher head on a later batch would measure it against
        // checkpoints gathered earlier and inflate the figure by whatever the
        // tree committed mid-walk (issue 1972).
        long[] perPartitionHeads = [];
        if (resumeFromInclusive is null)
        {
            var resolved = await optionsResolver.ResolveAsync(TreeId);
            var partitionCount = Math.Max(1, resolved.WalPartitions);
            perPartitionHeads = new long[partitionCount];
            for (var p = 0; p < partitionCount; p++)
            {
                var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{TreeId}/{p}");
                perPartitionHeads[p] = await coordinator.GetHeadOffsetAsync(cancellationToken);
            }
        }

        scan.Phase = ScanPagePhase.Descent;
        var startLeafId = await ResolveWalkStartLeafAsync(resumeFromInclusive);
        if (startLeafId is null)
        {
            // Empty shard - no projection state exists, so the sum of
            // WAL heads IS the lag. Reported by leaving the minimum at
            // long.MaxValue, which the reducer reads as "no leaves".
            return new ShardMaterialiserLagPage
            {
                WalHeadOffsets = perPartitionHeads,
                MinCheckpointOffset = long.MaxValue,
            };
        }

        // Find the chain-walk minimum checkpoint across this batch's leaves;
        // the driver reduces the per-batch minima and subtracts from the heads.
        var minCheckpoint = long.MaxValue;

        scan.Phase = ScanPagePhase.LeafWalk;
        var walk = BoundedLeafWalk.FromResolvedStart(
            grainFactory,
            startLeafId,
            resumeFromInclusive,
            scan.Budget);

        while (walk.HasLeaf)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // GetProjectionCheckpointOffsetAsync returns the legacy
            // scalar (partition 0) projection checkpoint; under multi-
            // partition the per-partition state lives on
            // ProjectionCheckpointOffsetsByPartition. For per-partition
            // accuracy we'd need a per-partition accessor on the leaf
            // grain - which isn't ship today and would be a separate
            // public-API expansion. As a conservative approximation the
            // legacy scalar (a lower bound on partition 0's actual
            // checkpoint) is treated as the floor across every
            // partition's min, yielding a slightly-overcounted lag
            // figure - which is the right direction for an operator
            // alarm signal (better to over-report lag than miss it).
            var legacyCheckpoint = await walk.CurrentLeaf.GetProjectionCheckpointOffsetAsync();
            if (legacyCheckpoint < minCheckpoint)
                minCheckpoint = legacyCheckpoint;

            if (!await walk.MoveNextAsync()) break;
        }

        return new ShardMaterialiserLagPage
        {
            WalHeadOffsets = perPartitionHeads,
            MinCheckpointOffset = minCheckpoint,
            ResumeFromInclusive = walk.ResumeFromInclusive,
        };
    }

    /// <inheritdoc />
    public async Task<long[]> SnapshotWalHeadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Per-partition WAL-head capture: each partition's coordinator
        // returns its next-to-be-assigned offset. The snapshot leaf's
        // ReplayWalAsync drives a per-partition slice walk against the
        // same coordinator set, so the returned array is the canonical
        // "open snapshot at this moment" capture point for the
        // multi-partition zero-observable-writes cursor design. Under
        // the default single-partition shape this collapses to a
        // single-element array matching the pre-multi-partition
        // scalar shape exactly.
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var heads = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{TreeId}/{p}");
            heads[p] = await coordinator.GetHeadOffsetAsync(cancellationToken);
        }
        return heads;
    }

    /// <inheritdoc />
    // Deliberately NOT work-bounded, and a resume budget is not a drop-in fix
    // (issue 1956). Issue 1961 settled on two mitigations that do not require
    // making the walk resumable, and both are applied here: the hard
    // end-to-end stall ceiling armed by the wrapper bounds the *hold*, and the
    // fold pass below is fanned out to cut the dominant cost.
    //
    // Do NOT "fix" this by capturing capturedHead first and freezing each leaf
    // AT it. That inversion is unsafe, not merely insufficient.
    // FreezeProjectionAsync always freezes at the leaf's OWN frontier - there
    // is no seam to freeze at a caller-supplied head - and
    // FoldTailOntoFrozenAsync folds only FORWARD over (frontier, capturedHead],
    // skipping any partition whose frontier already exceeds it. A leaf whose
    // head advanced past capturedHead before it was frozen would therefore have
    // post-capture writes baked into its frozen cache, the forward fold would
    // skip that partition, and those writes would be materialised into the
    // baseline: the exact zero-observable-writes violation this design exists to
    // prevent, failing silently rather than erroring. CRDT and LWW folds are not
    // invertible, so there is no rewind. See the class doc on LeafBaselineFreeze
    // (which names the same "overshooting re-freeze past capturedHead" hazard)
    // and issue 1961.
    public Task<SnapshotBaselineCaptureResult> CaptureSnapshotBaselineAsync(
        Guid token,
        CancellationToken cancellationToken)
    {
        if (token == Guid.Empty)
            throw new ArgumentException("Snapshot baseline token must not be empty.", nameof(token));

        // Armed before the first await, like every other bounded entry point on
        // this grain, so the ceiling covers the whole hold rather than starting
        // after the prologue. Only the HARD ceiling is in force here: the core
        // never samples the cooperative per-page budget, because this walk has
        // nowhere it can stop and resume from. Abandoning it is nonetheless safe
        // - the capture is read-only right up to the closing SeedAsync, so
        // nothing is half-applied, and the failed open is simply retried with a
        // fresh baseline token.
        var scan = BeginScanPage(nameof(CaptureSnapshotBaselineAsync));
        return GuardScanPageAsync(
            scan,
            CaptureSnapshotBaselineCoreAsync(token, cancellationToken, scan));
    }

    private async Task<SnapshotBaselineCaptureResult> CaptureSnapshotBaselineCoreAsync(
        Guid token,
        CancellationToken cancellationToken,
        ScanPageWalk scan)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Resolved synchronously and up front, so the fan-out costs no registry
        // round trip inside the window the stall ceiling bounds.
        var foldConcurrency = optionsResolver.GetSnapshotBaselineFoldConcurrency(TreeId);

        // Pass 1 (freeze): walk this shard's leaf chain and freeze each leaf's
        // committed cache, per-partition projection frontier, and in-flight
        // prepared sagas. Reading the next-sibling pointer before the freeze is
        // unnecessary here (the freeze is read-only and does not deactivate the
        // leaf), but we keep the leaf handle alongside its freeze so the fold
        // pass can target the exact same leaf with the uniform capturedHead.
        scan.Phase = ScanPagePhase.Descent;
        var leftmostId = await GetLeftmostLeafIdAsync();
        var frozen = new List<(IBPlusLeafGrain Leaf, LeafBaselineFreeze Freeze)>();
        var walk = new AtomicLeafWalk(nameof(CaptureSnapshotBaselineAsync));
        if (leftmostId is not null)
        {
            scan.Phase = ScanPagePhase.LeafWalk;
            var leafId = leftmostId.Value;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
                var freeze = await leaf.FreezeProjectionAsync(cancellationToken);
                frozen.Add((leaf, freeze));
                walk.RecordLeafVisited();
                scan.Budget.RecordLeafVisited();

                var next = await leaf.GetNextSiblingAsync();
                if (next is null)
                    break;
                leafId = next.Value;
            }
        }

        // Capture capturedHead AFTER every freeze has returned. Because each
        // freeze happened earlier in wall-clock than this head read,
        // head_now >= head_at_freeze >= frontier_at_freeze for every leaf and
        // partition, so frontier_p <= capturedHead_p uniformly with no
        // overshoot. That domination, not the exclusive hold, is what makes the
        // baseline a single consistent point: a leaf frozen early still receives
        // every write in [frontier, capturedHead] back from its tail fold, so
        // the materialised baseline equals the shard's state at capturedHead
        // however long the walk took and however many writes landed during it.
        // The uniform head also keeps a cross-leaf saga atomic: a terminal
        // beyond capturedHead leaves its saga pending (invisible) on every leaf
        // the saga touched.
        var capturedHead = await SnapshotWalHeadAsync(cancellationToken);

        // Pass 2 (fold + union): fold each leaf's own (frontier, capturedHead]
        // tail on top of its frozen cache and union the results. Leaves own
        // disjoint key ranges, so a collision here can only be a donor-orphan
        // duplicate left behind by an adaptive split; LWW-merging on collision
        // keeps the highest-timestamp variant (the snapshot leaf's read-time
        // IsKeyOwned filter then drops the orphan for the non-owning shard).
        //
        // The folds are fanned out because, unlike the chain walk above, this
        // pass is order-independent: every fold targets an already-captured head
        // and is a self-contained per-leaf call. Results are nonetheless
        // CONSUMED in strict leaf-chain order, so the union - including the
        // tie-breaking of the merge-mode adoption rule below, which is written
        // against a single accumulating pass - is byte-for-byte identical to a
        // serial fold. Only the dispatch schedule changes.
        //
        // The window is a ring buffer rather than a semaphore so that completed
        // -but-unconsumed results are bounded too: a plain gate would let folds
        // run arbitrarily far ahead of a slow leaf at the head of the chain and
        // pile every folded row set up in memory alongside the union.
        //
        // The union itself is a flat hash map carrying the per-key merge mode
        // alongside the value, and the ordinal ordering the baseline is
        // materialised in is imposed by one final key sort. A SortedDictionary
        // would instead allocate a red-black node per key and walk the tree
        // twice per row (once to probe, once to store), on top of a second hash
        // store into a parallel mode map and a third hash read to recover the
        // mode at materialise time. Folding the three keyed operations into one
        // single-probe write and sorting once at the end is output-identical:
        // both orders are ascending under StringComparer.Ordinal over a
        // distinct key set.
        //
        // Leaves own disjoint key ranges, so the union's final size is very
        // close to (leaf count x rows per leaf). Sizing it from the first
        // consumed leaf's row count therefore lands the backing store in one
        // shot instead of walking a grow-and-rehash chain - which matters more
        // here than for a typical map, because the value is a wide value tuple,
        // so every rehash copies it again. A red-black tree pays no grow chain
        // (each node is allocated once), so without this hint the flat map
        // trades bytes for the time it saves; with it, it wins on both.
        scan.Phase = ScanPagePhase.BaselineFold;
        var union = new Dictionary<string, (LwwValue<byte[]> Value, LatticeMergeMode? Mode)>(
            StringComparer.Ordinal);
        if (frozen.Count > 0)
        {
            var window = Math.Min(foldConcurrency, frozen.Count);
            var inFlight = new Task<IReadOnlyList<LeafSnapshotRow>>[window];
            for (var i = 0; i < window; i++)
                inFlight[i] = FoldLeafTailAsync(i);

            try
            {
                var sized = false;
                for (var i = 0; i < frozen.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var slot = i % window;
                    var rows = await inFlight[slot];

                    var dispatch = i + window;
                    inFlight[slot] = dispatch < frozen.Count
                        ? FoldLeafTailAsync(dispatch)
                        : CompletedNoRows;

                    if (!sized)
                    {
                        sized = true;
                        // Bounded so a single oversized first leaf cannot
                        // reserve an unreasonable amount on behalf of leaves
                        // that turn out small.
                        var estimate = Math.Min((long)rows.Count * frozen.Count, SnapshotUnionCapacityHintLimit);
                        if (estimate > 0)
                            union.EnsureCapacity((int)estimate);
                    }

                    FoldRowsIntoUnion(rows, union);
                }
            }
            catch
            {
                // One fold faulting (or a cancellation) abandons the rest of the
                // window. They are read-only calls, so abandoning them is safe,
                // but their faults must still be observed or they surface later
                // as TaskScheduler.UnobservedTaskException on a finalizer thread,
                // attributed to nothing.
                ObserveOutstandingFolds(inFlight);
                throw;
            }
        }

        // Reported once for the WHOLE hold, both passes included. The fold pass
        // dominates, so timing only the chain walk would under-report the hold
        // this diagnostic exists to surface.
        walk.ReportIfSlow(logger, context.GrainId);

        var orderedKeys = new string[union.Count];
        union.Keys.CopyTo(orderedKeys, 0);
        Array.Sort(orderedKeys, StringComparer.Ordinal);

        var materialised = new List<LeafSnapshotRow>(orderedKeys.Length);
        long rowBytes = 0;
        foreach (var key in orderedKeys)
        {
            var (value, mergeMode) = union[key];
            materialised.Add(new LeafSnapshotRow(key, value, mergeMode));
            rowBytes += LeafEntryCache.EntryBytes(key, value.IsTombstone ? null : value.Value);
        }

        var baseline = new SnapshotShardBaseline
        {
            Rows = materialised,
            CapturedHeadPerPartition = capturedHead,
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            RowBytes = rowBytes,
        };

        // Issue #916: seed the materialised baseline directly into the transient
        // per-shard snapshot leaf the cursor will reach, in memory, instead of
        // writing it to durable storage here. A snapshot that drains in a single
        // page never touches the durable store at all (no capture-time write, no
        // close-time delete). The leaf flushes the baseline to
        // ISnapshotBaselineStorageGrain lazily, only once the owning cursor's
        // first page returns HasMore = true (EnsurePersistedAsync), so any cursor
        // that survives past page 1 still has a durable baseline before the
        // client ever sees a continuation token.
        var snapshotLeaf = grainFactory.GetGrain<ISnapshotLeafGrain>(
            SnapshotLeafGrain.BuildBaselineKey(TreeId, ShardIndex, token));
        await snapshotLeaf.SeedAsync(TreeId, ShardIndex, baseline, token, cancellationToken);

        return new SnapshotBaselineCaptureResult(capturedHead, materialised.Count);

        Task<IReadOnlyList<LeafSnapshotRow>> FoldLeafTailAsync(int index)
        {
            var (leaf, freeze) = frozen[index];
            return leaf.FoldTailOntoFrozenAsync(freeze, capturedHead, cancellationToken);
        }
    }

    /// <summary>
    /// A shared already-completed empty fold result, so draining the tail of the
    /// fan-out window allocates nothing.
    /// </summary>
    private static readonly Task<IReadOnlyList<LeafSnapshotRow>> CompletedNoRows =
        Task.FromResult<IReadOnlyList<LeafSnapshotRow>>([]);

    /// <summary>
    /// Marks every still-outstanding fold in an abandoned fan-out window as
    /// observed. The folds are read-only, so leaving them running is harmless,
    /// but an unobserved faulted <see cref="Task"/> is not: it is re-raised on a
    /// finalizer thread as <see cref="TaskScheduler.UnobservedTaskException"/>,
    /// long after the call it belonged to, with no context attached.
    /// </summary>
    private static void ObserveOutstandingFolds(Task<IReadOnlyList<LeafSnapshotRow>>[] folds)
    {
        foreach (var fold in folds)
        {
            if (fold.IsCompleted)
            {
                _ = fold.Exception;
                continue;
            }

            _ = fold.ContinueWith(
                static completed => _ = completed.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Ceiling on the snapshot union's capacity hint, so an atypically large
    /// first leaf cannot reserve an unreasonable backing store on behalf of the
    /// rest. Above this the union simply grows as before.
    /// </summary>
    private const int SnapshotUnionCapacityHintLimit = 1 << 20;

    /// <summary>
    /// Folds one leaf's rows into the cross-leaf snapshot union with a single
    /// hash probe per row.
    /// </summary>
    /// <remarks>
    /// Synchronous by necessity: <c>ref</c> locals - and therefore
    /// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey,TValue}"/> -
    /// are not permitted inside an <c>async</c> method, so the row loop is
    /// lifted out of <see cref="CaptureSnapshotBaselineAsync"/>. Nothing here
    /// mutates <paramref name="union"/> other than through the returned slot,
    /// so the reference stays valid for the whole of each iteration.
    /// <para>
    /// Internal rather than private so the microbenchmark suite can measure the
    /// real production fold rather than a transcription of it.
    /// </para>
    /// </remarks>
    internal static void FoldRowsIntoUnion(
        IReadOnlyList<LeafSnapshotRow> rows,
        Dictionary<string, (LwwValue<byte[]> Value, LatticeMergeMode? Mode)> union)
    {
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(union, row.Key, out var existed);
            if (!existed)
            {
                slot = (row.Value, row.MergeMode);
                continue;
            }

            var merged = LwwValue<byte[]>.Merge(slot.Value, row.Value);
            // On a donor-orphan collision the per-key merge mode must follow
            // whichever value the LWW merge kept. If the incoming row won (its
            // timestamp is the survivor), adopt its mode - including a null
            // mode, which clears the stored one; otherwise leave the existing
            // mode untouched.
            var mode = ReferenceEquals(merged.Value, row.Value.Value)
                || merged.Timestamp.Equals(row.Value.Timestamp)
                ? row.MergeMode
                : slot.Mode;
            slot = (merged, mode);
        }
    }
}

