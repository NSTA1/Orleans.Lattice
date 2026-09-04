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
    public async Task<ShardMaterialiserLagPage> GetShardMaterialiserLagBoundedAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken)
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

        var walk = BoundedLeafWalk.FromResolvedStart(
            grainFactory,
            startLeafId,
            resumeFromInclusive,
            LeafWalkBudget.ForScanPage(await GetOptionsAsync()));

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
    public async Task<SnapshotBaselineCaptureResult> CaptureSnapshotBaselineAsync(Guid token, CancellationToken cancellationToken)
    {
        if (token == Guid.Empty)
            throw new ArgumentException("Snapshot baseline token must not be empty.", nameof(token));
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Pass 1 (freeze): walk this shard's leaf chain and freeze each leaf's
        // committed cache, per-partition projection frontier, and in-flight
        // prepared sagas. Reading the next-sibling pointer before the freeze is
        // unnecessary here (the freeze is read-only and does not deactivate the
        // leaf), but we keep the leaf handle alongside its freeze so the fold
        // pass can target the exact same leaf with the uniform capturedHead.
        var leftmostId = await GetLeftmostLeafIdAsync();
        var frozen = new List<(IBPlusLeafGrain Leaf, LeafBaselineFreeze Freeze)>();
        if (leftmostId is not null)
        {
            var leafId = leftmostId.Value;
            // NOT WORK-BOUNDED, and a budget is not the fix (issue 1956).
            // capturedHead is read AFTER every freeze has returned (see below),
            // which is what makes the baseline a single consistent point.
            // Releasing the shard mid-freeze would let writes land between two
            // leaves' freezes, so the snapshot cursor's zero-observable-writes
            // guarantee would not hold.
            //
            // The real fix is to invert that dependency - capture the head
            // first and freeze each leaf AT it - which makes the walk
            // resumable: tracked as issue 1961. Until then this is
            // instrumented, not fixed.
            var walk = new AtomicLeafWalk(nameof(CaptureSnapshotBaselineAsync));
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
                var freeze = await leaf.FreezeProjectionAsync(cancellationToken);
                frozen.Add((leaf, freeze));
                walk.RecordLeafVisited();

                var next = await leaf.GetNextSiblingAsync();
                if (next is null)
                    break;
                leafId = next.Value;
            }

            walk.ReportIfSlow(logger, context.GrainId);
        }

        // Capture capturedHead AFTER every freeze has returned. Because each
        // freeze happened earlier in wall-clock than this head read,
        // head_now >= head_at_freeze >= frontier_at_freeze for every leaf and
        // partition, so frontier_p <= capturedHead_p uniformly with no
        // overshoot. The uniform head also keeps a cross-leaf saga atomic: a
        // terminal beyond capturedHead leaves its saga pending (invisible) on
        // every leaf the saga touched.
        var capturedHead = await SnapshotWalHeadAsync(cancellationToken);

        // Pass 2 (fold + union): fold each leaf's own (frontier, capturedHead]
        // tail on top of its frozen cache and union the results. Leaves own
        // disjoint key ranges, so a collision here can only be a donor-orphan
        // duplicate left behind by an adaptive split; LWW-merging on collision
        // keeps the highest-timestamp variant (the snapshot leaf's read-time
        // IsKeyOwned filter then drops the orphan for the non-owning shard).
        var union = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var unionModes = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        foreach (var (leaf, freeze) in frozen)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rows = await leaf.FoldTailOntoFrozenAsync(freeze, capturedHead, cancellationToken);
            foreach (var row in rows)
            {
                if (union.TryGetValue(row.Key, out var existing))
                {
                    var merged = LwwValue<byte[]>.Merge(existing, row.Value);
                    union[row.Key] = merged;
                    // On a donor-orphan collision the per-key merge mode must
                    // follow whichever value the LWW merge kept. If the incoming
                    // row won (its timestamp is the survivor), adopt its mode;
                    // otherwise leave the existing mode untouched.
                    if (ReferenceEquals(merged.Value, row.Value.Value)
                        || merged.Timestamp.Equals(row.Value.Timestamp))
                    {
                        if (row.MergeMode is { } wonMode)
                            unionModes[row.Key] = wonMode;
                        else
                            unionModes.Remove(row.Key);
                    }
                }
                else
                {
                    union[row.Key] = row.Value;
                    if (row.MergeMode is { } mode)
                        unionModes[row.Key] = mode;
                }
            }
        }

        var materialised = new List<LeafSnapshotRow>(union.Count);
        long rowBytes = 0;
        foreach (var (key, value) in union)
        {
            var mergeMode = unionModes.TryGetValue(key, out var m) ? m : (LatticeMergeMode?)null;
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
    }
}

