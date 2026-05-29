namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateful cursor forwarding. Each <c>ILattice</c> cursor method
/// simply routes to a per-<c>{treeId}/{cursorId}</c>
/// <see cref="ILatticeCursorGrain"/> activation where the real work and
/// state persistence happens.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task<string> OpenKeyCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = reverse,
            PointInTime = pointInTime,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public async Task<string> OpenEntryCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Entries,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = reverse,
            PointInTime = pointInTime,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public Task<string> OpenSnapshotKeyCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Keys, startInclusive, endExclusive, reverse, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenSnapshotEntryCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Entries, startInclusive, endExclusive, reverse, cancellationToken);

    /// <summary>
    /// Shared open path for zero-observable-writes snapshot cursors.
    /// Both <see cref="LatticeCursorKind.Keys"/> and
    /// <see cref="LatticeCursorKind.Entries"/> route here; the spec
    /// carries the kind through to the cursor grain. Snapshot cursors
    /// are also point-in-time so saga decisions captured at open time
    /// are frozen alongside the per-shard WAL offsets - see
    /// <see cref="LatticeSnapshotCoordinate"/>.
    /// <para>
    /// Capture is a four-step fan-out:
    /// </para>
    /// <list type="number">
    /// <item><description>
    /// Resolve current routing (<see cref="GetRoutingAsync(CancellationToken)"/>)
    /// to pin the <see cref="ShardMap.Version"/> the cursor will route
    /// against for its lifetime.
    /// </description></item>
    /// <item><description>
    /// Fan out <see cref="IShardRootGrain.SnapshotWalHeadAsync"/> across
    /// every physical shard to capture per-shard next-to-be-assigned WAL
    /// offsets concurrently.
    /// </description></item>
    /// <item><description>
    /// Take a registry-decision snapshot via the per-tree
    /// <see cref="ITxRegistryGrain"/> so saga decisions captured at open
    /// time are frozen for the cursor's lifetime, mirroring the
    /// point-in-time cursor path.
    /// </description></item>
    /// <item><description>
    /// Gate the open against
    /// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/> using the
    /// largest captured WAL offset as a conservative per-shard cost
    /// projection (each captured offset is the count of records the
    /// snapshot leaf will replay for that shard). Fail-fast with
    /// <see cref="LatticeSnapshotReplayBudgetExceededException"/> so an
    /// expensive open does not silently consume per-shard memory.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task<string> OpenSnapshotCursorAsync(
        LatticeCursorKind kind,
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        CancellationToken cancellationToken)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        var (physicalTreeId, shardMap) = await GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Step 2: per-shard per-partition WAL-head capture, concurrent
        // across shards. The fan-out is intentionally not linearizable
        // across shards in real time - the registry snapshot below
        // resolves cross-shard saga visibility uniformly so the union
        // of all captures still encodes a deterministic tree-wide
        // view. Each shard returns one offset per WAL partition
        // (length equals the tree's pinned WalPartitions) so the
        // snapshot leaf can drive its per-partition replay with
        // saga-atomicity preserved across the multi-partition
        // boundary.
        var headTasks = new Task<long[]>[physicalShards.Count];
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var shard = GetShardGrainByIndex(physicalTreeId, physicalShards[i]);
            headTasks[i] = shard.SnapshotWalHeadAsync(cancellationToken);
        }
        await Task.WhenAll(headTasks);
        cancellationToken.ThrowIfCancellationRequested();

        var perShardPerPartitionOffsets = new Dictionary<int, IReadOnlyList<long>>(physicalShards.Count);
        long maxOffset = 0;
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var perPartition = headTasks[i].Result;
            perShardPerPartitionOffsets[physicalShards[i]] = perPartition;
            for (var p = 0; p < perPartition.Length; p++)
            {
                if (perPartition[p] > maxOffset) maxOffset = perPartition[p];
            }
        }

        // Step 4: replay-budget gate. Each shard's snapshot leaf will
        // replay records [0, capturedOffset), so MaxSnapshotReplayEntries
        // bounds the per-shard rebuild cost. We compare against the
        // deepest shard rather than the sum because the leaves rebuild
        // in parallel and the operator-facing knob is "per shard" - the
        // same shape as MaxLeafReplayEntries on activation-time replay.
        var opts = Options;
        if (opts.MaxSnapshotReplayEntries > 0 && maxOffset > opts.MaxSnapshotReplayEntries)
        {
            throw new LatticeSnapshotReplayBudgetExceededException(
                $"Snapshot open for tree '{TreeId}' would replay {maxOffset} WAL entries on the deepest shard, " +
                $"exceeding LatticeOptions.MaxSnapshotReplayEntries={opts.MaxSnapshotReplayEntries}. " +
                "Trigger a leaf-projection rebuild (RebuildLeafProjectionAsync) or raise the cap.");
        }

        // Step 3: registry-decision snapshot, mirroring the
        // point-in-time cursor path. A failure here returns null - the
        // cursor grain will treat that as "no sagas captured" and
        // proceed; the snapshot semantics weaken to "WAL-only" rather
        // than failing the open.
        var registrySnapshot = (await FetchRegistrySnapshotAsync()).Snap;
        cancellationToken.ThrowIfCancellationRequested();
        // The HLC stamped on the snapshot is the maximum HLC observed
        // across every captured decision; HybridLogicalClock.Zero when
        // the registry was empty. The cursor uses it only as a
        // diagnostic anchor - the registry snapshot dictionary itself
        // (transferred to LatticeRegistrySnapshotContext via the cursor
        // grain) is what gates visibility.
        var registryHlc = ComputeRegistrySnapshotHlc(registrySnapshot);

        var coordinate = new LatticeSnapshotCoordinate(
            shardMap.Version,
            perShardPerPartitionOffsets,
            registryHlc);

        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenSnapshotAsync(TreeId, new LatticeCursorSpec
        {
            Kind = kind,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = reverse,
            PointInTime = true,
            ZeroObservableWrites = true,
        }, coordinate);
        return cursorId;
    }

    /// <summary>
    /// Computes the HLC anchor for a captured registry snapshot. Used
    /// only as a diagnostic field on
    /// <see cref="LatticeSnapshotCoordinate.RegistrySnapshotHlc"/>;
    /// visibility gating is driven by the snapshot dictionary itself,
    /// not by this anchor.
    /// </summary>
    private static Primitives.HybridLogicalClock ComputeRegistrySnapshotHlc(
        Dictionary<Guid, TxStatus>? snapshot)
    {
        // The registry's per-decision HLCs are not exposed on the
        // current snapshot DTO, so we anchor at Zero when the snapshot
        // is null or empty and let the cursor grain treat the captured
        // dictionary as the authoritative gating input. A richer anchor
        // (e.g. the head HLC of the registry tree at capture time)
        // would require a new registry-side accessor and is not
        // required for correctness here.
        _ = snapshot;
        return Primitives.HybridLogicalClock.Zero;
    }

    /// <inheritdoc />
    public async Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = false,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public Task<LatticeCursorKeysPage> NextKeysAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.NextKeysAsync(pageSize);
    }

    /// <inheritdoc />
    public Task<LatticeCursorEntriesPage> NextEntriesAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.NextEntriesAsync(pageSize);
    }

    /// <inheritdoc />
    public Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.DeleteRangeStepAsync(maxToDelete);
    }

    /// <inheritdoc />
    public Task CloseCursorAsync(string cursorId, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.CloseAsync();
    }

    /// <summary>
    /// Builds the <c>{treeId}/{cursorId}</c> composite key used to address a
    /// cursor grain activation.
    /// </summary>
    private string BuildCursorKey(string cursorId) => $"{TreeId}/{cursorId}";
}
