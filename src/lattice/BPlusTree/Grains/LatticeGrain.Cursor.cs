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
    public Task<string> OpenKeyCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
        => OpenKeyCursorCoreAsync(startInclusive, endExclusive, reverse, pointInTime, null, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenKeyCursorWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
        => OpenKeyCursorCoreAsync(startInclusive, endExclusive, reverse, pointInTime, predicate, cancellationToken);

    private async Task<string> OpenKeyCursorCoreAsync(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool pointInTime,
        LatticePredicateNode? predicate,
        CancellationToken cancellationToken)
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
            Predicate = predicate,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public Task<string> OpenEntryCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
        => OpenEntryCursorCoreAsync(startInclusive, endExclusive, reverse, pointInTime, null, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenEntryCursorWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
        => OpenEntryCursorCoreAsync(startInclusive, endExclusive, reverse, pointInTime, predicate, cancellationToken);

    private async Task<string> OpenEntryCursorCoreAsync(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool pointInTime,
        LatticePredicateNode? predicate,
        CancellationToken cancellationToken)
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
            Predicate = predicate,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public Task<string> OpenSnapshotKeyCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Keys, startInclusive, endExclusive, reverse, null, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenSnapshotKeyCursorWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Keys, startInclusive, endExclusive, reverse, predicate, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenSnapshotEntryCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Entries, startInclusive, endExclusive, reverse, null, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenSnapshotEntryCursorWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
        => OpenSnapshotCursorAsync(LatticeCursorKind.Entries, startInclusive, endExclusive, reverse, predicate, cancellationToken);

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
    /// Fan out <see cref="IShardRootGrain.CaptureSnapshotBaselineAsync"/>
    /// across every physical shard to freeze a durable, per-cursor frozen
    /// baseline (each shard's leaf-chain projection at a uniform
    /// per-partition captured WAL head) concurrently. The captured heads
    /// double as the per-shard WAL-retention pin offsets.
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
    /// largest per-shard frozen-baseline row count as a conservative
    /// per-shard cost projection (the snapshot leaf materialises exactly
    /// these rows; it no longer replays the WAL at serve time). Fail-fast
    /// with <see cref="LatticeSnapshotReplayBudgetExceededException"/> so an
    /// expensive open does not silently consume per-shard memory.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task<string> OpenSnapshotCursorAsync(
        LatticeCursorKind kind,
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        LatticePredicateNode? predicate,
        CancellationToken cancellationToken)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Admission control (issue #1053): a snapshot open freezes and
        // materialises every shard's leaf chain on the non-reentrant shard roots
        // - heavier than a single write. When the tree is already WAL-saturated,
        // fanning that capture out piles work onto roots collapsing under write
        // back-pressure, starving replication applies and reads queued on those
        // same roots and feeding a client-retry storm on the resulting timeout.
        // Shed the open here - before GetRoutingAsync and the capture fan-out -
        // with a typed, retryable back-pressure error, so a saturated tree
        // refuses the expensive open cheaply instead of amplifying its own
        // collapse. Only Saturated sheds (a Throttled tree is normal moderate
        // load and stays browsable), mirroring the atomic-write saga's quiesce
        // gate. Gated by the default-on ShedSnapshotOpensWhenSaturated option.
        // The signal is silo-local and best-effort: on a non-hosted test
        // activation it is absent and the check is a no-op.
        if (Options.ShedSnapshotOpensWhenSaturated &&
            ResolveSaturationSignal() is { } saturationSignal &&
            saturationSignal.GetCurrentState(TreeId) == WalSaturationState.Saturated)
        {
            throw new LatticeSaturatedException(
                $"Snapshot cursor open for tree '{TreeId}' refused: the tree is saturated " +
                "(WAL back-pressure); the per-shard baseline capture was not started. " +
                "Retry the open after backing off until the tree drains.",
                TreeId);
        }

        // Capture the routing map fresh from the registry (force-refresh)
        // rather than trusting this activation's cached map. A snapshot is a
        // frozen replay: unlike the live scan path it cannot dynamically
        // reconcile a topology change discovered mid-scan, so the shard set it
        // fans out across, the per-shard WAL offsets it captures, and the
        // pinned map it filters donor orphans against must all derive from one
        // authoritative, post-any-prior-split map. A stale cached map omits a
        // freshly-split target shard from the fan-out entirely (losing every
        // post-split write routed there) while still replaying the donor's
        // retained orphan copies, which both drops live data and resurrects
        // moved-away keys. See issue #907.
        var (physicalTreeId, shardMap) = await GetRoutingAsync(forceRefresh: true, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Step 2: per-shard frozen-baseline capture, concurrent across
        // shards. Each shard freezes its leaf chain, captures a uniform
        // per-partition WAL head, folds each leaf's own (frontier, head]
        // tail exactly once, and persists the materialised per-shard
        // baseline keyed by this open's baseline token. Serving the cursor
        // then reads those frozen rows with no WAL replay, so a later WAL
        // GC that trims the prefix cannot turn the scan empty/partial (the
        // bug this fixes). Per-shard ShardActivationRetry wrap: a single
        // shard's cold-start seed-timeout retries only that shard, not the
        // whole fan-out.
        //
        // The fan-out is bounded to MaxConcurrentSnapshotCaptures shards at a
        // time (via captureGate). Each capture blocks its shard root's
        // non-reentrant turn for the full leaf walk, so an unbounded fan-out
        // across a wide tree blocks every shard root at once, starving
        // replication applies and reads queued on those same roots. Bounding
        // it keeps all but the in-flight shards free; the captured baseline
        // and its point-in-time consistency are unchanged - only the dispatch
        // schedule differs (see issue #1054).
        var baselineToken = Guid.NewGuid();
        var captureConcurrency = Math.Max(1, Options.MaxConcurrentSnapshotCaptures);
        using var captureGate = new SemaphoreSlim(captureConcurrency);
        var captureTasks = new Task<SnapshotBaselineCaptureResult>[physicalShards.Count];
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var shard = GetShardGrainByIndex(physicalTreeId, physicalShards[i]);
            captureTasks[i] = CaptureShardBaselineGatedAsync(
                shard, baselineToken, captureGate, cancellationToken);
        }
        await Task.WhenAll(captureTasks);
        cancellationToken.ThrowIfCancellationRequested();

        var perShardPerPartitionOffsets = new Dictionary<int, IReadOnlyList<long>>(physicalShards.Count);
        long maxBaselineRows = 0;
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var capture = captureTasks[i].Result;
            perShardPerPartitionOffsets[physicalShards[i]] = capture.CapturedHeadPerPartition;
            if (capture.RowCount > maxBaselineRows) maxBaselineRows = capture.RowCount;
        }

        // Step 4: replay-budget gate. With the frozen-baseline store the
        // per-shard cost is the materialised baseline row count (what the
        // snapshot leaf seeds into memory), NOT the captured WAL head: after
        // a GC trim the head can be arbitrarily large while the real
        // projection is tiny. Compare against the deepest shard rather than
        // the sum because the baselines are seeded in parallel and the
        // operator-facing knob is "per shard", mirroring MaxLeafReplayEntries.
        var opts = Options;
        if (opts.MaxSnapshotReplayEntries > 0 && maxBaselineRows > opts.MaxSnapshotReplayEntries)
        {
            throw new LatticeSnapshotReplayBudgetExceededException(
                $"Snapshot open for tree '{TreeId}' would materialise {maxBaselineRows} baseline rows on the deepest shard, " +
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
            registryHlc)
        {
            // Pin the routing map so each snapshot leaf can drop donor-orphan
            // keys whose virtual slot the map no longer assigns to it (see
            // LatticeSnapshotCoordinate.PinnedShardMap). Only needed when the
            // fan-out covers more than one physical shard - a single-shard
            // snapshot has no sibling that could hold an orphan copy, so we
            // leave the slot null there to avoid persisting the full slot
            // array for the common no-split case.
            PinnedShardMap = physicalShards.Count > 1 ? shardMap : null,

            // Per-cursor frozen-baseline identity. The per-shard baseline rows
            // captured above are persisted under this token; the snapshot
            // leaves load and serve them instead of replaying the WAL, and the
            // cursor close path deletes them by re-deriving the same keys.
            SnapshotBaselineToken = baselineToken,
        };

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
            Predicate = predicate,
        }, coordinate);
        return cursorId;
    }

    /// <summary>
    /// Captures one shard's snapshot baseline while holding a slot in
    /// <paramref name="gate"/>, so no more than
    /// <see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/> shard roots
    /// are blocked on <see cref="IShardRootGrain.CaptureSnapshotBaselineAsync"/>
    /// at once. The per-shard <see cref="ShardActivationRetry"/> wrap is
    /// preserved so a single shard's cold-start seed-timeout retries only that
    /// shard. The slot is released once the capture completes (or throws) so
    /// the remaining queued captures can drain their waits.
    /// </summary>
    private static async Task<SnapshotBaselineCaptureResult> CaptureShardBaselineGatedAsync(
        IShardRootGrain shard,
        Guid baselineToken,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken);
        try
        {
            return await ShardActivationRetry.RunAsync(
                () => shard.CaptureSnapshotBaselineAsync(baselineToken, cancellationToken),
                cancellationToken);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Computes the HLC anchor for a captured registry snapshot. Used
    /// only as a diagnostic field on
    /// <see cref="LatticeSnapshotCoordinate.RegistrySnapshotHlc"/>;
    /// visibility gating is driven by the snapshot dictionary itself,
    /// not by this anchor.
    /// </summary>
    private static Orleans.Lattice.HybridLogicalClock ComputeRegistrySnapshotHlc(
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
        return Orleans.Lattice.HybridLogicalClock.Zero;
    }

    /// <inheritdoc />
    public Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
        => OpenDeleteRangeCursorCoreAsync(startInclusive, endExclusive, null, cancellationToken);

    /// <inheritdoc />
    public Task<string> OpenDeleteRangeCursorWherePredicateAsync(LatticePredicateNode predicate, string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
        => OpenDeleteRangeCursorCoreAsync(startInclusive, endExclusive, predicate, cancellationToken);

    private async Task<string> OpenDeleteRangeCursorCoreAsync(string startInclusive, string endExclusive, LatticePredicateNode? predicate, CancellationToken cancellationToken)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
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
            Predicate = predicate,
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
        ThrowIfProtectedView();
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
