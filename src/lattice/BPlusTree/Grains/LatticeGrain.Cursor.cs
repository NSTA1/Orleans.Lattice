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
    /// carries the kind through to the cursor grain. Saga visibility is
    /// fixed by the frozen per-shard baselines, not by a registry-decision
    /// snapshot: each shard's baseline is folded to one uniform WAL head, so
    /// a saga whose commit terminal lands beyond that head stays pending, and
    /// invisible, on every leaf of that shard it touched - see
    /// <see cref="LatticeSnapshotCoordinate"/>.
    /// <para>
    /// After the saturation shed below, the open runs these steps in order:
    /// </para>
    /// <list type="number">
    /// <item><description>
    /// Resolve routing force-refreshed from the registry
    /// (<see cref="GetRoutingAsync(bool, CancellationToken)"/>) to pin the
    /// <see cref="ShardMap.Version"/> the cursor will route against for its
    /// lifetime.
    /// </description></item>
    /// <item><description>
    /// Fan out <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.CaptureSnapshotBaselineAsync"/>
    /// across every physical shard, whatever range the cursor covers and at
    /// most <see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/> at a
    /// time, to freeze a per-cursor baseline (each shard's leaf-chain
    /// projection at a uniform per-partition captured WAL head), seeded in
    /// memory into transient per-shard snapshot leaves (persisted only once
    /// the cursor pages past its first page). The cursor serves those rows
    /// with no WAL replay, so it does not depend on WAL retention.
    /// </description></item>
    /// <item><description>
    /// Gate the open against
    /// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/> using the
    /// largest per-shard baseline row count (the snapshot leaf serves exactly
    /// these rows; it no longer replays the WAL at serve time), failing with
    /// <see cref="LatticeSnapshotReplayBudgetExceededException"/> before any
    /// cursor is opened. The gate runs after the capture, so when it refuses
    /// the baselines have already been materialised and seeded in memory;
    /// they are never persisted.
    /// </description></item>
    /// <item><description>
    /// Fetch a registry-decision snapshot. Only the anchor derived from it,
    /// <see cref="LatticeSnapshotCoordinate.RegistrySnapshotHlc"/>, reaches the
    /// coordinate, and that is currently always
    /// <see cref="Orleans.Lattice.HybridLogicalClock.Zero"/>; the decisions
    /// themselves are not carried to the cursor.
    /// </description></item>
    /// <item><description>
    /// Build the coordinate and open the cursor grain with it.
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
        // with a typed back-pressure error whose source is SnapshotCursorOpen, so a saturated tree
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
                TreeId,
                LatticeSaturationSource.SnapshotCursorOpen);
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
        // tail exactly once, and seeds the materialised per-shard baseline,
        // keyed by this open's baseline token, into the transient snapshot
        // leaf's memory (persisted only once the cursor pages past its first
        // page, issue #916). Serving the cursor
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

        // Step 3: replay-budget gate. With the frozen-baseline store the
        // per-shard cost is the materialised baseline row count (what the
        // snapshot leaf seeds into memory), NOT the captured WAL head: after
        // a GC trim the head can be arbitrarily large while the real
        // projection is tiny. Compare against the deepest shard rather than
        // the sum because the baselines are seeded in parallel and the
        // operator-facing knob is "per shard", mirroring MaxLeafReplayEntries.
        // The capture above has already seeded every baseline by now, so the
        // gate refuses the cursor, not the capture cost.
        var opts = Options;
        if (opts.MaxSnapshotReplayEntries > 0 && maxBaselineRows > opts.MaxSnapshotReplayEntries)
        {
            throw new LatticeSnapshotReplayBudgetExceededException(
                $"Snapshot open for tree '{TreeId}' would materialise {maxBaselineRows} baseline rows on the deepest shard, " +
                $"exceeding LatticeOptions.MaxSnapshotReplayEntries={opts.MaxSnapshotReplayEntries}. " +
                "Trigger a leaf-projection rebuild (RebuildLeafProjectionAsync) or raise the cap.");
        }

        // Step 4: registry-decision snapshot. Only the HLC anchor derived from
        // it reaches the coordinate: the dictionary is not handed to the cursor
        // grain, which keeps no registry snapshot for a snapshot cursor,
        // because the frozen baselines captured above already fix saga
        // visibility. A registry transport failure yields a null snapshot and
        // the open proceeds; any other registry fault propagates and fails the
        // open.
        var registrySnapshot = (await FetchRegistrySnapshotAsync()).Snap;
        cancellationToken.ThrowIfCancellationRequested();
        // ComputeRegistrySnapshotHlc currently always returns
        // HybridLogicalClock.Zero: the registry snapshot DTO carries no
        // per-decision HLCs to take a maximum over. The cursor uses the
        // value only as a diagnostic anchor (and as its WAL cursor-registry
        // position, where Zero holds back nothing); visibility does not
        // depend on it.
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

            // Pin the resolved physical tree id so the cursor's open/read path
            // keys the transient snapshot leaves (and their durable baseline
            // rows) by the same tree id the physical shard roots used at
            // capture/seed time. After a ShadowCutover restore the logical tree
            // aliases to a fresh physical tree, so keying the leaf by the
            // logical id would miss the seeded activation and force a
            // from-storage reload that throws LatticeSnapshotExpiredException
            // (issue #1386).
            PhysicalTreeId = physicalTreeId,
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
    /// are blocked on <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.CaptureSnapshotBaselineAsync"/>
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
    /// Computes the HLC anchor for a captured registry snapshot, stamped on
    /// <see cref="LatticeSnapshotCoordinate.RegistrySnapshotHlc"/>. It
    /// currently always returns <see cref="Orleans.Lattice.HybridLogicalClock.Zero"/>,
    /// so every consumer of that field observes Zero - including the snapshot
    /// cursor's WAL cursor-registry position (which therefore holds back no
    /// trimming). The backup capture does not take its consistency-cut HLC from
    /// this anchor alone; it uses the highest HLC over the entries it captured.
    /// Visibility gating is driven by neither this anchor nor the snapshot
    /// dictionary, which is discarded here: a snapshot cursor's saga visibility
    /// is fixed by its frozen per-shard baselines.
    /// </summary>
    private static Orleans.Lattice.HybridLogicalClock ComputeRegistrySnapshotHlc(
        Dictionary<Guid, TxStatus>? snapshot)
    {
        // The registry's per-decision HLCs are not exposed on the
        // current snapshot DTO, so we always anchor at Zero, and the
        // dictionary is not passed on: the cursor grain keeps no registry
        // snapshot for a snapshot cursor. A richer anchor
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
        var page = cursor.NextEntriesAsync(pageSize);
        // Read-path value-decoder boundary: strip the per-value envelope from
        // each entry of the page on the way out. Zero-cost when inactive (cached
        // bool) - the cursor's page task is returned directly on the default
        // null-decoder path, byte-for-byte identical to the pre-seam behaviour.
        return ValueDecoderActive ? DecodeCursorPageAsync(page, cancellationToken) : page;
    }

    /// <summary>
    /// Awaits a cursor entries page and returns a copy whose entry values have
    /// had their per-value envelope stripped. Only invoked on the
    /// active-decoder branch, so the page rebuild it allocates is never paid on
    /// the default null-decoder path.
    /// </summary>
    private async Task<LatticeCursorEntriesPage> DecodeCursorPageAsync(
        Task<LatticeCursorEntriesPage> pageTask,
        CancellationToken cancellationToken)
    {
        var page = await pageTask;
        if (page.Entries.Count == 0)
        {
            return page;
        }

        var decoded = new List<KeyValuePair<string, byte[]>>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            var value = await DecodeValueAsync(entry.Value, cancellationToken);
            decoded.Add(new KeyValuePair<string, byte[]>(entry.Key, value!));
        }

        return page with { Entries = decoded };
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
