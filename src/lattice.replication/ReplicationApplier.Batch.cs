using Orleans.Lattice.BPlusTree.Grains;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Optimised batch-apply path for <see cref="ReplicationApplier"/>.
/// Groups the inbound batch into contiguous same-<c>(treeId, originClusterId)</c>
/// runs and collapses the per-entry per-origin high-water-mark
/// round-trips to a single
/// <see cref="IReplicationHighWaterMarkGrain.GetAsync"/> at the start
/// of each run plus a single
/// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> at the
/// end. The causal-apply buffer is drained once at the end of each
/// run that advanced the persisted HWM rather than after every
/// successful apply, and the local vector clock is fetched at most
/// once per run on demand (only when the first causal-dep entry is
/// seen) and re-fetched only when an apply has happened since.
/// </summary>
internal sealed partial class ReplicationApplier
{
    /// <summary>
    /// Upper bound on the capacity hint given to the batched-apply pending
    /// buckets in <see cref="ApplyOriginRunAsync"/>. The run length is an exact
    /// upper bound on a bucket's final size, but a long run may defer only a
    /// few entries (the rest deduped, rejected or causally parked), so the hint
    /// is clamped to keep a pathological run from over-allocating.
    /// </summary>
    private const int PendingBatchCapacityHintLimit = 256;

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0)
        {
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        // SYSTEM-ORIGIN APPLY BYPASS (issue #982). Same rationale as ApplyAsync:
        // enter the system-origin scope for the whole batch so every apply the
        // multi-entry run path drives bypasses the receiver's access gate. The
        // single-entry fast path below defers to ApplyAsync, which enters the
        // scope itself; entering here as well is nest-safe (the inner enter
        // saves and restores the outer state) so the double-cover is harmless.
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();

        // Single-entry: defer to the per-entry path so behaviour is
        // bit-identical with the legacy receiver. The per-entry path
        // already covers every classification (range delete, local-origin
        // defence, dedup, causal-park, success). Inbound-direction
        // peer-stats recording still fires here so a transport that
        // ships one-entry batches surfaces the same
        // <c>peer.last_contact_seconds{direction="inbound"}</c> signal
        // as multi-entry batches.
        if (entries.Count == 1)
        {
            var single = entries[0];
            try
            {
                var r = await ApplyAsync(single, cancellationToken).ConfigureAwait(false);
                RecordInboundContact(single, success: true);
                return r;
            }
            catch
            {
                RecordInboundContact(single, success: false);
                throw;
            }
        }

        // The inbound batch is walked as a sequence of contiguous
        // same-(treeId, origin) runs. The receiver protocol guarantees
        // the batch is shipped from a single producer in WAL order so a
        // 256-entry inbound batch from one origin collapses to a single
        // run.
        //
        // Independent runs - those targeting distinct trees - may apply
        // concurrently up to the host-configured
        // <see cref="LatticeReplicationOptions.ApplyMaxParallelRuns"/>
        // degree of parallelism, bounding apply latency (and the
        // resulting apply.lag back-pressure) under multi-tree load.
        // Parallelism is only ever introduced across independent runs:
        // runs that share a tree stay strictly sequential in WAL order,
        // so the per-tree causal-apply buffer, the shadow-forward
        // dedupe cache, per-origin FIFO, and per-origin high-water-mark
        // monotonicity all observe the identical access order they
        // would under fully-sequential apply. The default DOP of 1
        // takes the sequential walk, bit-identical to the historical
        // behaviour.
        var plan = BuildParallelApplyPlanOrNull(entries);
        if (plan is null)
        {
            LatticeReplicationMetrics.ApplyParallelRuns.Record(1, LatticeTenantLabel.Platform);
            return await ApplyRunsSequentiallyAsync(entries, cancellationToken).ConfigureAwait(false);
        }

        return await ApplyRunsInParallelAsync(entries, plan.Value, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Applies every contiguous <c>(treeId, originClusterId)</c> run in
    /// the inbound batch strictly sequentially, in write-ahead-log
    /// order, awaiting each before starting the next. This is the
    /// fully-sequential apply path used whenever cross-tree parallelism
    /// is disabled (the default
    /// <see cref="LatticeReplicationOptions.ApplyMaxParallelRuns"/> of
    /// <c>1</c>) or impossible (a single-tree batch). It preserves the
    /// historical apply ordering and allocation profile exactly.
    /// </summary>
    private async Task<ApplyResult> ApplyRunsSequentiallyAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken)
    {
        var anyApplied = false;
        var highest = HybridLogicalClock.Zero;
        var anyDeferred = false;
        var i = 0;
        while (i < entries.Count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var startTreeId = entries[i].TreeId;
            var startOrigin = entries[i].OriginClusterId;
            // The wire merge-mode is part of the run key, not just the run's
            // first entry. The receiver gate classifies a run from its
            // representative first entry, while dispatch inside the run switches
            // on each entry's own Mode - so segmenting on (tree, origin) alone
            // let a peer head a run with one conforming entry and smuggle
            // entries carrying an arbitrary merge algebra behind it, past the
            // gate. Including Mode means a mode change starts a new run that is
            // classified on its own merits and dead-lettered if it disagrees
            // with the locally-resolved mode. A legitimate batch carries a
            // batch-constant mode per run, so this never splits a well-formed
            // run and costs one extra comparison per entry.
            var startMode = entries[i].Mode;
            var j = i + 1;
            while (j < entries.Count
                && string.Equals(entries[j].TreeId, startTreeId, StringComparison.Ordinal)
                && string.Equals(entries[j].OriginClusterId, startOrigin, StringComparison.Ordinal)
                && entries[j].Mode == startMode)
            {
                j++;
            }

            var runResult = await ApplyRunSegmentAsync(entries, i, j, cancellationToken).ConfigureAwait(false);
            if (runResult.Applied)
            {
                anyApplied = true;
            }
            if (runResult.Deferred)
            {
                anyDeferred = true;
            }
            if (runResult.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = runResult.HighWaterMark;
            }
            i = j;
        }

        return new ApplyResult { Applied = anyApplied, HighWaterMark = highest, Deferred = anyDeferred };
    }

    /// <summary>
    /// Applies the inbound batch's tree-groups concurrently up to the
    /// effective degree of parallelism recorded on
    /// <paramref name="plan"/>. Each tree-group applies its own runs
    /// strictly sequentially in write-ahead-log order on a single task,
    /// so within-tree ordering (per-origin FIFO, causal gating, HWM
    /// monotonicity, atomic-batch boundaries) is preserved exactly;
    /// only runs across distinct trees - which share no per-tree state -
    /// overlap. The per-group results are aggregated after every group
    /// completes, so the returned <see cref="ApplyResult"/> is
    /// order-independent (logical-OR of <see cref="ApplyResult.Applied"/>
    /// and max of <see cref="ApplyResult.HighWaterMark"/>).
    /// </summary>
    private async Task<ApplyResult> ApplyRunsInParallelAsync(
        IReadOnlyList<WalRecord> entries,
        ParallelApplyPlan plan,
        CancellationToken cancellationToken)
    {
        LatticeReplicationMetrics.ApplyParallelRuns.Record(plan.EffectiveDegreeOfParallelism, LatticeTenantLabel.Platform);

        using var throttle = new SemaphoreSlim(plan.EffectiveDegreeOfParallelism);
        var tasks = new Task<ApplyResult>[plan.TreeOrder.Count];
        for (var g = 0; g < plan.TreeOrder.Count; g++)
        {
            var segments = plan.Groups[plan.TreeOrder[g]];
            tasks[g] = ApplyTreeGroupAsync(entries, segments, throttle, cancellationToken);
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);

        var anyApplied = false;
        var highest = HybridLogicalClock.Zero;
        var anyDeferred = false;
        foreach (var runResult in results)
        {
            if (runResult.Applied)
            {
                anyApplied = true;
            }
            if (runResult.Deferred)
            {
                anyDeferred = true;
            }
            if (runResult.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = runResult.HighWaterMark;
            }
        }

        return new ApplyResult { Applied = anyApplied, HighWaterMark = highest, Deferred = anyDeferred };
    }

    /// <summary>
    /// Applies one tree's runs sequentially under a bounded-concurrency
    /// gate. The run sequence preserves write-ahead-log order so the
    /// per-tree causal-apply buffer and shadow-forward dedupe cache see
    /// the identical access order the sequential path would produce.
    /// The <paramref name="throttle"/> semaphore bounds how many
    /// tree-groups apply at once across the whole batch.
    /// </summary>
    private async Task<ApplyResult> ApplyTreeGroupAsync(
        IReadOnlyList<WalRecord> entries,
        List<(int Start, int End)> segments,
        SemaphoreSlim throttle,
        CancellationToken cancellationToken)
    {
        await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var anyApplied = false;
            var highest = HybridLogicalClock.Zero;
            var anyDeferred = false;
            foreach (var (start, end) in segments)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var runResult = await ApplyRunSegmentAsync(entries, start, end, cancellationToken)
                    .ConfigureAwait(false);
                if (runResult.Applied)
                {
                    anyApplied = true;
                }
                if (runResult.Deferred)
                {
                    anyDeferred = true;
                }
                if (runResult.HighWaterMark.CompareTo(highest) > 0)
                {
                    highest = runResult.HighWaterMark;
                }
            }
            return new ApplyResult { Applied = anyApplied, HighWaterMark = highest, Deferred = anyDeferred };
        }
        finally
        {
            throttle.Release();
        }
    }

    /// <summary>
    /// Applies a single contiguous <c>(treeId, originClusterId)</c> run
    /// and records the bidirectional inbound per-peer contact, mirroring
    /// the success / failure recording the sequential walk performed
    /// inline. Shared by the sequential and parallel apply paths so the
    /// observability and exception semantics are identical regardless of
    /// the configured degree of parallelism.
    /// </summary>
    private async Task<ApplyResult> ApplyRunSegmentAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        CancellationToken cancellationToken)
    {
        // DURABLE RECEIVE FENCE (issue #1173). Mirror the single-entry gate in
        // ApplyAsync for the batch path: while a restore saga has paused inbound
        // apply for this run's tree, defer the whole run with an explicit
        // Deferred=true signal (HWM unchanged) so no laggard post-cut entries are
        // merged and the receive path returns a not-accepted, cursor-preserving
        // ack that makes the sender re-ship the run after the fence lifts. A run
        // is a single (treeId, originClusterId) segment, so one gate check covers
        // it.
        if (_receiveGate is not null
            && await _receiveGate.IsReceivePausedAsync(entries[startInclusive].TreeId, cancellationToken)
                .ConfigureAwait(false))
        {
            return new ApplyResult
            {
                Applied = false,
                HighWaterMark = HybridLogicalClock.Zero,
                Deferred = true,
            };
        }

        try
        {
            var runResult = await ApplyOriginRunAsync(entries, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
            RecordInboundContact(entries[startInclusive], success: true);
            return runResult;
        }
        catch
        {
            RecordInboundContact(entries[startInclusive], success: false);
            throw;
        }
    }

    /// <summary>
    /// Builds the cross-tree parallel-apply plan for the inbound batch,
    /// or returns <see langword="null"/> when the batch must apply
    /// fully-sequentially. Returns <see langword="null"/> - taking the
    /// allocation-free sequential walk - whenever the batch targets a
    /// single tree (the overwhelmingly common inbound shape, since the
    /// transport ships per-(tree, peer)) or no participating tree
    /// configures
    /// <see cref="LatticeReplicationOptions.ApplyMaxParallelRuns"/>
    /// greater than <c>1</c>.
    /// </summary>
    /// <remarks>
    /// Independence is defined at the tree granularity: the contiguous
    /// <c>(treeId, originClusterId)</c> run segments are grouped by tree
    /// (preserving write-ahead-log order within each tree), and the
    /// effective degree of parallelism is the maximum configured
    /// <see cref="LatticeReplicationOptions.ApplyMaxParallelRuns"/>
    /// across the participating trees, clamped to the number of tree
    /// groups. Same-tree origins remain in one ordered group so the
    /// per-tree causal-apply buffer, shadow-forward dedupe cache, and
    /// per-origin FIFO / high-water-mark invariants are observed exactly
    /// as in the sequential path.
    /// </remarks>
    private ParallelApplyPlan? BuildParallelApplyPlanOrNull(IReadOnlyList<WalRecord> entries)
    {
        // Cheap first pass: bail to the sequential walk the moment the
        // batch is confirmed single-tree. This keeps the steady-state
        // hot path (one tree, one origin) allocation-free.
        var firstTree = entries[0].TreeId;
        var multiTree = false;
        for (var k = 1; k < entries.Count; k++)
        {
            if (!string.Equals(entries[k].TreeId, firstTree, StringComparison.Ordinal))
            {
                multiTree = true;
                break;
            }
        }
        if (!multiTree)
        {
            return null;
        }

        // Multi-tree batch: materialise the contiguous (tree, origin)
        // run segments grouped by tree, preserving WAL order within each
        // tree.
        var groups = new Dictionary<string, List<(int Start, int End)>>(StringComparer.Ordinal);
        var order = new List<string>();
        var i = 0;
        while (i < entries.Count)
        {
            // WalRecord is a wide readonly struct, so every read through the
            // IReadOnlyList<WalRecord> indexer copies the whole record onto the
            // stack. Bind each entry once and project the run-key fields off
            // that single copy instead of indexing three times per candidate.
            var start = entries[i];
            var startTreeId = start.TreeId ?? string.Empty;
            var startOrigin = start.OriginClusterId;
            // Mode is part of the run key here for the same reason as on the
            // sequential path: the gate classifies a run from its first entry,
            // so a heterogeneous-mode run would carry unclassified entries past
            // it. See ApplyRunsSequentiallyAsync.
            var startMode = start.Mode;
            var j = i + 1;
            while (j < entries.Count)
            {
                var candidate = entries[j];
                if (!string.Equals(candidate.TreeId ?? string.Empty, startTreeId, StringComparison.Ordinal)
                    || !string.Equals(candidate.OriginClusterId, startOrigin, StringComparison.Ordinal)
                    || candidate.Mode != startMode)
                {
                    break;
                }
                j++;
            }

            // One hash and one bucket walk per run: the miss branch assigns
            // unconditionally, so the probe-then-store pair folds onto a single
            // slot reference. Legal here because the enclosing method is
            // synchronous - ref locals are illegal in async methods - and safe
            // because nothing mutates `groups` while the ref is live (`order`
            // is a distinct collection).
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(groups, startTreeId, out var existed);
            if (!existed)
            {
                slot = new List<(int Start, int End)>();
                order.Add(startTreeId);
            }
            slot!.Add((i, j));
            i = j;
        }

        // Resolve the effective degree of parallelism: the max
        // configured ApplyMaxParallelRuns across the participating
        // trees, clamped to the number of tree groups. A single group
        // (or no tree opting in) means parallel apply is moot.
        var maxParallel = 1;
        foreach (var treeId in order)
        {
            var configured = options.Get(treeId).ApplyMaxParallelRuns;
            if (configured > maxParallel)
            {
                maxParallel = configured;
            }
        }
        if (maxParallel <= 1 || order.Count <= 1)
        {
            return null;
        }

        return new ParallelApplyPlan(groups, order, Math.Min(maxParallel, order.Count));
    }

    /// <summary>
    /// Immutable plan describing how the inbound batch's runs are
    /// grouped by tree for cross-tree parallel apply. <see cref="Groups"/>
    /// maps each tree id to its ordered run segments;
    /// <see cref="TreeOrder"/> preserves first-seen tree order; and
    /// <see cref="EffectiveDegreeOfParallelism"/> is the bounded
    /// concurrency the parallel apply path uses (configured maximum
    /// clamped to the tree-group count).
    /// </summary>
    private readonly record struct ParallelApplyPlan(
        Dictionary<string, List<(int Start, int End)>> Groups,
        List<string> TreeOrder,
        int EffectiveDegreeOfParallelism);

    /// <summary>
    /// Records an inbound per-peer contact against the bidirectional
    /// <see cref="ReplicationPeerStats"/> using the entry's
    /// <see cref="WalRecord.OriginClusterId"/> as the peer key.
    /// Range-delete entries and any other system-internal records that
    /// carry no <see cref="WalRecord.OriginClusterId"/> are skipped:
    /// inbound contact only makes sense for entries authored by a
    /// remote peer. Local-origin entries (the loopback defence path)
    /// are similarly excluded - they describe a same-cluster mutation
    /// that bounced through the apply pipeline and have no inbound
    /// peer to attribute. The recording is best-effort and never
    /// throws into the apply pipeline.
    /// </summary>
    private void RecordInboundContact(WalRecord representative, bool success)
    {
        if (_peerStats is null)
        {
            return;
        }
        if (string.IsNullOrEmpty(representative.OriginClusterId)
            || string.IsNullOrEmpty(representative.TreeId))
        {
            return;
        }
        var resolved = options.Get(representative.TreeId);
        if (string.Equals(representative.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
        {
            // Local-origin defence path - no inbound peer to attribute.
            return;
        }
        if (success)
        {
            _peerStats.RecordInboundSuccess(representative.TreeId, representative.OriginClusterId!);
        }
        else
        {
            _peerStats.RecordInboundError(representative.TreeId, representative.OriginClusterId!);
        }
    }

    /// <summary>
    /// Applies a contiguous run of entries that share the same
    /// <c>(treeId, originClusterId)</c> tuple. The run is identified
    /// by half-open indices <paramref name="startInclusive"/> and
    /// <paramref name="endExclusive"/>.
    /// </summary>
    /// <remarks>
    /// <para>The per-entry classification is preserved exactly:</para>
    /// <list type="bullet">
    ///   <item><description>Range-delete entries bypass HWM dedup and
    ///   apply unconditionally (they carry <see cref="HybridLogicalClock.Zero"/>
    ///   by design).</description></item>
    ///   <item><description>Point entries are deduped against the
    ///   snapshot-pinned causal floor (single
    ///   <see cref="IReplicationHighWaterMarkGrain.GetPinnedFloorAsync"/>
    ///   read per run); the floor is constant across a run, so no
    ///   in-memory running threshold is maintained. The incrementally
    ///   advanced per-origin diagonal is deliberately NOT a drop
    ///   threshold (per-origin HLC is non-monotonic in WAL-append
    ///   order - #1060).</description></item>
    ///   <item><description>The local vector clock is fetched on
    ///   demand the first time a causal-dep entry is seen, then
    ///   reused until an apply mutates it (a "dirty" flag re-fetches
    ///   on next causal-dep check).</description></item>
    ///   <item><description>The HWM advance is deferred to the end of
    ///   the run (single <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>),
    ///   and the causal-apply buffer is drained once per advanced
    ///   run (single <c>DrainBufferAsync</c>).</description></item>
    /// </list>
    /// <para>Per-entry instrumentation
    /// (<see cref="LatticeReplicationMetrics.ApplyDuration"/>,
    /// <see cref="LatticeReplicationMetrics.ApplyLag"/>,
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>) is
    /// recorded inside the loop so per-entry observability is
    /// preserved.</para>
    /// </remarks>
    private async Task<ApplyResult> ApplyOriginRunAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        CancellationToken cancellationToken)
    {
        var first = entries[startInclusive];
        var treeId = first.TreeId;
        var origin = first.OriginClusterId;

        // Defensive: an empty tree-id or empty origin must surface as
        // the same ArgumentException the per-entry path raises. Falling
        // back to per-entry preserves the exact validation message and
        // keeps the local-origin defence consistent.
        if (string.IsNullOrEmpty(treeId) || string.IsNullOrEmpty(origin))
        {
            return await ApplyRunPerEntryAsync(entries, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
        }

        // RECEIVER-SIDE ENROLLMENT / MERGE-MODE GATE (issue #1267). Mirror the
        // per-entry gate in ApplyAsync for the batch path. A run is a single
        // (treeId, originClusterId, mode) segment - mode is part of the run key,
        // so every entry in the run provably carries the mode the representative
        // first entry classifies, and the classification covers the whole run:
        // reject the run when the tree is not enrolled here, or dead-letter it
        // when the peer-supplied wire mode disagrees with the locally resolved
        // mode. Checked before any HWM grain call so a rejected run costs no
        // round-trip.
        var admission = ClassifyInboundTree(in first);
        if (admission != InboundTreeAdmission.Admit)
        {
            return await RejectRunAsync(entries, startInclusive, endExclusive, admission, cancellationToken)
                .ConfigureAwait(false);
        }

        // RECEIVER-SIDE TENANT-ISOLATION GATE (issue #1633). Mirror the per-entry
        // gate in ApplyAsync for the batch path. A run is a single (treeId, origin)
        // segment, so its one tree id names one owning tenant and a single check
        // covers the whole run. A run whose tree names a non-existent tenant, or a
        // tenant not resident in this serving region, is refused: each entry is
        // dead-lettered (the tree is enrolled and therefore bounded) and the HWM is
        // left unchanged so the sender re-ships and convergence recovers once the
        // tenant exists / becomes resident. Bypassed entirely when tenancy is off
        // (the null gate's IsActive is false), so the batch path is byte-for-byte
        // unchanged. Checked before any HWM grain call so a refused run costs no
        // round-trip.
        if (_tenantIsolationGate is not null && _tenantIsolationGate.IsActive)
        {
            var decision = await _tenantIsolationGate
                .EvaluateAsync(treeId, cancellationToken).ConfigureAwait(false);
            if (decision != ReplicationTenantIsolationDecision.Admit)
            {
                return await RejectTenantIsolationRunAsync(
                    entries, startInclusive, endExclusive, decision, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        var resolved = options.Get(treeId);
        if (string.Equals(origin, resolved.ClusterId, StringComparison.Ordinal))
        {
            // Local-origin defence: the per-entry path classifies each
            // entry as Dedup with HighWaterMark=Zero. Replay the same
            // classification (and per-entry duration sample) here.
            for (var k = startInclusive; k < endExclusive; k++)
            {
                var startTs = Stopwatch.GetTimestamp();
                RecordApplyDuration(treeId, origin!, startTs, LatticeReplicationMetrics.OutcomeDedup);
            }
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        var hwmGrain = GetHwmGrain(treeId);
        var hwm = await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);
        // Snapshot-pinned causal floor for this origin - the sole valid
        // point-write drop threshold (see ApplyAsync for the full
        // rationale). Read once per run: pins never happen mid-run.
        // Zero when no snapshot has been pinned, so nothing is dropped.
        var pinnedFloor = await hwmGrain.GetPinnedFloorAsync(origin!, cancellationToken).ConfigureAwait(false);

        // Bootstrap-drain mode: receiver-side bootstrap replay opens a
        // <see cref="LatticeBootstrapApplyContext"/> scope around the
        // entire drain. While that scope is active the per-origin HWM
        // gate must be suppressed and the end-of-run HWM advance must
        // be skipped, mirroring the per-entry path's bypass at
        // <see cref="ApplyAsync"/>. The snapshot exporter visits
        // shards / leaves in arbitrary order, so applying steady-state
        // HWM dedup during bootstrap replay can drop a still-pending
        // saga key with a strictly-earlier source HLC and break
        // per-saga all-or-nothing visibility on the bootstrapped peer.
        // The post-drain
        // <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
        // installs the HWM at the snapshot's AsOfHlc atomically. The
        // current bootstrap coordinator routes through the per-entry
        // path, so this branch is defence-in-depth for any future
        // drainer that batches; the matching test fixture covers it
        // verbatim.
        var bootstrapMode = LatticeBootstrapApplyContext.IsActive;

        // Per-tree shadow-forward dedupe cache (see ApplyAsync for the
        // race scenario it closes). The cache instance is fetched once
        // per run; per-entry TryAdd is performed after the pinned-floor
        // dedupe so floor-deduped entries do not pollute the cache (which
        // would break operator-driven re-pin recovery, where lowering
        // the per-origin frontier must re-admit previously-deduped
        // identity tuples). On apply failure the reservation is rolled
        // back via Remove so the transport's retry path is not silently
        // suppressed.
        var dedupeCache = _dedupeCaches.GetOrAdd(
            treeId,
            static (_, capacity) => new RecentApplyCache(capacity),
            resolved.ShadowForwardDedupeCacheSize);

        var anyApplied = false;
        var advancedAtAll = false;
        var highestApplied = hwm;

        // Lazy local vector clock: only the first causal-dep entry
        // pays the GetVectorAsync round trip; later entries reuse it
        // until an apply mutates it (which may have moved the local
        // VC), at which point we mark it dirty and re-fetch on the
        // next causal-dep check.
        VersionVector? cachedLocalVc = null;
        var localVcDirty = false;

        // Pending batched LWW Set/Delete items. Items pass classification
        // (not range delete, not dedup'd, not causally parked) and are
        // deferred into a single ApplyMergeManyAsync at end of run rather
        // than issuing one shard RPC per item. State changes
        // (highestApplied, anyApplied, advancedAtAll,
        // localVcDirty) and per-entry instrumentation
        // (ApplyDuration, ApplyLag, FifoState) are deferred until the
        // flush succeeds, mirroring the per-entry path's semantics under
        // partial-batch failure.
        List<ApplyMergeItem>? pendingItems = null;
        List<(int EntryIndex, long StartTs)>? pendingApplies = null;
        IReplicationApplyGrain? applyGrain = null;

        // Capacity hint for the pending buckets below. The run length is an
        // exact upper bound on how many entries either bucket can take, so it
        // removes the whole 4/8/16/.../1024 doubling chain a bucket grown from
        // empty would walk (each doubling allocates a fresh backing array and
        // abandons the previous one). It is clamped so a long run that defers
        // only a handful of entries - most of it deduped or causally parked -
        // cannot over-allocate. The buckets stay lazily constructed, so a run
        // that defers nothing still allocates nothing.
        var pendingCapacityHint = Math.Clamp(
            endExclusive - startInclusive,
            4,
            PendingBatchCapacityHintLimit);

        // Pending batched typed-CRDT delta items. Mirror of pendingItems
        // for non-prepared CRDT-mode Set entries: each passes the same
        // classification gauntlet (HWM dedup, shadow-forward dedup, causal
        // park) and is deferred into a single ApplyCrdtDeltaManyAsync at
        // end of run, which folds every delta inside one grain turn (no
        // per-entry read-merge-write round trip). A tree resolves to a
        // single merge mode, so in practice only one of the two pending
        // buckets is ever populated within a run; the cross-bucket flushes
        // below keep the invariant explicit so the two batches are never
        // reordered relative to each other on a (hypothetical) mixed run.
        List<ApplyCrdtDeltaItem>? pendingCrdtItems = null;
        List<(int EntryIndex, long StartTs)>? pendingCrdtApplies = null;

        async Task FlushPendingCrdtAsync()
        {
            if (pendingCrdtItems is null || pendingCrdtItems.Count == 0)
            {
                return;
            }

            applyGrain ??= grainFactory.GetGrain<IReplicationApplyGrain>(treeId);

            var dispatchItems = pendingCrdtItems;
            var dispatchApplies = pendingCrdtApplies!;
            pendingCrdtItems = null;
            pendingCrdtApplies = null;

            try
            {
                await applyGrain.ApplyCrdtDeltaManyAsync(dispatchItems).ConfigureAwait(false);
            }
            catch
            {
                // Mirror FlushPendingAsync: record OutcomeFailure for each
                // deferred entry and roll back its shadow-forward cache
                // reservation so the transport's retry path readmits it.
                foreach (var (deferredIdx, deferredStartTs) in dispatchApplies)
                {
                    RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                    dedupeCache.Remove(entries[deferredIdx]);
                }
                throw;
            }

            for (var p = 0; p < dispatchApplies.Count; p++)
            {
                var (deferredIdx, deferredStartTs) = dispatchApplies[p];
                var deferredEntry = entries[deferredIdx];
                RecordApplyLag(deferredEntry);
                RecordAppliedContentForIndex(in deferredEntry, resolved);
                if (!bootstrapMode)
                {
                    RecordFifoState(deferredEntry);
                }
                RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeSuccess);

                if (deferredEntry.Timestamp.CompareTo(highestApplied) > 0)
                {
                    highestApplied = deferredEntry.Timestamp;
                }
            }

            anyApplied = true;
            advancedAtAll = true;
            localVcDirty = true;
        }

        async Task FlushPendingAsync()
        {
            if (pendingItems is null || pendingItems.Count == 0)
            {
                return;
            }

            applyGrain ??= grainFactory.GetGrain<IReplicationApplyGrain>(treeId);

            // Hand the list off to the apply call by reference and
            // immediately null the locals - NSubstitute and other mocks
            // capture the reference for late argument matching, so a
            // subsequent .Clear() would mutate the captured snapshot
            // out from under the assertion. Production code paths read
            // the list synchronously inside ApplyMergeManyAsync, so
            // ownership transfer is safe.
            var dispatchItems = pendingItems;
            var dispatchApplies = pendingApplies!;
            pendingItems = null;
            pendingApplies = null;

            try
            {
                await applyGrain.ApplyMergeManyAsync(dispatchItems).ConfigureAwait(false);
            }
            catch
            {
                // Mirror the per-entry path: a throw records OutcomeFailure
                // for each deferred entry and rolls back their
                // shadow-forward cache reservations so the transport's
                // retry path admits them again. Without the rollback the
                // dead-letter decorator's "Applied=false clears the
                // counter" rule would silently drop the entry until
                // FIFO eviction.
                foreach (var (deferredIdx, deferredStartTs) in dispatchApplies)
                {
                    RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                    dedupeCache.Remove(entries[deferredIdx]);
                }
                throw;
            }

            // Flush succeeded: advance state and emit per-entry
            // observability.
            for (var p = 0; p < dispatchApplies.Count; p++)
            {
                var (deferredIdx, deferredStartTs) = dispatchApplies[p];
                var deferredEntry = entries[deferredIdx];
                RecordApplyLag(deferredEntry);
                RecordAppliedContentForIndex(in deferredEntry, resolved);
                if (!bootstrapMode)
                {
                    // Bootstrap drain is intentionally non-monotonic
                    // per (tree, origin) - see the bootstrapMode
                    // comment at run entry - so the steady-state FIFO
                    // regression counter must stay silent.
                    RecordFifoState(deferredEntry);
                }
                RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeSuccess);

                if (deferredEntry.Timestamp.CompareTo(highestApplied) > 0)
                {
                    highestApplied = deferredEntry.Timestamp;
                }
            }

            anyApplied = true;
            advancedAtAll = true;
            localVcDirty = true;
        }

        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entry = entries[k];
            var startTs = Stopwatch.GetTimestamp();
            var outcome = LatticeReplicationMetrics.OutcomeFailure;
            var deferred = false;
            // Tracks whether the current iteration owns a live
            // shadow-forward cache reservation that must be rolled back
            // if an exception escapes. Cleared when ownership transfers
            // (deferral to pendingApplies, successful inline apply) or
            // when the park branch returns normally - in which case the
            // reservation is intentionally retained so duplicate-emit
            // pairs of the parked entry are suppressed while it is
            // buffered.
            var cacheReservedForCurrent = false;
            try
            {
                if (entry.Op == MutationKind.DeleteRange)
                {
                    // Range delete forces the pending LWW batch to flush
                    // first - the producer ordered the WAL such that
                    // entries before the range delete must observe their
                    // effect after, and any deferred LWW work must be
                    // visible before the range walk starts.
                    await FlushPendingAsync().ConfigureAwait(false);
                    await ApplyRangeAsync(entry, cancellationToken).ConfigureAwait(false);
                    InvalidateAppliedContentIndexForRange(in entry, resolved);
                    anyApplied = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                // Saga terminal-mark records (TxCommit / TxAbort) mirror
                // the single-entry ApplyAsync path (see the terminal branch
                // there for the full rationale): they are saga-id-keyed and
                // idempotent on the receiver, so they bypass the per-origin
                // HWM check, the shadow-forward dedup cache, and the
                // causal-park gate below, and route through
                // ApplyTxTerminalCoreAsync - which drives the per-tree
                // TxRegistry mark AND, for a cross-tree atomic write, the
                // receiver-side barrier that flips every participant tree's
                // pending saga keys into the visible projection together.
                // WITHOUT this branch a terminal batched together with its
                // saga's prepared entries (the production shipper coalesces
                // contiguous WAL entries into one inbound batch) falls
                // through to ApplyPointAsync, whose op switch has no
                // TxCommit / TxAbort case and throws "Unsupported
                // point-apply op": the whole batch faults, the terminal is
                // never applied, and the cross-tree receiver barrier never
                // releases, so the saga's keys stay invisible on the peer
                // forever (issue #1525). Any deferred LWW / CRDT batch must
                // flush first so the terminal linearizes after every
                // prepared / plain entry that precedes it in WAL order.
                // Terminals are HWM-neutral (they never advance the
                // per-origin high-water mark), so - like the range-delete
                // branch above - this leaves highestApplied / advancedAtAll
                // untouched.
                if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
                {
                    await FlushPendingAsync().ConfigureAwait(false);
                    await FlushPendingCrdtAsync().ConfigureAwait(false);
                    await ApplyTxTerminalCoreAsync(entry, cancellationToken).ConfigureAwait(false);
                    anyApplied = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                // Phase D1c: saga prepare-phase entries
                // (IsPrepared && AtomicBatchSize > 0) bypass BOTH the
                // pinned-floor dedup AND the causal-park gate below.
                // See ReplicationApplier.ApplyAsync for the full
                // rationale; the same conditions apply on the batched
                // per-entry pass. Compute the flag once and reuse it
                // for both gates.
                var isPreparedAtomicBatch = entry.IsPrepared && entry.AtomicBatchSize > 0;

                if (!bootstrapMode
                    && !isPreparedAtomicBatch
                    && entry.Timestamp.CompareTo(pinnedFloor) <= 0)
                {
                    outcome = LatticeReplicationMetrics.OutcomeDedup;
                    continue;
                }

                // Shadow-forward dedupe cache: suppress the duplicate-emit
                // pair that structural rewrites (split / merge / saga
                // compensate) generate when they shadow-forward a user
                // write into a different shard. See ApplyAsync for the
                // detailed race scenario. The check sits after the
                // pinned-floor dedupe so floor-deduped entries do not
                // pollute the cache.
                if (!dedupeCache.TryAdd(entry))
                {
                    outcome = LatticeReplicationMetrics.OutcomeShadowForwardDedup;
                    continue;
                }
                cacheReservedForCurrent = true;

                // Phase D1c: saga prepare-phase entries
                // (IsPrepared && AtomicBatchSize > 0) bypass the
                // causal-park gate. The producer-side batched saga
                // path (AtomicWriteGrain.ExecutePhaseAsync's parallel
                // cross-leaf SetManyAsync) can stamp prepared writes
                // with VectorClock frontiers whose entries point at
                // sibling per-leaf clocks. Parking those entries
                // produces a chicken-and-egg deadlock: the sibling
                // prepared write that would advance localVc may be
                // parked itself behind the very same VC, and neither
                // drains until the matching terminal arrives - but the
                // terminal is gated on every prepared write applying
                // first. Bypass the park gate; the per-leaf
                // AddPreparedMutation routes the entry into the
                // pending-tx bucket where causal ordering across the
                // saga's keys is irrelevant (the terminal flip is the
                // single atomic-visibility transition). Idempotency on
                // re-delivery is upheld by LwwValue.Merge inside
                // AddPreparedMutation and the per-tx
                // _recentlyTerminal dedup on the terminal mark.
                if (!isPreparedAtomicBatch && HasCausalDependencies(entry))
                {
                    if (cachedLocalVc is null || localVcDirty)
                    {
                        cachedLocalVc = await hwmGrain.GetVectorAsync(cancellationToken).ConfigureAwait(false);
                        localVcDirty = false;
                    }
                    if (!CausalApplyBuffer.DependenciesSatisfied(entry, cachedLocalVc, resolved.ClusterId))
                    {
                        await ParkAsync(entry, resolved, cancellationToken).ConfigureAwait(false);
                        // Park retains the cache reservation (mirroring
                        // ApplyAsync's park branch): the parked entry,
                        // when drained, routes via ApplyPointAsync
                        // directly and bypasses the cache, so the
                        // retained reservation continues to suppress
                        // duplicate-emit pairs of the parked entry that
                        // arrive while it is buffered. Release local
                        // rollback responsibility so the catch below
                        // does not undo the intentional retention.
                        cacheReservedForCurrent = false;
                        outcome = LatticeReplicationMetrics.OutcomeParkedCausalBuffer;
                        continue;
                    }
                }

                // Classify: LWW-register Set/Delete entries batch through
                // ApplyMergeManyAsync; non-prepared typed-CRDT Set entries
                // batch through ApplyCrdtDeltaManyAsync (folded server-side
                // in a single grain turn). All other entries (range
                // deletes, CRDT deletes, prepared/saga entries) stay on the
                // per-entry path.
                //
                // Saga prepare-phase entries (IsPrepared==true) are
                // explicitly excluded from both batched paths: the batched
                // LWW path collapses the per-entry route through
                // ApplyMergeManyAsync, which calls into the shard-root's
                // generic LWW merge primitive without honouring
                // IsPrepared / TransactionId. Routing a prepared
                // record through that primitive applies it directly
                // into the visible projection, bypassing the per-tx
                // pending bucket on the receiver leaf - the same
                // failure mode the producer-side prepare path exists
                // to prevent, manifesting on the wire instead of in
                // memory. Cross-cluster atomic-visibility of a saga
                // collapses to ad-hoc per-key arrival order: keys
                // whose prepares are batched land as visible writes
                // before the terminal arrives, and a receiver reader
                // that scans the batch mid-flight observes a strict
                // subset of the saga's keys. Forcing prepared entries
                // back onto the per-entry path routes them through
                // ApplyPointAsync's IsPrepared branch, which calls
                // ApplyPreparedSetAsync / ApplyPreparedDeleteAsync on
                // the receiver and parks them in the leaf's per-tx
                // pending bucket until the matching terminal arrives.
                var batchable = entry.Mode == LatticeMergeMode.LwwRegister
                    && (entry.Op == MutationKind.Set || entry.Op == MutationKind.Delete)
                    && !entry.IsPrepared;

                // Non-prepared typed-CRDT Set entries carry their post-merge
                // contribution exclusively via Delta and fold idempotently,
                // so a run of them collapses into a single
                // ApplyCrdtDeltaManyAsync. A CRDT Set arriving with a null
                // Delta is a hard wire error; route it to the per-entry path
                // so ApplyPointAsync's typed-delta dispatch raises the same
                // ArgumentException it always has. OrMap is intentionally
                // excluded: its receiver path is the generic-shaped
                // ApplyOrMapDeltaAsync seam (resolved from the host-registered
                // (TKey,TValue) shape), so it stays on its proven per-entry
                // path rather than the closed-shape batch fold.
                var crdtBatchable = entry.Mode != LatticeMergeMode.LwwRegister
                    && entry.Mode != LatticeMergeMode.OrMap
                    && entry.Op == MutationKind.Set
                    && entry.Delta is not null
                    && !entry.IsPrepared;

                if (!batchable && !crdtBatchable)
                {
                    await FlushPendingAsync().ConfigureAwait(false);
                    await FlushPendingCrdtAsync().ConfigureAwait(false);
                    await ApplyPointAsync(entry).ConfigureAwait(false);
                    // Successful apply: clear local rollback
                    // responsibility. The cache reservation is retained
                    // in the steady-state cache (it is the desired
                    // outcome for non-failure paths).
                    cacheReservedForCurrent = false;
                    RecordApplyLag(entry);
                    RecordAppliedContentForIndex(in entry, resolved);
                    if (!bootstrapMode)
                    {
                        // Bootstrap drain suppresses FIFO state tracking
                        // for the same reason the deferred-apply branch
                        // does above.
                        RecordFifoState(entry);
                    }

                    if (entry.Timestamp.CompareTo(highestApplied) > 0)
                    {
                        highestApplied = entry.Timestamp;
                    }
                    anyApplied = true;
                    advancedAtAll = true;
                    localVcDirty = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                if (crdtBatchable)
                {
                    // Defer into the CRDT bucket. Flush any pending LWW
                    // batch first so the two batches are never reordered
                    // relative to each other (a no-op in practice because a
                    // tree resolves to a single merge mode, but it keeps the
                    // ordering invariant explicit).
                    await FlushPendingAsync().ConfigureAwait(false);

                    pendingCrdtItems ??= new List<ApplyCrdtDeltaItem>(pendingCapacityHint);
                    pendingCrdtApplies ??= new List<(int, long)>(pendingCapacityHint);
                    pendingCrdtItems.Add(new ApplyCrdtDeltaItem
                    {
                        Key = entry.Key,
                        Mode = entry.Mode,
                        Delta = entry.Delta!,
                        SourceHlc = entry.Timestamp,
                        OriginClusterId = entry.OriginClusterId!,
                        SourceVectorClock = null,
                        ExpiresAtTicks = entry.ExpiresAtTicks,
                    });
                    pendingCrdtApplies.Add((k, startTs));
                    // Ownership of the cache reservation transfers to
                    // pendingCrdtApplies; FlushPendingCrdtAsync's failure
                    // path rolls it back if the eventual flush throws.
                    cacheReservedForCurrent = false;
                    deferred = true;
                    continue;
                }

                // Batched LWW path. Flush any pending CRDT batch first so
                // the two batches are never reordered (see above).
                await FlushPendingCrdtAsync().ConfigureAwait(false);

                // Validate Set's value-non-null contract
                // here so the ArgumentException surface matches the
                // per-entry path (ApplyPointAsync raises the same).
                if (entry.Op == MutationKind.Set && entry.Value is null)
                {
                    throw new ArgumentException(
                        "WalRecord.Value must be non-null for MutationKind.Set.",
                        nameof(entries));
                }

                pendingItems ??= new List<ApplyMergeItem>(pendingCapacityHint);
                pendingApplies ??= new List<(int, long)>(pendingCapacityHint);
                pendingItems.Add(new ApplyMergeItem
                {
                    Key = entry.Key,
                    Value = entry.Op == MutationKind.Set ? entry.Value : null,
                    SourceHlc = entry.Timestamp,
                    OriginClusterId = entry.OriginClusterId!,
                    SourceVectorClock = null,
                    ExpiresAtTicks = entry.Op == MutationKind.Set ? entry.ExpiresAtTicks : 0,
                    IsTombstone = entry.Op == MutationKind.Delete,
                });
                pendingApplies.Add((k, startTs));
                // Ownership of the cache reservation transfers to
                // pendingApplies; FlushPendingAsync's failure path
                // rolls it back if the eventual flush throws.
                cacheReservedForCurrent = false;
                deferred = true;
            }
            catch
            {
                // Roll back the current iteration's reservation if it
                // was held but neither applied, parked, nor deferred.
                // Hit by ApplyPointAsync / ParkAsync / GetVectorAsync
                // throws and by the contract-violation ArgumentException
                // for batchable Set with null Value.
                if (cacheReservedForCurrent)
                {
                    dedupeCache.Remove(entry);
                }

                // An exception escaping the loop body would otherwise
                // leave any previously-deferred entries with a captured
                // start timestamp but no recorded outcome, producing
                // phantom started-never-completed samples in the apply
                // duration histogram. Record OutcomeFailure for every
                // deferred entry now (the throwing entry's own failure
                // is recorded by the finally below). Cold path only -
                // hit by contract violations (Set with null Value),
                // mid-loop cancellation, and FlushPendingAsync re-throws
                // when the throw originates somewhere other than inside
                // FlushPendingAsync (which nulls pendingApplies before
                // its own await and so leaves this branch a no-op).
                // Each deferred entry's cache reservation is rolled
                // back here for the same dead-letter-retry reason
                // FlushPendingAsync rolls back its own dispatched set.
                if (pendingApplies is { Count: > 0 })
                {
                    foreach (var (deferredIdx, deferredStartTs) in pendingApplies)
                    {
                        RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                        dedupeCache.Remove(entries[deferredIdx]);
                    }
                    pendingItems = null;
                    pendingApplies = null;
                }

                // Same rollback for any deferred CRDT batch (see above).
                if (pendingCrdtApplies is { Count: > 0 })
                {
                    foreach (var (deferredIdx, deferredStartTs) in pendingCrdtApplies)
                    {
                        RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                        dedupeCache.Remove(entries[deferredIdx]);
                    }
                    pendingCrdtItems = null;
                    pendingCrdtApplies = null;
                }
                throw;
            }
            finally
            {
                if (!deferred)
                {
                    RecordApplyDuration(treeId, origin!, startTs, outcome);
                }
            }
        }

        // End-of-run flush of any remaining deferred items.
        await FlushPendingAsync().ConfigureAwait(false);
        await FlushPendingCrdtAsync().ConfigureAwait(false);

        if (advancedAtAll && !bootstrapMode)
        {
            var advanced = await hwmGrain.TryAdvanceAsync(origin!, highestApplied, cancellationToken)
                .ConfigureAwait(false);
            var newHwm = advanced
                ? highestApplied
                : await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);

            if (advanced)
            {
                await DrainBufferAsync(treeId, hwmGrain, resolved, cancellationToken).ConfigureAwait(false);
            }

            return new ApplyResult { Applied = anyApplied, HighWaterMark = newHwm };
        }

        // Bootstrap mode: the per-origin HWM is pinned atomically at
        // the snapshot's AsOfHlc by
        // <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
        // after the drain completes; advancing it mid-drain would
        // suppress still-pending saga keys with strictly-earlier source
        // HLCs. Surface the pre-drain HWM so callers observe the
        // canonical pre-pin frontier.
        return new ApplyResult { Applied = anyApplied, HighWaterMark = hwm };
    }

    /// <summary>
    /// Fallback per-entry walk for runs whose first entry has an empty
    /// tree-id or origin. Routes through <see cref="ApplyAsync"/> so
    /// the per-entry validation guards surface the correct
    /// <see cref="ArgumentException"/> path.
    /// </summary>
    private async Task<ApplyResult> ApplyRunPerEntryAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        CancellationToken cancellationToken)
    {
        var anyApplied = false;
        var highest = HybridLogicalClock.Zero;
        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var r = await ApplyAsync(entries[k], cancellationToken).ConfigureAwait(false);
            if (r.Applied)
            {
                anyApplied = true;
            }
            if (r.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = r.HighWaterMark;
            }
        }
        return new ApplyResult { Applied = anyApplied, HighWaterMark = highest };
    }

    /// <summary>
    /// Terminal handler for a run the receiver-side enrollment / merge-mode
    /// gate rejected (issue #1267). The whole run shares one (treeId, origin,
    /// wire mode), so a single classification covers every entry. A
    /// not-enrolled run is dropped (no dead-letter, since a non-enrolled tree
    /// id is peer-controlled and parking it would let a peer spawn unbounded
    /// DLQ activations); a mode-mismatch run dead-letters each entry with
    /// <see cref="LatticeReplicationMetrics.ReasonModeMismatch"/> because its
    /// tree is enrolled and therefore bounded. Every entry records the matching
    /// apply-duration outcome so per-entry receiver observability is preserved,
    /// and a single warning is logged per run rather than per entry to avoid a
    /// log-flood amplification from a hostile peer. Returns a non-applied,
    /// HWM-unchanged result so the run neither merges nor advances the
    /// per-origin high-water-mark.
    /// </summary>
    private async Task<ApplyResult> RejectRunAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        InboundTreeAdmission admission,
        CancellationToken cancellationToken)
    {
        var first = entries[startInclusive];
        var treeId = first.TreeId;
        var origin = first.OriginClusterId ?? string.Empty;

        if (admission == InboundTreeAdmission.RejectModeMismatch)
        {
            var expectedMode = ResolveLocalMergeMode(treeId, out _)!.Value;
            _logger.LogWarning(
                "Rejected inbound replication run of {Count} entries for tree '{Tree}' from origin '{Origin}': "
                + "wire merge mode '{WireMode}' disagrees with the locally resolved mode '{LocalMode}'.",
                endExclusive - startInclusive, treeId, origin, first.Mode, expectedMode);

            for (var k = startInclusive; k < endExclusive; k++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var startTs = Stopwatch.GetTimestamp();
                await DeadLetterModeMismatchAsync(entries[k], expectedMode, cancellationToken).ConfigureAwait(false);
                RecordApplyDuration(treeId, origin, startTs, LatticeReplicationMetrics.OutcomeRejectedModeMismatch);
            }

            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        if (admission == InboundTreeAdmission.RejectNoEnrollmentSource)
        {
            // Fail closed on ambiguity (issue #1398): mirror the per-entry arm
            // in ApplyAsync. With no enrollment source wired the gate cannot be
            // evaluated, so the whole run is dropped (no dead-letter - the tree
            // id is peer-controlled). Unreachable in production; reachable only
            // by a mis-wired hand-built applier, so warn once.
            if (Interlocked.Exchange(ref _noEnrollmentSourceWarned, 1) == 0)
            {
                _logger.LogWarning(
                    "Dropping inbound replication run for tree '{Tree}' from origin '{Origin}': "
                    + "no replication enrollment source is configured on this receiver "
                    + "(no ILatticeReplicationContext and no ReplicatedTrees map), so the "
                    + "enrollment gate cannot be evaluated. All inbound entries are dropped "
                    + "until a replication context is wired. This warning is logged once.",
                    treeId, origin);
            }

            for (var k = startInclusive; k < endExclusive; k++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var startTs = Stopwatch.GetTimestamp();
                RecordApplyDuration(treeId, origin, startTs, LatticeReplicationMetrics.OutcomeRejectedNotReplicated);
            }

            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        _logger.LogWarning(
            "Rejected inbound replication run of {Count} entries for tree '{Tree}' from origin '{Origin}': "
            + "the tree is not enrolled for replication on this receiver.",
            endExclusive - startInclusive, treeId, origin);

        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var startTs = Stopwatch.GetTimestamp();
            RecordApplyDuration(treeId, origin, startTs, LatticeReplicationMetrics.OutcomeRejectedNotReplicated);
        }

        return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
    }

    /// <summary>
    /// Terminal handler for a run the receiver-side tenant-isolation gate refused
    /// (issue #1633). The whole run shares one (treeId, origin), so its one tree id
    /// names one owning tenant and a single decision covers every entry. Because the
    /// tree is enrolled (and therefore bounded), every entry is dead-lettered with
    /// the matching reason tag - <see cref="LatticeReplicationMetrics.ReasonForeignTenant"/>
    /// for a non-existent tenant or <see cref="LatticeReplicationMetrics.ReasonTenantOffline"/>
    /// for an out-of-region tenant - and records the matching apply-duration outcome,
    /// so per-entry receiver observability is preserved. A single warning is logged
    /// per run rather than per entry to avoid a log-flood amplification from a hostile
    /// peer. Returns a non-applied, HWM-unchanged result so the run neither merges nor
    /// advances the per-origin high-water-mark, and the sender re-ships (converging
    /// once the tenant exists / becomes resident).
    /// </summary>
    private async Task<ApplyResult> RejectTenantIsolationRunAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        ReplicationTenantIsolationDecision decision,
        CancellationToken cancellationToken)
    {
        var first = entries[startInclusive];
        var treeId = first.TreeId;
        var origin = first.OriginClusterId ?? string.Empty;

        var outcome = decision == ReplicationTenantIsolationDecision.RejectOutOfRegion
            ? LatticeReplicationMetrics.OutcomeRejectedTenantOffline
            : LatticeReplicationMetrics.OutcomeRejectedForeignTenant;

        _logger.LogWarning(
            "Rejected inbound replication run of {Count} entries for tree '{Tree}' from origin '{Origin}': "
            + "the tenant-isolation gate refused the write ({Decision}); the run was not applied.",
            endExclusive - startInclusive, treeId, origin, decision);

        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var startTs = Stopwatch.GetTimestamp();
            await DeadLetterTenantIsolationAsync(entries[k], decision, cancellationToken).ConfigureAwait(false);
            RecordApplyDuration(treeId, origin, startTs, outcome);
        }

        return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
    }
}
