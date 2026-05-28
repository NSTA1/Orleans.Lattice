using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

// Saga prepare/commit-broadcast terminal-mark primitive.
//
// AppendTxTerminalAsync is the single linearization point for an
// atomic-write saga on a given shard. The saga coordinator (the
// AtomicWriteGrain) calls this exactly once per touched shard after
// every prepare-phase per-key write has routed through the leaf.
//
// Two delivery channels carry the terminal mark, in this order:
//
//   1. WAL append via ICommitLogWriter - the terminal mutation is
//      stamped with Kind=TxCommit/TxAbort and Key=ShardIndex.ToString()
//      and dispatched to the writer adapter, which maps the stamped
//      shard index to a WAL partition (typically by modulo against the
//      configured partition count). When the shard count exceeds the
//      partition count multiple shards collapse to the same partition;
//      receivers dedupe by transaction id, so the apply path is
//      idempotent under repeated terminals from different shards.
//      This is the durability boundary: crash recovery on any leaf in
//      this shard replays the prepare writes and the terminal mark
//      from the same WAL, and the leaf's ILeafProjection.Apply switch
//      flips the per-leaf pending-tx bucket exactly as it would have
//      flipped on the foreground RPC.
//   2. Foreground RPC fan-out to the saga's affected leaves -
//      best-effort immediate visibility for live leaves so a continuous
//      reader observes the post-saga state without waiting for a
//      replay-coordinator pull. The fan-out targets the precise subset
//      of leaves that received a prepare-phase write under this saga
//      on this shard (see ShardRootGrain.TxAffectedLeaves), avoiding
//      the activation-pressure spike that would result from waking
//      every leaf in the chain. Each leaf's ApplyTxTerminalAsync is
//      idempotent: leaves that hold a bucket either flip it into the
//      visible projection (committed=true) or drop it (committed=false);
//      the per-leaf _recentlyTerminal HashSet dedups a terminal that
//      arrives via both channels (RPC then WAL replay, or vice versa).
//      When the per-saga affected-leaves map is missing - the
//      shard-root deactivated between prepare and terminal, or the
//      call arrived via a path that bypasses the routing layer - the
//      code falls back to walking the full chain so correctness is
//      preserved at the cost of the broader fan-out.
//
// Shadow-forwarding integration: when this shard is in Draining /
// Drained the terminal-mark call is mirrored in parallel to the
// destination shard via ForwardShadowAsync. The destination's
// AppendTxTerminalAsync runs its own WAL append + RPC fan-out on
// the destination shard's WAL partition.
//
// Replication interaction: the WAL append flows through the standard
// replogship path so receiver clusters observe the terminal mutation
// on their local WAL too. Cross-cluster atomic visibility relies on
// the terminal-HLC ordering invariant established below: the terminal
// is stamped with an HLC strictly greater than every prepare's stamp
// on this shard's chain (computed by fanning out GetClockAsync over
// the affected-leaves subset and Tick-ing once over the max - for the
// untouched leaves contribute no prepare for this saga, so their
// clocks are irrelevant to the invariant). Receivers merge inbound
// records by HLC across WAL partitions, so this invariant guarantees
// every prepare on a shard is observed before the terminal that
// resolves it - without which a Zero-stamped terminal would always
// sort ahead of non-Zero prepares and flush an empty pending bucket.
internal sealed partial class ShardRootGrain
{
    private bool _commitLogWriterResolved;
    private ICommitLogWriter? _commitLogWriter;

    /// <summary>
    /// Lazily resolves the commit-log writer from the activation's
    /// service provider. Returns <see langword="null"/> when no adapter
    /// has been registered (the single-node / unit-test path) - in
    /// that case the saga relies entirely on the foreground RPC
    /// fan-out for terminal delivery and there is no replay-coordinator
    /// recovery path to seed.
    /// </summary>
    private ICommitLogWriter? ResolveCommitLogWriter()
    {
        if (_commitLogWriterResolved)
            return _commitLogWriter;

        _commitLogWriterResolved = true;
        _commitLogWriter = context.ActivationServices?.GetService<ICommitLogWriter>();
        return _commitLogWriter;
    }

    /// <inheritdoc />
    public async Task<WalRecord?> AppendTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null,
        CancellationToken cancellationToken = default,
        bool inlineWalAppend = true)
    {
        // Pre-flight: refuse if this shard is rejecting (mid Rejecting phase of
        // a tree-rewrite) so the caller can catch StaleTreeRoutingException and
        // retry against the destination tree's shard. PrepareForOperationAsync
        // additionally runs EnsureRootAsync, which is load-bearing for the
        // cross-migration backstop path: when the retroactive prepared-mutation
        // sweep migrates a saga's prepared mutations to a freshly-activated
        // destination shard whose RootNodeId is still null, a terminal arriving
        // with committedValues must initialize the destination's tree before
        // BroadcastTerminalToLeavesAsync runs, or the per-leaf RootNodeId-is-null
        // guard there silently drops every backstop write and the sweep's
        // prepared entries become orphans in _pendingTx - which surface as
        // pre-saga reads once the saga's decision is forgotten by
        // ITxRegistryGrain.
        await PrepareForOperationAsync();

        if (transactionId == Guid.Empty)
            return null;

        cancellationToken.ThrowIfCancellationRequested();

        // c2-xx instrumentation (next-step routing per the c2-xix
        // memo): record the per-shard wall-clock contribution to the
        // saga's broadcast phase. The c2-xvi/xvii saga-side
        // saga.broadcast.duration captures the saga's wall-clock
        // wait on the Task.WhenAll across ~32 shards; this histogram
        // surfaces the per-shard cost so the gap between per-shard
        // and per-leaf timing attributes the non-leaf overhead on
        // the shard (affected-leaves resolution, HLC compute, optional
        // WAL append, scheduler dispatch).
        var shardBroadcastStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        // c2-xxiii: hoisted out of the outer try so it can be returned
        // after the SagaBroadcastShardDuration finally records.
        WalRecord? pendingTerminal = null;
        try
        {
#if LATTICE_DIAG
        var diagTrackedCount = (_affectedLeavesByTx is not null && _affectedLeavesByTx.TryGetValue(transactionId, out var diagAff)) ? diagAff.Count : -1;
        DiagSink.Write($"[DIAG terminal-recv] gid={context.GrainId} shardIdx={ShardIndex} tx={transactionId} committed={committed} trackedAffectedCount={diagTrackedCount} committedValuesCount={committedValues?.Count ?? 0} committedKeys=[{(committedValues is null ? "<null>" : string.Join(",", committedValues.Keys))}]");
#endif

        // Step 1 (target resolution) - prefer the per-saga
        // affected-leaves set recorded during prepare-routed writes
        // (see ShardRootGrain.TxAffectedLeaves). When present, the
        // fan-out targets only the leaves that genuinely hold a
        // pending bucket for this saga, avoiding the activation
        // spike that would result from waking every leaf in the
        // chain. When absent (shard-root deactivated mid-saga, or
        // the routing layer was bypassed), fall back to the full
        // chain walk so correctness is preserved.
        //
        // The cross-migration LWW backstop (committedValues) is
        // applied in step 4 to a UNION of trackedAffected and the
        // set of leaves that own a saga key NOW - leaves outside
        // trackedAffected but holding a saga key are exactly the
        // Bug B exposure surface that the backstop is designed to
        // cover, so the fan-out must reach them too.
        // c2-xxi follow-up: per-stage sub-attribution. Each of the
        // four sub-spans (resolve, hlc, wal, fanout) is recorded on
        // SagaBroadcastShardStageDuration so the c2-xx-measured
        // ~143ms per-shard envelope can be split into its
        // constituents. Tags are constructed once per call (the
        // tree/shard pair is invariant for the activation's lifetime
        // but we still need fresh KeyValuePair instances per Record
        // call to avoid mutating shared structs).
        var stageTagTree = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
        var stageTagShard = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex);

        var resolveStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        HashSet<GrainId>? trackedAffected;
        IReadOnlyList<IBPlusLeafGrain> hlcLeaves;
        try
        {
            trackedAffected = TryConsumeAffectedLeaves(transactionId);
            if (trackedAffected is { Count: > 0 })
            {
                var resolved = new List<IBPlusLeafGrain>(trackedAffected.Count);
                foreach (var id in trackedAffected)
                    resolved.Add(grainFactory.GetGrain<IBPlusLeafGrain>(id));
                hlcLeaves = resolved;
            }
            else
            {
                // Fallback path. The chain walk itself is sequential
                // (each step needs the previous leaf's next-sibling
                // pointer); the subsequent fan-outs (clock collection,
                // terminal apply) parallelise across the collected list.
                hlcLeaves = await CollectChainLeavesAsync(cancellationToken);
            }
        }
        finally
        {
            LatticeMetrics.SagaBroadcastShardStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(resolveStartTicks).TotalMilliseconds,
                stageTagTree, stageTagShard, LatticeMetrics.StageResolveTag);
        }

        // Step 2 (terminal HLC) - fan out GetClockAsync across the
        // target subset and Tick once over the max so the resulting
        // terminal HLC is strictly greater than every prepare's HLC
        // stamped on this shard during this saga. Querying only the
        // affected leaves is sufficient because untouched leaves do
        // not stamp prepares for this saga and therefore cannot
        // contribute to the per-saga max. Load-bearing for
        // cross-cluster atomic visibility: receivers merge inbound
        // WAL records by HLC across partitions, so a Zero-stamped
        // terminal would always sort ahead of non-Zero prepares and
        // flush an empty pending bucket on too-early arrival.
        // Honours LatticeHlcOverrideContext.Current - when set (e.g.
        // a receiver-side relay stamping a source-cluster terminal
        // verbatim) the override wins so the receiver's local record
        // matches the authoring cluster's HLC bit-identically.
        var hlcStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        HybridLogicalClock terminalHlc;
        try
        {
            terminalHlc = await ComputeTerminalHlcAsync(hlcLeaves);
        }
        finally
        {
            LatticeMetrics.SagaBroadcastShardStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(hlcStartTicks).TotalMilliseconds,
                stageTagTree, stageTagShard, LatticeMetrics.StageHlcTag);
        }

        // Step 3 (durability) - append the terminal mark to this
        // shard's WAL partition before any in-memory delivery. The
        // adapter bypasses the FNV key-hash for TxCommit/TxAbort kinds
        // and routes by Key=ShardIndex.ToString(). When no replication
        // adapter is registered (single-node / unit-test path), the
        // writer resolves to null and the WAL append is skipped - the
        // RPC fan-out below remains the only delivery channel for
        // such configurations, which is sufficient because there is
        // no replay-coordinator recovery seam to seed.
        //
        // c2-xxiii lift: when inlineWalAppend is false the shard
        // builds the record but does NOT write it - the caller (the
        // saga coordinator) collects every touched-shard record and
        // dispatches them as one batched ICommitLogWriter.AppendManyAsync
        // call, collapsing N serialised single-entry partition
        // transactions into one per-partition batched transaction.
        // Durability is still saga-awaited; only the dispatcher
        // changes. The pre-stage timer still wraps the work even when
        // the actual AppendAsync is skipped, so the per-shard `wal`
        // histogram drops to ~0 on the saga path and rises only on
        // direct callers (cross-cluster replay, shadow-forward,
        // retroactive sweep) - exactly the attribution surface
        // c2-xxii established.
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            var walStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                var terminal = new WalRecord
                {
                TreeId = TreeId,
                Op = committed ? MutationKind.TxCommit : MutationKind.TxAbort,
                Key = ShardIndex.ToString(CultureInfo.InvariantCulture),
                Timestamp = terminalHlc,
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
                TransactionId = transactionId,
                Category = LatticeMaintenanceContext.Current,
                // IsPrepared is false on terminals - the terminal IS
                // the resolution, not the prepare phase.
                IsPrepared = false,
                // Stamp the authoritative chain-shard index slot in
                // addition to the legacy mutation.Key encoding. The
                // writer continues to route by parsing mutation.Key
                // for back-compat with pre-Option A WAL records, but
                // every other consumer (the activation-time replay
                // filter on the receiving leaf, operator tooling, and
                // cross-cluster receivers) reads the typed slot.
                ShardIndex = ShardIndex,
                // Saga touched-shard count for receiver-side
                // cross-cluster all-or-nothing visibility gating.
                // Stamped from the ambient set by the saga
                // coordinator's MarkOneShardAsync. Defaults to 0 when
                // the ambient is unset - e.g. a unit-test driving
                // AppendTxTerminalAsync directly without going through
                // the saga, or a legacy in-flight path that pre-dates
                // this gate. A 0 falls back to the legacy "mark on
                // first terminal" semantics on the receiver, matching
                // pre-gate behaviour.
                AtomicShardCount = LatticeAtomicShardCountContext.Current ?? 0,
            };

                if (inlineWalAppend)
                {
                    await writer.AppendAsync(terminal, cancellationToken);
                }
                else
                {
                    pendingTerminal = terminal;
                }
            }
            finally
            {
                LatticeMetrics.SagaBroadcastShardStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(walStartTicks).TotalMilliseconds,
                    stageTagTree, stageTagShard, LatticeMetrics.StageWalTag);
            }
        }

        // Step 4 (immediate visibility) - fan out the terminal mark
        // to the resolved target subset in parallel. Each leaf is
        // idempotent (the per-leaf _recentlyTerminal HashSet dedups
        // a terminal that arrives via both channels). The
        // shadow-forward call mirrors the entire AppendTxTerminalAsync
        // shape onto the destination shard, which independently
        // appends its own WAL terminal + fans out on its own chain.
        //
        // ForwardShadowAsync targets state.State.ShadowForward, i.e.
        // the resize / online-merge destination tree (a different
        // physical tree id, same shard index). Active only when this
        // shard is participating in a tree-wide rewrite. The
        // SAME-physical-tree split-forward channel is NOT driven from
        // here - the saga (AtomicWriteGrain.BroadcastTerminalsAsync)
        // and the cross-cluster replication apply path
        // (LatticeGrain.ApplyTxTerminalAsync) pre-resolve the
        // transitive closure of split destinations via
        // TerminalFanOutResolver.ResolveTransitiveAsync and fan the
        // terminal out flat (in parallel) across every shard in the
        // closure. That replaces the previous recursive
        // ForwardSplitTerminalAsync hop on the receiving shard, which
        // unbounded the RPC chain depth under cascading mid-saga
        // splits and tripped Orleans' default response timeout on
        // deep multi-hop reshard chains. The saga walks each shard's
        // GetSplitForwardTargetsAsync, BFS-expanding the seed set
        // until no new destinations are discovered, then fans every
        // shard in the closure in parallel - so cascading splits
        // collapse to a single-hop parallel fan-out at the saga
        // layer.
        var fanOutStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var leafFanOut = BroadcastTerminalToLeavesAsync(
                trackedAffected,
                transactionId,
                committed,
                committedValues,
                cancellationToken);

            var shadowForward = ForwardShadowAsync(
                (transactionId, committed, committedValues, cancellationToken),
                static (target, state) => (Task)target.AppendTxTerminalAsync(
                    state.transactionId, state.committed, state.committedValues, state.cancellationToken));

            await Task.WhenAll(leafFanOut, shadowForward);
        }
        finally
        {
            LatticeMetrics.SagaBroadcastShardStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(fanOutStartTicks).TotalMilliseconds,
                stageTagTree, stageTagShard, LatticeMetrics.StageFanOutTag);
        }
        }
        finally
        {
            LatticeMetrics.SagaBroadcastShardDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(shardBroadcastStartTicks).TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex));
        }

        return pendingTerminal;
    }

    /// <summary>
    /// Walks this shard's leaf chain and collects every leaf grain
    /// reference into a list. The chain walk itself is sequential
    /// (each step needs the previous leaf's next-sibling pointer), so
    /// the wall-clock cost is <c>chain-length × next-sibling RPC</c>.
    /// Returns an empty list when the tree has no root or the
    /// leftmost-leaf lookup returns null.
    /// </summary>
    private async Task<List<IBPlusLeafGrain>> CollectChainLeavesAsync(CancellationToken cancellationToken)
    {
        var leaves = new List<IBPlusLeafGrain>();
        if (state.State.RootNodeId is null)
            return leaves;

        var leftmostId = await GetLeftmostLeafIdAsync();
        if (leftmostId is null)
            return leaves;

        var currentId = leftmostId.Value;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            leaves.Add(leaf);

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return leaves;
    }

    /// <summary>
    /// Computes the terminal-mark HLC for this shard. When
    /// <see cref="LatticeHlcOverrideContext.Current"/> is set (the
    /// receiver-side relay path), the override is returned verbatim so
    /// the receiver's local WAL record matches the authoring cluster's
    /// HLC bit-identically. Otherwise fans out
    /// <see cref="IBPlusLeafGrain.GetClockAsync"/> across the chain in
    /// parallel, takes the max, and ticks once - guaranteeing the
    /// returned HLC is strictly greater than every prepare's stamp on
    /// this shard during the saga. Empty chains return
    /// <c>Tick(Zero)</c> so a non-Zero HLC always lands on the WAL even
    /// when the saga touches no leaves on this shard (e.g. a degenerate
    /// abort path on an empty tree).
    /// </summary>
    private static async Task<HybridLogicalClock> ComputeTerminalHlcAsync(IReadOnlyList<IBPlusLeafGrain> leaves)
    {
        var ovr = LatticeHlcOverrideContext.Current;
        if (ovr is { } sourceHlc)
            return sourceHlc;

        if (leaves.Count == 0)
            return HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var clockTasks = new Task<HybridLogicalClock>[leaves.Count];
        for (var i = 0; i < leaves.Count; i++)
            clockTasks[i] = leaves[i].GetClockAsync();
        var clocks = await Task.WhenAll(clockTasks);

        var max = HybridLogicalClock.Zero;
        foreach (var c in clocks)
        {
            if (c > max)
                max = c;
        }

        return HybridLogicalClock.Tick(max);
    }

    /// <summary>
    /// Fans out the saga terminal mark to the union of (a) every leaf in
    /// <paramref name="trackedAffected"/> (the leaves that received a
    /// prepare-phase write under this saga on this shard) and (b) every
    /// leaf that currently owns a key in <paramref name="committedValues"/>
    /// (the set of saga keys this shard routes to NOW). Each targeted
    /// leaf receives ONLY its own per-key subset of the backstop dict -
    /// the shard root performs the per-key-to-leaf grouping via
    /// <see cref="TraverseToLeafAsync"/> so the leaf does not have to
    /// perform a range-ownership check. Each leaf invocation is
    /// idempotent (the per-leaf <c>_recentlyTerminal</c> HashSet dedups
    /// a terminal arriving via both the foreground RPC and the WAL
    /// replay channels). When both sources are empty, falls back to a
    /// chain walk so a saga that touched no keys on this shard (e.g. an
    /// abort whose prepare never reached us) still propagates the
    /// terminal - historic pre-backstop behaviour.
    /// </summary>
    private async Task BroadcastTerminalToLeavesAsync(
        HashSet<GrainId>? trackedAffected,
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Per-leaf target map. Key = leaf grain id. Value = the subset
        // of committedValues that routes to this leaf, or null when the
        // leaf is reached only via trackedAffected (the pre-backstop
        // pending-flip path).
        Dictionary<GrainId, Dictionary<string, byte[]>?>? leafTargets = null;

        if (trackedAffected is { Count: > 0 })
        {
            leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>(trackedAffected.Count);
            foreach (var id in trackedAffected)
                leafTargets[id] = null;
        }

        // Group committedValues by destination leaf via per-key
        // traversal. This is the same routing helper used by SetAsync,
        // so the resolved leaf is the authoritative current owner of
        // the key on this shard.
        if (committed && committedValues is { Count: > 0 })
        {
            leafTargets ??= new Dictionary<GrainId, Dictionary<string, byte[]>?>();
            foreach (var kvp in committedValues)
            {
                if (state.State.RootNodeId is null)
                    continue;
                var leafId = state.State.RootIsLeaf
                    ? state.State.RootNodeId!.Value
                    : await TraverseToLeafAsync(kvp.Key);
                if (!leafTargets.TryGetValue(leafId, out var bucket) || bucket is null)
                {
                    bucket = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    leafTargets[leafId] = bucket;
                }
                bucket[kvp.Key] = kvp.Value;
            }
        }

        if (leafTargets is null || leafTargets.Count == 0)
        {
            // Fallback path. Historic pre-backstop behaviour: walk the
            // chain so every leaf sees the terminal even when neither
            // the routing layer (trackedAffected) nor a backstop
            // payload (committedValues) was supplied. Empty chains are
            // a no-op.
            var chain = await CollectChainLeavesAsync(cancellationToken);
            if (chain.Count == 0) return;
            var chainTasks = new Task[chain.Count];
            for (var i = 0; i < chain.Count; i++)
                chainTasks[i] = TimedApplyTxTerminalAsync(chain[i], transactionId, committed, null);
            await Task.WhenAll(chainTasks);
            return;
        }

        var pending = new Task[leafTargets.Count];
        var idx = 0;
        foreach (var kvp in leafTargets)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(kvp.Key);
            IReadOnlyDictionary<string, byte[]>? subset = kvp.Value;
            pending[idx++] = TimedApplyTxTerminalAsync(leaf, transactionId, committed, subset);
        }
        await Task.WhenAll(pending);
    }

    /// <summary>
    /// Wraps a single per-leaf <see cref="IBPlusLeafGrain.ApplyTxTerminalAsync"/>
    /// call with timing instrumentation: records the wall-clock
    /// duration on <see cref="LatticeMetrics.SagaBroadcastLeafDuration"/>
    /// tagged with the shard's tree and shard index. The
    /// <c>try/finally</c> ensures the observation fires even on the
    /// failure path; the caller's <see cref="Task.WhenAll(Task[])"/>
    /// still observes the original exception unchanged.
    /// </summary>
    private async Task TimedApplyTxTerminalAsync(
        IBPlusLeafGrain leaf,
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? subset)
    {
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await leaf.ApplyTxTerminalAsync(transactionId, committed, subset);
        }
        finally
        {
            LatticeMetrics.SagaBroadcastLeafDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex));
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Synchronous read of the persisted split state - no WAL append,
    /// no fan-out, no clock-tick. The shard root computes the union of
    /// (a) <see cref="ShardRootState.SplitInProgress"/>.ShadowTargetShardIndex
    /// (when a split is in progress) and (b) every distinct value in
    /// <see cref="ShardRootState.MovedAwaySlots"/>, excluding this
    /// shard's own index. Used by
    /// <see cref="TerminalFanOutResolver.ResolveTransitiveAsync"/> as
    /// the per-shard BFS expansion step.
    /// <para>
    /// <b>Why both windows are required.</b> A saga whose prepare runs
    /// during <see cref="ShardSplitPhase.BeginShadowWrite"/> /
    /// <see cref="ShardSplitPhase.Drain"/> / <see cref="ShardSplitPhase.Swap"/>
    /// shadow-forwards every prepared write to the destination shard,
    /// where the destination's leaf buckets the value into its own
    /// <c>_pendingTx[txid]</c>. If the saga's terminal broadcast then
    /// lands on this source shard *after* the split has progressed to
    /// <see cref="ShardSplitPhase.Reject"/> - or after the split has
    /// fully completed and <see cref="ShardRootState.SplitInProgress"/>
    /// has been cleared - the destination's pending bucket would be
    /// orphaned without an explicit terminal mark. Reporting every
    /// destination this shard has ever migrated slots to via
    /// <see cref="ShardRootState.MovedAwaySlots"/> lets the saga's
    /// flat fan-out reach all of them: each destination either
    /// flushes a real pending bucket into its visible projection
    /// (committed=true) or drops it (committed=false), and destinations
    /// that hold no pending bucket for this saga simply no-op via
    /// their per-leaf <c>_recentlyTerminal</c> dedup.
    /// </para>
    /// </remarks>
    public Task<List<int>> GetSplitForwardTargetsAsync()
    {
        HashSet<int>? targets = null;

        var sip = state.State.SplitInProgress;
        if (sip is not null && sip.ShadowTargetShardIndex != MyShardIndex)
        {
            targets = new HashSet<int> { sip.ShadowTargetShardIndex };
        }

        var moved = state.State.MovedAwaySlots;
        if (moved.Count > 0)
        {
            foreach (var target in moved.Values)
            {
                if (target == MyShardIndex) continue;
                targets ??= new HashSet<int>();
                targets.Add(target);
            }
        }

        if (targets is null || targets.Count == 0)
            return Task.FromResult(new List<int>());

        var list = new List<int>(targets);
        list.Sort();
        return Task.FromResult(list);
    }
}
