using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
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
//   1. WAL append via ICommitLogWriter — the terminal mutation is
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
//   2. Foreground RPC fan-out to the saga's affected leaves —
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
//      When the per-saga affected-leaves map is missing — the
//      shard-root deactivated between prepare and terminal, or the
//      call arrived via a path that bypasses the routing layer — the
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
// the affected-leaves subset and Tick-ing once over the max — for the
// untouched leaves contribute no prepare for this saga, so their
// clocks are irrelevant to the invariant). Receivers merge inbound
// records by HLC across WAL partitions, so this invariant guarantees
// every prepare on a shard is observed before the terminal that
// resolves it — without which a Zero-stamped terminal would always
// sort ahead of non-Zero prepares and flush an empty pending bucket.
internal sealed partial class ShardRootGrain
{
    private bool _commitLogWriterResolved;
    private ICommitLogWriter? _commitLogWriter;

    /// <summary>
    /// Lazily resolves the commit-log writer from the activation's
    /// service provider. Returns <see langword="null"/> when no adapter
    /// has been registered (the single-node / unit-test path) — in
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
    public async Task AppendTxTerminalAsync(Guid transactionId, bool committed, CancellationToken cancellationToken = default)
    {
        // Pre-flight gate: refuse if this shard is rejecting (mid
        // Rejecting phase of a tree-rewrite). The caller will catch
        // StaleTreeRoutingException and retry against the destination
        // tree's shard, which is the correct linearization point now.
        ThrowIfTreeRejecting();

        if (transactionId == Guid.Empty)
            return;

        cancellationToken.ThrowIfCancellationRequested();

        // Step 1 (target resolution) — prefer the per-saga
        // affected-leaves set recorded during prepare-routed writes
        // (see ShardRootGrain.TxAffectedLeaves). When present, the
        // fan-out targets only the leaves that genuinely hold a
        // pending bucket for this saga, avoiding the activation
        // spike that would result from waking every leaf in the
        // chain. When absent (shard-root deactivated mid-saga, or
        // the routing layer was bypassed), fall back to the full
        // chain walk so correctness is preserved.
        var trackedAffected = TryConsumeAffectedLeaves(transactionId);
        IReadOnlyList<IBPlusLeafGrain> targetLeaves;
        if (trackedAffected is { Count: > 0 })
        {
            var resolved = new List<IBPlusLeafGrain>(trackedAffected.Count);
            foreach (var id in trackedAffected)
                resolved.Add(grainFactory.GetGrain<IBPlusLeafGrain>(id));
            targetLeaves = resolved;
        }
        else
        {
            // Fallback path. The chain walk itself is sequential
            // (each step needs the previous leaf's next-sibling
            // pointer); the subsequent fan-outs (clock collection,
            // terminal apply) parallelise across the collected list.
            targetLeaves = await CollectChainLeavesAsync(cancellationToken);
        }

        // Step 2 (terminal HLC) — fan out GetClockAsync across the
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
        // Honours LatticeHlcOverrideContext.Current — when set (e.g.
        // a receiver-side relay stamping a source-cluster terminal
        // verbatim) the override wins so the receiver's local record
        // matches the authoring cluster's HLC bit-identically.
        var terminalHlc = await ComputeTerminalHlcAsync(targetLeaves);

        // Step 3 (durability) — append the terminal mark to this
        // shard's WAL partition before any in-memory delivery. The
        // adapter bypasses the FNV key-hash for TxCommit/TxAbort kinds
        // and routes by Key=ShardIndex.ToString(). When no replication
        // adapter is registered (single-node / unit-test path), the
        // writer resolves to null and the WAL append is skipped — the
        // RPC fan-out below remains the only delivery channel for
        // such configurations, which is sufficient because there is
        // no replay-coordinator recovery seam to seed.
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            var terminal = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = committed ? MutationKind.TxCommit : MutationKind.TxAbort,
                Key = ShardIndex.ToString(CultureInfo.InvariantCulture),
                Timestamp = terminalHlc,
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
                TransactionId = transactionId,
                Category = LatticeMaintenanceContext.Current,
                // IsPrepared is false on terminals — the terminal IS
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
            };

            await writer.AppendAsync(terminal, cancellationToken);
        }

        // Step 4 (immediate visibility) — fan out the terminal mark
        // to the resolved target subset in parallel. Each leaf is
        // idempotent (the per-leaf _recentlyTerminal HashSet dedups
        // a terminal that arrives via both channels). The
        // shadow-forward call mirrors the entire AppendTxTerminalAsync
        // shape onto the destination shard, which independently
        // appends its own WAL terminal + fans out on its own chain.
        var leafFanOut = BroadcastTerminalToLeavesAsync(targetLeaves, transactionId, committed, cancellationToken);

        var shadowForward = ForwardShadowAsync(
            (transactionId, committed, cancellationToken),
            static (target, state) => target.AppendTxTerminalAsync(state.transactionId, state.committed, state.cancellationToken));

        await Task.WhenAll(leafFanOut, shadowForward);
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
    /// parallel, takes the max, and ticks once — guaranteeing the
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
    /// Fans out the saga terminal mark to every leaf in
    /// <paramref name="leaves"/> in parallel and awaits all completions.
    /// Each leaf invocation is idempotent (the per-leaf
    /// <c>_recentlyTerminal</c> HashSet dedups a terminal arriving via
    /// both the foreground RPC and the WAL replay channels). Empty
    /// lists are a no-op.
    /// </summary>
    private static async Task BroadcastTerminalToLeavesAsync(
        IReadOnlyList<IBPlusLeafGrain> leaves,
        Guid transactionId,
        bool committed,
        CancellationToken cancellationToken)
    {
        if (leaves.Count == 0)
            return;

        cancellationToken.ThrowIfCancellationRequested();

        var pending = new Task[leaves.Count];
        for (var i = 0; i < leaves.Count; i++)
            pending[i] = leaves[i].ApplyTxTerminalAsync(transactionId, committed);

        await Task.WhenAll(pending);
    }
}
