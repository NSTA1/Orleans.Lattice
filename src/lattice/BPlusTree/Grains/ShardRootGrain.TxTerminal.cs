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
//   2. Foreground RPC fan-out to every leaf in this shard's chain —
//      best-effort immediate visibility for live leaves so a continuous
//      reader observes the post-saga state without waiting for a
//      replay-coordinator pull. Each leaf's ApplyTxTerminalAsync is
//      idempotent: leaves with no pending bucket under the transaction
//      id no-op, leaves that hold a bucket either flip it into the
//      visible projection (committed=true) or drop it (committed=false).
//      The leaf-side _recentlyTerminal HashSet dedups a terminal that
//      arrives via both channels (RPC then WAL replay, or vice versa).
//
// Shadow-forwarding integration: when this shard is in Draining /
// Drained the terminal-mark call is mirrored in parallel to the
// destination shard via ForwardShadowAsync. The destination's
// AppendTxTerminalAsync runs its own WAL append + RPC fan-out on
// the destination shard's WAL partition.
//
// Replication interaction: the WAL append flows through the standard
// replogship path so receiver clusters observe the terminal mutation
// on their local WAL too — cross-cluster atomic visibility falls out
// for free, with no atomic-batch-detection wire framing.
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

        // Step 1 (durability) — append the terminal mark to this
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
                // Terminals collapse a saga-wide event onto a single
                // shard-level WAL entry; there is no per-key HLC to
                // surface, so we follow the DeleteRange precedent and
                // stamp Zero rather than picking a leaf-side clock.
                Timestamp = HybridLogicalClock.Zero,
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

        // Step 2 (immediate visibility) — fan out the terminal mark
        // to every leaf in this shard's chain in parallel. Each leaf
        // is idempotent (the per-leaf _recentlyTerminal HashSet
        // dedups a terminal that arrives via both channels). The
        // shadow-forward call mirrors the entire AppendTxTerminalAsync
        // shape onto the destination shard, which independently
        // appends its own WAL terminal + fans out on its own chain.
        var leafFanOut = BroadcastTerminalToLeavesAsync(transactionId, committed, cancellationToken);

        var shadowForward = ForwardShadowAsync(
            (transactionId, committed, cancellationToken),
            static (target, state) => target.AppendTxTerminalAsync(state.transactionId, state.committed, state.cancellationToken));

        await Task.WhenAll(leafFanOut, shadowForward);
    }

    /// <summary>
    /// Walks this shard's leaf chain and applies the saga terminal mark
    /// to every leaf in parallel. Each leaf invocation is independent,
    /// so we collect tasks and await once at the end. The chain walk
    /// itself is sequential (each step needs the previous leaf's next
    /// sibling pointer), so the total wall-clock latency is
    /// <c>(chain-length × next-sibling RPC) + max(per-leaf apply RPC)</c>.
    /// For small saga shards this dominates over a hypothetical "track
    /// touched leaves per saga" optimisation, which would require an
    /// extra registration RPC on every prepare write.
    /// </summary>
    private async Task BroadcastTerminalToLeavesAsync(Guid transactionId, bool committed, CancellationToken cancellationToken)
    {
        // Empty tree — no leaves to broadcast to.
        if (state.State.RootNodeId is null)
            return;

        var leftmostId = await GetLeftmostLeafIdAsync();
        if (leftmostId is null)
            return;

        var pending = new List<Task>();
        var currentId = leftmostId.Value;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            pending.Add(leaf.ApplyTxTerminalAsync(transactionId, committed));

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        if (pending.Count > 0)
            await Task.WhenAll(pending);
    }
}
