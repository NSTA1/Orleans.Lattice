using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The coalesced pin mirror (issue #6166). Every resident leaf mirrors its durable pin to
/// one of a tree's few pin shards after its checkpoints, and each mirror was its own grain
/// call: one exclusive turn on a non-reentrant shard per report. At scale those turns grew
/// with the tree, and a birth seed - which a bulk append awaits - queued behind them.
/// </summary>
/// <remarks>
/// <para>
/// A silo now keeps at most one mirror call in flight per pin shard. Reports for that shard
/// that arrive meanwhile wait in a per-consumer, latest-wins map and go together in the next
/// call, so the shard sees one turn per round trip per silo rather than one per report.
/// </para>
/// <para>
/// Nothing about a report's meaning changes. It is still a best-effort, monotone-max merge
/// that only ever leaves the durable pin lower than the leaf's true frontier, which retains
/// more WAL and is always safe. The debounce rollback is applied per report on a shed or
/// fault exactly as before, and every queued report is also held pending, so the silo-stop
/// flush writes one still waiting here (issue #3509).
/// </para>
/// <para>
/// Memory: one map entry per consumer with a report waiting, per shard with a call in
/// flight; the maps are drained by each call and reused.
/// </para>
/// </remarks>
internal sealed partial class LeafCursorReporter
{
    /// <summary>The most reports one mirror call carries; a larger backlog goes in several.</summary>
    internal const int MaxMirrorReportsPerCall = 256;

    /// <summary>Per pin-shard grain key: the reports waiting behind that shard's call in flight.</summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, MirrorQueue> _mirrorQueues =
        new(StringComparer.Ordinal);

    /// <summary>A pin shard's waiting mirror reports and whether a call to it is in flight.</summary>
    private sealed class MirrorQueue(string treeName)
    {
        public readonly string TreeName = treeName;

        public readonly object Gate = new();

        /// <summary>The waiting reports, latest-wins per consumer on both axes.</summary>
        public Dictionary<string, MaterialiserPinReport> Waiting = new(StringComparer.Ordinal);

        /// <summary>The map the call in flight drained, reused as the next waiting map.</summary>
        public Dictionary<string, MaterialiserPinReport> Spare = new(StringComparer.Ordinal);

        public bool Draining;
    }

    /// <summary>
    /// Queues <paramref name="report"/> for its pin shard and starts that shard's call when
    /// none is in flight. Never awaits; the call runs fire-and-forget as the mirror always has.
    /// </summary>
    private void EnqueueMirror(string treeName, MaterialiserPinReport report)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(options);
        var grainKey = WalMaterialiserPinRouting.ShardKey(treeName, report.ConsumerId, shardCount);
        var queue = _mirrorQueues.GetOrAdd(grainKey, static (_, tree) => new MirrorQueue(tree), treeName);
        bool start;
        lock (queue.Gate)
        {
            var waiting = queue.Waiting;
            if (waiting.TryGetValue(report.ConsumerId, out var queued))
            {
                report = new MaterialiserPinReport(
                    report.ConsumerId,
                    report.Frontier > queued.Frontier ? report.Frontier : queued.Frontier,
                    Math.Max(report.CheckpointOffset, queued.CheckpointOffset));
            }

            waiting[report.ConsumerId] = report;
            start = !queue.Draining;
            queue.Draining = true;
        }

        if (start)
        {
            _ = DrainMirrorAsync(grainKey, queue);
        }
    }

    /// <summary>
    /// Sends <paramref name="queue"/>'s waiting reports to its shard, one call at a time,
    /// until none are waiting. Never throws.
    /// </summary>
    private async Task DrainMirrorAsync(string grainKey, MirrorQueue queue)
    {
        while (true)
        {
            Dictionary<string, MaterialiserPinReport> batch;
            lock (queue.Gate)
            {
                if (queue.Waiting.Count == 0)
                {
                    queue.Draining = false;
                    return;
                }

                batch = queue.Waiting;
                queue.Waiting = queue.Spare;
                queue.Spare = batch;
            }

            var all = new MaterialiserPinReport[batch.Count];
            batch.Values.CopyTo(all, 0);
            batch.Clear();
            for (var start = 0; start < all.Length; start += MaxMirrorReportsPerCall)
            {
                await SendMirrorAsync(grainKey, queue.TreeName, new ArraySegment<MaterialiserPinReport>(
                    all, start, Math.Min(MaxMirrorReportsPerCall, all.Length - start))).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Sends one mirror call and settles each report's debounce and pending state. Never throws.</summary>
    private async Task SendMirrorAsync(string grainKey, string treeName, ArraySegment<MaterialiserPinReport> reports)
    {
        if (AwaitDiagnostics.Enabled)
        {
            AwaitDiagnostics.Count("wal.pin.mirror.call");
            AwaitDiagnostics.Add("wal.pin.mirror.call.reports", reports.Count);
        }

        bool written;
        Exception? fault = null;
        try
        {
            written = await SeedShardAsync(grainKey, reports, writeThrough: false, sheddable: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            written = false;
            fault = ex;
        }

        for (var i = 0; i < reports.Count; i++)
        {
            var key = (treeName, reports[i].ConsumerId);
            if (written)
            {
                ClearPendingIfCovered(key, reports[i].Frontier, reports[i].CheckpointOffset);
            }
            else
            {
                // Shed or faulted, not written: roll the debounce back so the next note
                // re-attempts, and keep the value pending for the silo-stop flush.
                _durableDebounce.TryRemove(key, out _);
                NotePending(key, reports[i].Frontier, reports[i].CheckpointOffset);
            }
        }

        if (fault is not null)
        {
            logger?.LogWarning(
                fault,
                "Failed to persist durable WAL materialiser pin(s) ({Count}) for tree {TreeId} to {PinShard}; they are retried on the next checkpoint.",
                reports.Count,
                treeName,
                grainKey);
        }
    }
}
