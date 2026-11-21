using System.Text;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Attribution for a refused batch-write fan-out (issue #5095): the refusal
/// names the shards whose branches had not settled, and the eventual outcome of
/// each abandoned branch is logged once it settles, so a stalled shard (a leaf
/// split waiting on a sibling, a reactivating leaf replaying its WAL) is
/// attributable from the refusal instead of only being visible as a saturation
/// count.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// The most shard indices a refusal message lists before it summarises the
    /// remainder as a count, so a refusal across a very wide fan-out stays a
    /// bounded string.
    /// </summary>
    internal const int MaxOutstandingShardsListed = 16;

    /// <summary>
    /// The minimum interval between two settlement reports from one silo, so a
    /// sustained saturation episode refusing many batches does not flood the log.
    /// </summary>
    private const long SettlementReportIntervalMilliseconds = 10_000;

    /// <summary>
    /// <see cref="Environment.TickCount64"/> at the last settlement report, or
    /// <see langword="long.MinValue"/> before the first.
    /// </summary>
    private static long s_lastSettlementReportTicks = long.MinValue;

    /// <summary>
    /// Describes the shard branches of a multi-shard fan-out that had not
    /// settled when its bound fired, for the refusal message. Runs only on the
    /// refusal path.
    /// </summary>
    /// <param name="buckets">The batch's shard buckets, in dispatch order.</param>
    /// <param name="branches">The per-shard writes, one per bucket, in the same order.</param>
    /// <returns>A description such as <c>2 of 64 shard branch(es) still outstanding: shard(s) 10,41</c>.</returns>
    internal static string DescribeOutstandingFanOutShards<T>(
        ShardFanoutBuckets<T> buckets,
        IReadOnlyList<Task> branches)
    {
        var builder = new StringBuilder();
        var outstanding = 0;
        var i = 0;
        foreach (var (shardIdx, _) in buckets)
        {
            if (i >= branches.Count)
                break;

            if (!branches[i++].IsCompleted)
            {
                if (outstanding < MaxOutstandingShardsListed)
                {
                    if (outstanding > 0)
                        builder.Append(',');
                    builder.Append(shardIdx);
                }

                outstanding++;
            }
        }

        return FormatOutstanding(outstanding, buckets.Count, builder.ToString());
    }

    /// <summary>
    /// Describes the single outstanding branch of a one-shard batch write, for
    /// the refusal message.
    /// </summary>
    /// <param name="shardIdx">The physical shard index the batch was routed to.</param>
    internal static string DescribeOutstandingSingleShard(int shardIdx) =>
        FormatOutstanding(1, 1, shardIdx.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static string FormatOutstanding(int outstanding, int total, string listed)
    {
        if (outstanding == 0)
            return $"0 of {total} shard branch(es) still outstanding";

        var suffix = outstanding > MaxOutstandingShardsListed
            ? $" and {outstanding - MaxOutstandingShardsListed} more"
            : string.Empty;
        return $"{outstanding} of {total} shard branch(es) still outstanding: shard(s) {listed}{suffix}";
    }

    /// <summary>
    /// Logs, once every abandoned branch of a refused multi-shard fan-out has
    /// settled, how each outstanding shard eventually settled. Rate-limited to
    /// one report per silo per interval.
    /// </summary>
    private void ReportAbandonedFanOutSettlement<T>(
        Task all,
        ShardFanoutBuckets<T> buckets,
        IReadOnlyList<Task> branches,
        TimeSpan budget)
    {
        if (!logger.IsEnabled(LogLevel.Warning))
            return;

        var shards = new List<int>();
        var pending = new List<Task>();
        var i = 0;
        foreach (var (shardIdx, _) in buckets)
        {
            if (i >= branches.Count)
                break;

            var branch = branches[i++];
            if (!branch.IsCompleted)
            {
                shards.Add(shardIdx);
                pending.Add(branch);
            }
        }

        ScheduleSettlementReport(all, shards, pending, budget);
    }

    /// <summary>
    /// Logs how the abandoned branch of a refused one-shard batch write
    /// eventually settled. Rate-limited like
    /// <see cref="ReportAbandonedFanOutSettlement{T}"/>.
    /// </summary>
    private void ReportAbandonedSingleShardSettlement(Task single, int shardIdx, TimeSpan budget)
    {
        if (!logger.IsEnabled(LogLevel.Warning))
            return;

        ScheduleSettlementReport(single, [shardIdx], [single], budget);
    }

    private void ScheduleSettlementReport(Task all, List<int> shards, List<Task> pending, TimeSpan budget)
    {
        if (shards.Count == 0)
            return;

        var refusedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        var state = new SettlementReport(logger, TreeId, shards, pending, budget, refusedAt);
        _ = all.ContinueWith(
            static (_, s) => ((SettlementReport)s!).Emit(),
            state,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Claims the per-silo settlement-report slot when the interval has
    /// elapsed since the last claim.
    /// </summary>
    private static bool TryClaimSettlementReportSlot()
    {
        var now = Environment.TickCount64;
        var last = Interlocked.Read(ref s_lastSettlementReportTicks);
        if (last != long.MinValue && now - last < SettlementReportIntervalMilliseconds)
            return false;

        return Interlocked.CompareExchange(ref s_lastSettlementReportTicks, now, last) == last;
    }

    private sealed record SettlementReport(
        ILogger Logger,
        string TreeId,
        List<int> Shards,
        List<Task> Branches,
        TimeSpan Budget,
        long RefusedAt)
    {
        public void Emit()
        {
            if (!TryClaimSettlementReportSlot())
                return;

            var builder = new StringBuilder();
            for (var i = 0; i < Shards.Count; i++)
            {
                if (i > 0)
                    builder.Append("; ");

                var branch = Branches[i];
                builder.Append("shard ").Append(Shards[i]).Append(": ");
                if (branch.IsFaulted)
                {
                    var fault = branch.Exception?.GetBaseException();
                    builder.Append("faulted (").Append(fault?.GetType().Name).Append(": ")
                        .Append(fault?.Message).Append(')');
                }
                else if (branch.IsCanceled)
                {
                    builder.Append("canceled");
                }
                else
                {
                    builder.Append("committed");
                }
            }

            var lateBy = System.Diagnostics.Stopwatch.GetElapsedTime(RefusedAt);
            Logger.LogWarning(
                "Batch write to tree {TreeId} was refused after its fan-out budget {Budget}; the abandoned shard branch(es) settled {LateBy} after the refusal: {Outcomes}. At most one such report is logged per silo every 10 seconds.",
                TreeId,
                Budget,
                lateBy,
                builder.ToString());
        }
    }
}
