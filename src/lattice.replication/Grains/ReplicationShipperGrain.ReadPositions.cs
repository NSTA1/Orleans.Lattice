using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The shipper as an <see cref="IWalOffsetConsumer"/> (issue #4579): it tells
/// the WAL GC, on any silo, the lowest offset per partition it has not durably
/// acknowledged, so no unshipped entry is trimmed however its HLC compares with
/// the cursor the shipper has reported.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is published.</b> The durable <see cref="ReplicationShipperState.PartitionCursors"/>,
/// which a held saga terminal already caps (#4480), never the uncapped
/// in-memory resume point. A position is raised only after the write that made
/// it durable, and lowered as soon as it is lowered in memory (an alias rebind,
/// a rewind), before the next read.
/// </para>
/// <para>
/// <b>Registration.</b> Before the first read of a physical log the shipper
/// registers with that log's <see cref="IWalOffsetConsumerRegistryGrain"/>; a
/// registered shipper that has read nothing publishes position 0 for every
/// partition, so a cold shipper holds the whole log rather than racing the GC.
/// On a rebind it registers with the new log first and only then withdraws from
/// the old one, so there is no window in which neither holds.
/// </para>
/// </remarks>
internal sealed partial class ReplicationShipperGrain : IWalOffsetConsumer
{
    /// <summary>
    /// The published read positions: an immutable snapshot replaced whole, read
    /// by the interleaved <see cref="GetDurableReadPositionsAsync"/>.
    /// <see cref="ReadPositions.WalTreeId"/> is <see langword="null"/> for the
    /// snapshot taken at activation before any bind, which answers for any log.
    /// </summary>
    private volatile ReadPositions? _readPositions;

    /// <summary>The physical log this activation has registered with, or <see langword="null"/>.</summary>
    private string? _registeredReadLog;

    private sealed record ReadPositions(string? WalTreeId, long[] Positions);

    /// <inheritdoc />
    public Task<long[]?> GetDurableReadPositionsAsync(string walTreeId)
    {
        var snapshot = _readPositions;
        if (snapshot is null)
        {
            // An interleaved call that reached the activation before its
            // activation hook published: hold the whole log.
            return Task.FromResult<long[]?>([]);
        }

        if (snapshot.WalTreeId is { } bound && !string.Equals(bound, walTreeId, StringComparison.Ordinal))
        {
            return Task.FromResult<long[]?>(null);
        }

        return Task.FromResult<long[]?>((long[])snapshot.Positions.Clone());
    }

    /// <summary>
    /// Publishes the durable positions loaded at activation, for the persisted
    /// bound log (or for any log when none is persisted yet).
    /// </summary>
    private void PublishActivationReadPositions()
    {
        var bound = state.State.BoundPhysicalTreeId;
        _readPositions = new ReadPositions(
            string.IsNullOrEmpty(bound) ? null : bound,
            CurrentPartitionPositions());
    }

    /// <summary>
    /// Runs before every read: registers with the bound log when this activation
    /// has not, withdrawing from a previous log afterwards, and otherwise lowers
    /// any published position the in-memory cursors have dropped below.
    /// </summary>
    private async Task EnsureReadPositionsPublishedAsync()
    {
        var log = _walTreeId;
        if (!string.Equals(_registeredReadLog, log, StringComparison.Ordinal))
        {
            var previous = _registeredReadLog;
            await _grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(log).RegisterAsync(Context.GrainId);
            _readPositions = new ReadPositions(log, LowerOf(_readPositions, log, CurrentPartitionPositions()));
            _registeredReadLog = log;
            if (previous is not null)
            {
                try
                {
                    await _grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(previous).UnregisterAsync(Context.GrainId);
                }
                catch (Exception ex)
                {
                    // Harmless to leave: this shipper answers null for a log it
                    // no longer reads, so the stale entry holds nothing.
                    Logger.LogWarning(ex,
                        "Withdrawing {Context} from the offset consumers of retired log {Log} failed; it no longer holds that log.",
                        LogContext, previous);
                }
            }

            return;
        }

        var current = _readPositions;
        var lowered = LowerOf(current, log, CurrentPartitionPositions());
        if (current is null || current.WalTreeId != log || !lowered.AsSpan().SequenceEqual(current.Positions))
        {
            _readPositions = new ReadPositions(log, lowered);
        }
    }

    /// <summary>
    /// Raises the published positions to the durable cursors just written. Called
    /// only after a successful <c>WriteStateAsync</c>.
    /// </summary>
    private void PublishDurableReadPositions()
    {
        if (_registeredReadLog is { } log)
        {
            _readPositions = new ReadPositions(log, CurrentPartitionPositions());
        }
    }

    private long[] CurrentPartitionPositions()
    {
        var partitions = Math.Max(1, _optionsMonitor.Get(_treeName).ReplogPartitions);
        var positions = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            positions[p] = state.State.PartitionCursors.TryGetValue(p, out var next) ? Math.Max(0, next) : 0;
        }

        return positions;
    }

    /// <summary>
    /// The element-wise minimum of the published positions and
    /// <paramref name="candidate"/>, when the published ones describe
    /// <paramref name="log"/>; otherwise <paramref name="candidate"/> itself. A
    /// partition one side lacks takes position 0.
    /// </summary>
    private static long[] LowerOf(ReadPositions? published, string log, long[] candidate)
    {
        if (published is null || (published.WalTreeId is { } bound && bound != log))
        {
            return candidate;
        }

        var result = new long[candidate.Length];
        for (var p = 0; p < candidate.Length; p++)
        {
            result[p] = p < published.Positions.Length ? Math.Min(candidate[p], published.Positions[p]) : 0;
        }

        return result;
    }
}
