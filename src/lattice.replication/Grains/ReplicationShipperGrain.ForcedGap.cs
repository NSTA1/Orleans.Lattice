using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Forced-gap handling (issue #4534). A write-ahead-log trim under the
/// <see cref="LatticeOptions.WalRetention"/> ceiling can remove records this
/// shipper has not delivered to its peer. The shipper used to skip the trimmed
/// prefix silently. When the lost record was a saga prepare whose terminal is
/// still retained, the terminal then reached the peer without it: the receiver
/// committed the saga and drained the other keys while the lost key had no
/// bucket, a torn saga.
/// <para>
/// A shipping read whose first entry is above the requested sequence is that
/// gap (offsets are dense, and only a trim removes them). On finding one, the
/// shipper durably records the tree's current snapshot export epoch in
/// <see cref="ReplicationShipperState.ReseedRequiredEpoch"/>, before it
/// consumes past the gap, and from then on withholds every saga record -
/// prepares, terminals, and terminals it was holding - from the peer, while
/// plain writes keep shipping. Nothing the peer already holds can tear: a
/// staged bucket with no terminal stays invisible. The marker clears once the
/// peer acknowledges a full bootstrap from an export taken after it
/// (<see cref="ReplicationAck.BootstrapEpoch"/> greater than the recorded
/// epoch); the shipper then re-ships every partition from its lowest retained
/// entry, which, with the export's decision rows, delivers every retained
/// saga whole.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary><see langword="true"/> while the peer must be re-seeded before it may receive saga records.</summary>
    internal bool ReseedRequired => state.State.ReseedRequiredEpoch is not null;

    private static bool IsSagaRecord(in WalRecord record) =>
        record.IsPrepared || record.Op is MutationKind.TxCommit or MutationKind.TxAbort;

    /// <summary>
    /// Takes the peer off the log after a forced gap on <paramref name="partition"/>.
    /// Idempotent while a re-seed is outstanding.
    /// </summary>
    private async Task MarkReseedRequiredAsync(int partition, long requested, long firstRetained)
    {
        if (ReseedRequired)
        {
            return;
        }

        // The replay hold first: every export the re-seed can complete from is
        // taken after the epoch read below, so no saga in flight at it loses its
        // decision while the replay may still read it (#4533).
        var epoch = await TakePeerOffLogStateAsync();

        // A held terminal may belong to a saga that lost a prepare in the gap.
        _terminalHolds.Clear();
        _prepareTallies.Clear();
        _prepareTallyOrder.Clear();
        PurgeSagaRecordsFromDrainBuffer();

        // Durable before the merge consumes past the gap.
        await state.WriteStateAsync();
        ReportReseedState();

        Logger.LogWarning(
            "{Context}: WAL partition {Partition} was trimmed past the unshipped cursor (requested sequence {Requested}, "
            + "first retained {FirstRetained}). Saga records are withheld from the peer until it is re-seeded from a "
            + "snapshot export after epoch {Epoch}.",
            LogContext, partition, requested, firstRetained, epoch);
    }

    /// <summary>
    /// Takes the replay hold, then records the re-seed marker at the tree's
    /// current export epoch and the per-partition point the rewind must still
    /// find retained, in state, without writing it. Returns the epoch.
    /// </summary>
    private async Task<long> TakePeerOffLogStateAsync()
    {
        // The replay hold first: every export the re-seed can complete from is
        // taken after the epoch read below, so no saga in flight at it loses its
        // decision while the replay may still read it (#4533).
        await TakeReplayHoldAsync();
        var epoch = await _grainFactory.GetGrain<IReplicationExportEpochGrain>(_treeName).GetAsync();

        // Every saga record withheld from here on sits at or above the durable
        // cursor and is still retained, so keep it from the GC until the rewind
        // re-ships it (#4533).
        var heads = await ReadRetainedHeadsAsync();
        var retain = new long[heads.Length];
        for (var p = 0; p < heads.Length; p++)
        {
            var cursor = state.State.PartitionCursors.TryGetValue(p, out var c) ? Math.Max(0, c) : 0;
            retain[p] = Math.Max(cursor, heads[p]);
        }

        state.State.ReseedRetainFrom = retain;
        state.State.ReseedRequiredEpoch = epoch;
        state.State.ReseedRequiredSinceUtcTicks = _cursorFlushClock.GetUtcNow().UtcTicks;
        return epoch;
    }

    /// <summary>Each partition's lowest retained sequence (its next sequence when empty).</summary>
    private async Task<long[]> ReadRetainedHeadsAsync()
    {
        var heads = new long[_partitionCount];
        for (var p = 0; p < _partitionCount; p++)
        {
            var grain = _partitionGrainCache[p] ??=
                _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}");
            var head = await grain.ReadShippingAsync(0, 1, CancellationToken.None);
            heads[p] = head.Entries.Count > 0 ? head.Entries[0].Sequence : head.NextSequence;
        }

        return heads;
    }

    // The highest export epoch the peer echoed this tick, by any ack: a push
    // (serial or pipelined) or a liveness probe.
    private long? _reseedEchoThisTick;

    /// <summary>Records the bootstrap epoch an ack echoes, for <see cref="MaybeClearReseedAsync"/>.</summary>
    private void NoteReseedEcho(ReplicationAck ack)
    {
        if (ack.BootstrapEpoch is { } echoed && (_reseedEchoThisTick is not { } seen || echoed > seen))
        {
            _reseedEchoThisTick = echoed;
        }
    }

    /// <summary>
    /// Clears the re-seed marker once the peer acknowledged, by any ack this
    /// tick, a bootstrap from an export taken after it, and rewinds every
    /// partition to its lowest retained entry so every retained saga is
    /// delivered whole. Runs at the end of the pump tick, after every batch
    /// has folded its cursors, so no fold raises a rewound partition again.
    /// </summary>
    private async Task MaybeClearReseedAsync()
    {
        var echo = _reseedEchoThisTick;
        _reseedEchoThisTick = null;
        if (echo is not { } echoedThisTick)
        {
            return;
        }

        var ack = new ReplicationAck { Accepted = true, BootstrapEpoch = echoedThisTick };
        if (state.State.ReseedRequiredEpoch is not { } marker
            || ack.BootstrapEpoch is not { } echoed
            || echoed <= marker)
        {
            return;
        }

        // The replay that follows the rewind is exact only while every
        // registry honours the replay's purge hold (#4533): stay off the log,
        // withholding saga records, until no silo predates it.
        if (!AllSilosHonourPurgeHolds())
        {
            LogPurgeHoldUnsupported();
            return;
        }

        // Hold every partition from offset 0 while the rewind reads each head,
        // so no GC pass on any silo trims between the read and the rewind
        // (issue #4579): the rewound positions are below the published ones.
        HoldPublishedReadPositionsAtZero();
        var heads = await ReadRetainedHeadsAsync();

        // The retention ceiling trimmed records this shipper withheld since it
        // took the peer off the log: the echoed export may predate their saga's
        // decision, so the rewind could not deliver it whole. Wait for an export
        // taken after this point instead.
        if (state.State.ReseedRetainFrom is { } retainFrom)
        {
            for (var p = 0; p < heads.Length; p++)
            {
                if (p < retainFrom.Length && heads[p] > retainFrom[p])
                {
                    var renewed = await TakePeerOffLogStateAsync();
                    await state.WriteStateAsync();
                    PublishDurableReadPositions();
                    ReportReseedState();
                    Logger.LogWarning(
                        "{Context}: WAL partition {Partition} was trimmed past saga records withheld for the re-seed; "
                        + "the peer must be re-seeded again, from a snapshot export after epoch {Epoch}.",
                        LogContext, p, renewed);
                    return;
                }
            }
        }

        for (var p = 0; p < _partitionCount; p++)
        {
            var lowest = heads[p];
            if (!state.State.PartitionCursors.TryGetValue(p, out var cursor) || cursor > lowest)
            {
                state.State.PartitionCursors[p] = lowest;
            }
        }

        // Every rewound position is at or below its partition's lowest retained
        // entry, so publishing it before the write releases nothing retained.
        PublishDurableReadPositions();
        Array.Clear(_ackedNext);
        state.State.ReseedRequiredEpoch = null;
        state.State.ReseedRetainFrom = null;
        // Re-shipping from the lowest retained entry is a replay whose
        // snapshot carries any saga it withholds (#4533).
        await BeginReplayFilterAsync(_partitionCount, carried: true);
        state.State.ReseedRequiredSinceUtcTicks = 0;
        // The re-seed settled what the applied low watermark was clamped on (#4586).
        await OnReseedRewoundForFrontierAsync(echoed);
        await state.WriteStateAsync();
        ReportReseedState();

        Logger.LogInformation(
            "{Context}: the peer completed a bootstrap from export epoch {Echoed} (marker {Marker}); saga records "
            + "resume and every partition re-ships from its lowest retained entry.",
            LogContext, echoed, marker);
    }

    /// <summary>
    /// Publishes the re-seed state to the peer-status read path, where a peer
    /// awaiting a re-seed classifies as stalled.
    /// </summary>
    private void ReportReseedState() =>
        _peerStats.RecordReseedRequired(
            _treeName,
            _peerClusterId,
            ReseedRequired ? new DateTimeOffset(state.State.ReseedRequiredSinceUtcTicks, TimeSpan.Zero) : null);

    /// <summary>Removes every saga record staged in the current batch.</summary>
    private void PurgeSagaRecordsFromDrainBuffer()
    {
        var write = 0;
        for (var read = 0; read < _drainBuffer.Count; read++)
        {
            var record = _drainBuffer[read];
            if (IsSagaRecord(in record))
            {
                _drainEncodedByteCount -= _drainEncodedSegments[read].Count;
                continue;
            }

            _drainBuffer[write] = record;
            _drainEncodedSegments[write] = _drainEncodedSegments[read];
            write++;
        }

        _drainBuffer.RemoveRange(write, _drainBuffer.Count - write);
        _drainEncodedSegments.RemoveRange(write, _drainEncodedSegments.Count - write);
    }
}
