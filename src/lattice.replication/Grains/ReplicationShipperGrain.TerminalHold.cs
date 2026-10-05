using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Saga terminal hold (issue #4480): a replicated saga terminal is not
/// delivered to the peer until every prepare of its saga that this WAL holds
/// has been acknowledged.
/// <para>
/// A saga's prepares and its per-shard terminals live in different WAL
/// partitions (prepares by key hash, a terminal by shard index), and the k-way
/// merge orders by <see cref="HybridLogicalClock"/>. That order does not deliver
/// a prepare before the terminal that resolves it: partitions are not
/// HLC-monotonic in append order (leaf clocks are independent and skew across
/// silos), a partition read empty mid-tick is not read again that tick, and a
/// pipelined batch that fails is re-shipped after later batches applied. A
/// terminal that overtakes a prepare completes the receiver's tally, the
/// receiver leaf records the saga terminal with no bucket, and the late prepare
/// is refused, so the receiver serves the saga split for good.
/// </para>
/// <para>
/// The merge therefore pulls every terminal it consumes out of the stream into
/// an in-memory hold and keeps draining its partition (no head-of-line block,
/// so holds cannot wait on one another). A hold is released into a later batch
/// once the peer's acknowledged frontier covers every prepare of the saga:
/// precisely, from a per-transaction tally of the prepared records the merge
/// has consumed (every <see cref="WalRecord.AtomicBatchIndex"/> up to
/// <see cref="WalRecord.AtomicBatchSize"/>, with the highest sequence per
/// partition); or, when the tally cannot complete (prepares acknowledged by an
/// earlier activation, unsized legacy prepares, an evicted tally), from a tail
/// barrier - each partition's tail read after the terminal was consumed. The
/// barrier is sound because a prepare's WAL append completes before the saga
/// decides, and the decision precedes every terminal append. Release is gated
/// on acknowledgements rather than on consumption, so a failed batch carrying a
/// prepare cannot be re-shipped behind its terminal.
/// </para>
/// <para>
/// A held terminal's partition cursor is capped at the terminal's sequence, and
/// the reported HLC cursor below its clock, so a crash re-reads it and the WAL
/// GC cannot trim it. The in-memory resume point (<see cref="_ackedNext"/>) is
/// not capped, so the entries after a held terminal are not re-shipped on every
/// tick. Holds are only taken when ordering is not already guaranteed by the
/// stream itself - more than one partition, or a pipelining window above one.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary>
    /// Upper bound on the per-transaction prepare tallies held at once. An
    /// evicted tally only moves its saga's terminals onto the tail-barrier
    /// release path, which is slower but equally sound.
    /// </summary>
    internal const int PrepareTallyCapacity = 1024;

    private readonly List<TerminalHold> _terminalHolds = new();
    private readonly Dictionary<Guid, PrepareTally> _prepareTallies = new();
    private readonly Queue<Guid> _prepareTallyOrder = new();

    /// <summary>
    /// Per partition, the next sequence past every entry the peer has
    /// acknowledged (or the merge consumed and filtered). Unlike
    /// <see cref="ReplicationShipperState.PartitionCursors"/> it is never capped
    /// by a held terminal: it is both the hold release frontier and the
    /// in-memory resume point.
    /// </summary>
    private long[] _ackedNext = Array.Empty<long>();

    /// <summary>Identifier of the batch the latest <see cref="MergeOneBatchAsync"/> call carved.</summary>
    private long _mergeBatchId;

    /// <summary>Whether the current merge takes terminal holds.</summary>
    private bool _holdsEnabled;

    /// <summary>A terminal pulled out of the merge until its saga's prepares are acknowledged.</summary>
    private sealed class TerminalHold
    {
        public required int Partition { get; init; }

        public required long Sequence { get; init; }

        public required byte[] Payload { get; init; }

        public required WalRecord Record { get; init; }

        /// <summary>Per-partition tails read after the terminal was consumed; null until read.</summary>
        public long[]? TailBarrier { get; set; }

        /// <summary>The batch carrying the terminal whose acknowledgement is outstanding, or 0.</summary>
        public long EmittedBatchId { get; set; }

        /// <summary>
        /// Set when the hold was carried across a rebind to a new source log. Its
        /// sequence refers to the retired log, so it neither caps a cursor nor
        /// waits on its own partition: it waits for the new log's tally or tail
        /// barrier instead.
        /// </summary>
        public bool Carried { get; set; }
    }

    /// <summary>The prepared records of one saga the merge has consumed.</summary>
    private sealed class PrepareTally
    {
        private ulong[] _seen;
        private int _seenCount;

        public PrepareTally(int batchSize, int partitions)
        {
            BatchSize = batchSize;
            _seen = new ulong[(batchSize + 63) / 64];
            MaxSequence = new long[partitions];
            Array.Fill(MaxSequence, -1L);
        }

        public int BatchSize { get; private set; }

        /// <summary>Highest consumed prepare sequence per partition, -1 for none.</summary>
        public long[] MaxSequence { get; private set; }

        /// <summary>Set when a prepare carried an index outside its batch, so completeness cannot be judged.</summary>
        public bool Unreliable { get; private set; }

        public bool IsComplete => !Unreliable && _seenCount >= BatchSize;

        public void Record(int batchSize, int index, int partition, long sequence)
        {
            if (batchSize != BatchSize || index < 0 || index >= BatchSize)
            {
                Unreliable = true;
            }
            else
            {
                ref var word = ref _seen[index >> 6];
                var bit = 1UL << (index & 63);
                if ((word & bit) == 0)
                {
                    word |= bit;
                    _seenCount++;
                }
            }

            if (partition >= MaxSequence.Length)
            {
                var grown = new long[partition + 1];
                Array.Fill(grown, -1L);
                MaxSequence.CopyTo(grown, 0);
                MaxSequence = grown;
            }

            if (sequence > MaxSequence[partition])
            {
                MaxSequence[partition] = sequence;
            }
        }
    }

    /// <summary>Number of terminals currently held. Test seam.</summary>
    internal int HeldTerminalCountForTesting => _terminalHolds.Count;

    /// <summary>
    /// Whether ordering a terminal behind its prepares needs a hold: with one
    /// partition and a window of one, the stream already delivers a partition's
    /// entries in append order and applies each batch before the next ships.
    /// </summary>
    private bool TerminalHoldsRequired(LatticeReplicationOptions options) =>
        _partitionCount > 1 || options.ShipMaxInFlight > 1;

    /// <summary>
    /// Brings the uncapped acknowledged frontier up to the durable cursors and
    /// re-arms every hold for the new tick: no batch is in flight across ticks,
    /// so a hold emitted into a batch that never acknowledged ships again.
    /// </summary>
    private void PrepareTerminalHoldsForTick(int partitions)
    {
        if (_ackedNext.Length < partitions)
        {
            Array.Resize(ref _ackedNext, partitions);
        }

        for (var p = 0; p < partitions; p++)
        {
            if (state.State.PartitionCursors.TryGetValue(p, out var saved) && saved > _ackedNext[p])
            {
                _ackedNext[p] = saved;
            }
        }

        foreach (var hold in _terminalHolds)
        {
            hold.EmittedBatchId = 0;
        }
    }

    /// <summary>
    /// Records a consumed prepared record against its saga's tally. Only this
    /// cluster's own prepares are tallied - a foreign-origin prepare belongs to a
    /// saga whose terminals this shipper never ships.
    /// </summary>
    private void TallyPrepare(in WalRecord record, int partition, long sequence, LatticeReplicationOptions options)
    {
        if (record.TransactionId == Guid.Empty
            || !string.Equals(record.OriginClusterId, options.ClusterId, StringComparison.Ordinal))
        {
            return;
        }

        if (!_prepareTallies.TryGetValue(record.TransactionId, out var tally))
        {
            while (_prepareTallies.Count >= PrepareTallyCapacity && _prepareTallyOrder.Count > 0)
            {
                _prepareTallies.Remove(_prepareTallyOrder.Dequeue());
            }

            tally = new PrepareTally(record.AtomicBatchSize, _partitionCount);
            _prepareTallies[record.TransactionId] = tally;
            _prepareTallyOrder.Enqueue(record.TransactionId);
        }

        tally.Record(record.AtomicBatchSize, record.AtomicBatchIndex, partition, sequence);
    }

    /// <summary>Pulls a consumed terminal into a hold, unless it is already held.</summary>
    private void HoldTerminal(int partition, long sequence, byte[] payload, in WalRecord record)
    {
        foreach (var existing in _terminalHolds)
        {
            if (!existing.Carried && existing.Partition == partition && existing.Sequence == sequence)
            {
                return;
            }
        }

        _terminalHolds.Add(new TerminalHold
        {
            Partition = partition,
            Sequence = sequence,
            // The shipping page's bytes are borrowed for the read call only.
            Payload = payload.ToArray(),
            Record = record,
        });
    }

    /// <summary>
    /// Reads every partition's tail once for the holds that need a barrier: no
    /// complete prepare tally, and no barrier yet. The read is issued after the
    /// terminals were consumed, which is all the barrier requires.
    /// </summary>
    private async Task EnsureTailBarriersAsync(CancellationToken cancellationToken)
    {
        var needed = false;
        foreach (var hold in _terminalHolds)
        {
            if (hold.TailBarrier is null && !HasCompleteTally(hold))
            {
                needed = true;
                break;
            }
        }

        if (!needed)
        {
            return;
        }

        var partitions = _partitionCount;
        var reads = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            var grain = _partitionGrainCache[p] ??=
                _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}");
            reads[p] = grain.GetNextSequenceAsync(cancellationToken).AsTask();
        }

        var tails = await Task.WhenAll(reads);
        foreach (var hold in _terminalHolds)
        {
            if (hold.TailBarrier is null && !HasCompleteTally(hold))
            {
                hold.TailBarrier = tails;
            }
        }
    }

    private bool HasCompleteTally(TerminalHold hold) =>
        _prepareTallies.TryGetValue(hold.Record.TransactionId, out var tally) && tally.IsComplete;

    /// <summary>Whether the peer has acknowledged every prepare the hold waits on.</summary>
    private bool IsReleasable(TerminalHold hold)
    {
        // Every entry ahead of the terminal in its own partition. A hold carried
        // across a rebind has no position in the log now being read.
        if (!hold.Carried
            && (hold.Partition >= _ackedNext.Length || _ackedNext[hold.Partition] < hold.Sequence))
        {
            return false;
        }

        if (_prepareTallies.TryGetValue(hold.Record.TransactionId, out var tally) && tally.IsComplete)
        {
            var maxSequence = tally.MaxSequence;
            for (var q = 0; q < maxSequence.Length; q++)
            {
                if (maxSequence[q] >= 0 && (q >= _ackedNext.Length || _ackedNext[q] <= maxSequence[q]))
                {
                    return false;
                }
            }

            return true;
        }

        if (hold.TailBarrier is not { } barrier)
        {
            return false;
        }

        var bound = Math.Min(barrier.Length, _ackedNext.Length);
        for (var q = 0; q < bound; q++)
        {
            if ((hold.Carried || q != hold.Partition) && _ackedNext[q] < barrier[q])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether a hold not yet in an outstanding batch could ship now, or still
    /// needs the tail barrier the next merge reads before it can be judged.
    /// </summary>
    private bool HasReleasableTerminalHold()
    {
        foreach (var hold in _terminalHolds)
        {
            if (hold.EmittedBatchId != 0)
            {
                continue;
            }

            if (IsReleasable(hold)
                || (hold.TailBarrier is null && !HasCompleteTally(hold)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Opens the batch with every releasable hold, ahead of the entries the
    /// merge adds after them. A terminal's position relative to those entries is
    /// immaterial: its prepares are already acknowledged.
    /// </summary>
    private void EmitReleasableTerminalHolds(int maxPerBatch)
    {
        foreach (var hold in _terminalHolds)
        {
            if (_drainBuffer.Count >= maxPerBatch)
            {
                return;
            }

            if (hold.EmittedBatchId != 0 || !IsReleasable(hold))
            {
                continue;
            }

            hold.EmittedBatchId = _mergeBatchId;
            _drainBuffer.Add(hold.Record);
            _drainEncodedSegments.Add(new ArraySegment<byte>(hold.Payload));
            _drainEncodedByteCount += hold.Payload.Length;
        }
    }

    /// <summary>
    /// Retires the holds whose terminals rode in <paramref name="batchId"/>, once
    /// the peer acknowledged it (or it was parked on the dead-letter queue).
    /// Must run before the batch's cursor fold, which then lifts the caps.
    /// </summary>
    private void RetireTerminalHolds(long batchId)
    {
        if (_terminalHolds.Count > 0)
        {
            _terminalHolds.RemoveAll(h => h.EmittedBatchId == batchId);
        }
    }

    /// <summary>The lowest sequence a hold in <paramref name="partition"/> pins its durable cursor at.</summary>
    private long TerminalHoldCursorCap(int partition)
    {
        var cap = long.MaxValue;
        foreach (var hold in _terminalHolds)
        {
            if (!hold.Carried && hold.Partition == partition && hold.Sequence < cap)
            {
                cap = hold.Sequence;
            }
        }

        return cap;
    }

    /// <summary>
    /// Folds an acknowledged per-partition consumed snapshot into the uncapped
    /// frontier, then writes each durable partition cursor as that frontier
    /// capped by the partition's held terminals. Returns whether a durable
    /// cursor moved.
    /// </summary>
    private bool FoldAckedPartitions(long[] maxReadSeq, bool[] advanced)
    {
        if (_ackedNext.Length < _partitionCount)
        {
            Array.Resize(ref _ackedNext, _partitionCount);
        }

        for (var p = 0; p < _partitionCount; p++)
        {
            if (advanced[p] && maxReadSeq[p] + 1 > _ackedNext[p])
            {
                _ackedNext[p] = maxReadSeq[p] + 1;
            }
        }

        var changed = false;
        for (var p = 0; p < _partitionCount; p++)
        {
            var target = _terminalHolds.Count == 0
                ? _ackedNext[p]
                : Math.Min(_ackedNext[p], TerminalHoldCursorCap(p));
            var existing = state.State.PartitionCursors.TryGetValue(p, out var saved) ? saved : 0L;
            if (target <= existing)
            {
                continue;
            }

            state.State.PartitionCursors[p] = target;
            changed = true;
        }

        // Retire the encode-failure quarantine in the same state write as the
        // cursor move past it (#4614).
        if (TryRetireEncodeQuarantine())
        {
            changed = true;
        }

        return changed;
    }

    /// <summary>
    /// Keeps the reported HLC cursor below every held terminal, so the WAL GC's
    /// cursor floor cannot trim a terminal that has not shipped.
    /// </summary>
    private HybridLogicalClock CapCursorBelowHeldTerminals(HybridLogicalClock cursor)
    {
        if (_terminalHolds.Count == 0)
        {
            return cursor;
        }

        var lowest = cursor;
        var capped = false;
        foreach (var hold in _terminalHolds)
        {
            if (hold.Record.Timestamp.CompareTo(lowest) <= 0)
            {
                lowest = hold.Record.Timestamp;
                capped = true;
            }
        }

        if (!capped)
        {
            return cursor;
        }

        if (lowest.Counter > 0)
        {
            return new HybridLogicalClock { WallClockTicks = lowest.WallClockTicks, Counter = lowest.Counter - 1 };
        }

        return lowest.WallClockTicks > 0
            ? new HybridLogicalClock { WallClockTicks = lowest.WallClockTicks - 1, Counter = int.MaxValue }
            : HybridLogicalClock.Zero;
    }

    /// <summary>
    /// The source log was replaced under the shipper (an alias swap): the
    /// cursors restart at the new log's start and the retired log is no longer
    /// read. What happens to a terminal held from the retired log depends on why
    /// the source moved (#4490):
    /// <list type="bullet">
    ///   <item><description>
    ///     After a saga pause (a coordinated restore cutting over to the restored
    ///     copy, the only alias move made while shipping is paused) every hold is
    ///     dropped. Both clusters were reset to the cut: a saga before the cut is
    ///     settled by each side's restored copy, a terminal after it must not
    ///     cross the cut, and every bucket the peer staged from the retired log
    ///     belonged to the copy its own cutover replaced.
    ///   </description></item>
    ///   <item><description>
    ///     Otherwise (an online resize, its undo, a schema remediation, an
    ///     operator alias change) each hold is carried forward by transaction: it
    ///     waits for the new log's prepare tally or tail barrier, so it cannot
    ///     overtake a prepare the new copy mirrored, and it does not strand the
    ///     buckets the peer already staged from the retired log.
    ///   </description></item>
    /// </list>
    /// </summary>
    private void ResetTerminalHoldsForNewSource(bool followsSagaPause)
    {
        if (followsSagaPause)
        {
            if (_terminalHolds.Count > 0)
            {
                Logger.LogInformation(
                    "{Context}: dropping {Count} held saga terminal(s) of the retired source log after a saga pause; "
                    + "both clusters were restored to the cut.",
                    LogContext, _terminalHolds.Count);
                _terminalHolds.Clear();
            }
        }
        else
        {
            foreach (var hold in _terminalHolds)
            {
                hold.Carried = true;
                hold.TailBarrier = null;
                hold.EmittedBatchId = 0;
            }
        }

        Array.Clear(_ackedNext);
        _prepareTallies.Clear();
        _prepareTallyOrder.Clear();
    }
}
