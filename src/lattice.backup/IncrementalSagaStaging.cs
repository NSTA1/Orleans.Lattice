using System.Runtime.InteropServices;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The dependency-free rule that keeps an incremental backup saga-consistent
/// (issue #4589). An atomic write's prepare-phase writes reach the WAL as
/// <see cref="LatticeMutation.IsPrepared"/> entries under one
/// <see cref="LatticeMutation.TransactionId"/>, before its per-shard
/// <see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>
/// terminals. An increment that copied them as ordinary data would hold an
/// aborted or undecided batch's writes as committed, or a batch partially.
/// <para>
/// The staging decides each transaction against the capture's decision snapshot
/// (D0, taken under the #4485 decision gate), so an increment resolves a saga on
/// exactly the terms a full capture does:
/// </para>
/// <list type="bullet">
/// <item><description><b>Committed in D0</b>: its prepared writes are emitted, all
/// together, once they cover <see cref="LatticeMutation.AtomicBatchSize"/>. A
/// committed transaction whose prepares the window does not cover - some precede
/// the base's frontier - cannot be held whole by a delta, so the capture falls back
/// to a full backup (<see cref="RequiresFullFallback"/>).</description></item>
/// <item><description><b>Aborted, or Indeterminate</b> (a long-forgotten decision a
/// full capture hides) in D0, or a <see cref="MutationKind.TxAbort"/> in the window:
/// its writes are dropped.</description></item>
/// <item><description><b>Undecided</b> in D0: its writes are dropped, and the
/// frontier the increment records is held back to its earliest entry on each
/// partition (<see cref="HeldOffsets"/>), so the next increment reads it again and
/// resolves it then. <see cref="BlockedFloor"/> is the WAL pin that keeps those
/// entries from being trimmed in between.</description></item>
/// </list>
/// The base hands on the transactions it held as undecided
/// (<see cref="TrackBaseUndecided"/>). Each is looked up in D0 even if none of its
/// records reaches the window: committed, it must be held whole by the window or
/// the capture falls back; still undecided, it is handed on again
/// (<see cref="CarriedUndecided"/>). Without this an increment would omit a saga the
/// base held pre-saga that commits before its terminals reach the WAL.
/// <para>
/// A committed transaction is also held back until every shard terminal it expects
/// has been read, so a terminal that lands after the drain is not met by a later
/// increment that no longer holds its prepares (which would force a full backup).
/// This is the receiver-side staging rule of <c>ViewMaintainerGrain</c> (prepares
/// keyed by <see cref="LatticeMutation.AtomicBatchIndex"/>, completeness by
/// <see cref="LatticeMutation.AtomicBatchSize"/> and
/// <see cref="LatticeMutation.AtomicShardCount"/>, the held-back checkpoint), with
/// the decision taken from D0 rather than from the terminals.
/// </para>
/// </summary>
/// <typeparam name="TEntry">The emitted form of a prepared write, or <see langword="null"/> when the write lies outside the capture's scope.</typeparam>
internal sealed class IncrementalSagaStaging<TEntry>
    where TEntry : class
{
    private readonly Dictionary<Guid, Staged> _staged = new();
    private readonly List<Guid> _unresolved = new();

    /// <summary>
    /// <see langword="true"/> once a committed transaction was found whose prepares
    /// the window does not cover, or whose terminal disagrees with the decision
    /// snapshot. The caller abandons the delta for a full backup.
    /// </summary>
    public bool RequiresFullFallback { get; private set; }

    /// <summary>
    /// Registers the transactions the base capture held pre-saga because they were
    /// undecided at its decision gate, so they are looked up even when none of
    /// their records is in the window.
    /// </summary>
    /// <param name="txIds">The base's undecided transactions; <see langword="null"/> for a base captured before they were recorded.</param>
    public void TrackBaseUndecided(IEnumerable<Guid>? txIds)
    {
        if (txIds is null)
        {
            return;
        }

        foreach (var txId in txIds)
        {
            if (txId == Guid.Empty)
            {
                continue;
            }

            if (!_staged.TryGetValue(txId, out var tx))
            {
                tx = new Staged();
                _staged[txId] = tx;
                _unresolved.Add(txId);
            }

            tx.BaseUndecided = true;
        }
    }

    /// <summary>
    /// The transactions the base held as undecided that are still undecided in D0
    /// and were not settled by an abort in the window: the increment hands them on
    /// to the next one.
    /// </summary>
    public IReadOnlyList<Guid> CarriedUndecided
    {
        get
        {
            var carried = new List<Guid>();
            foreach (var (txId, tx) in _staged)
            {
                if (tx.BaseUndecided && !tx.AbortSeen && tx.Status is null or TxStatus.InFlight)
                {
                    carried.Add(txId);
                }
            }

            return carried;
        }
    }

    /// <summary>
    /// Stages a WAL entry if it belongs to an atomic write's prepare or terminal.
    /// </summary>
    /// <param name="mutation">The surfaced mutation.</param>
    /// <param name="partition">The WAL partition it was read from.</param>
    /// <param name="offset">Its offset in that partition.</param>
    /// <param name="emitted">The entry to emit if the transaction commits; <see langword="null"/> when the key is outside the scope (it still counts towards the batch).</param>
    /// <returns><see langword="true"/> when the entry was a saga entry and must not be emitted as an ordinary write.</returns>
    public bool TryStage(in LatticeMutation mutation, int partition, long offset, TEntry? emitted)
    {
        var isTerminal = mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort;
        if (!isTerminal && !mutation.IsPrepared)
        {
            return false;
        }

        var txId = mutation.TransactionId;
        if (txId == Guid.Empty)
        {
            // A prepared write with no transaction id has no terminal that could
            // ever make it visible, and a terminal with none settles nothing.
            return true;
        }

        if (!_staged.TryGetValue(txId, out var tx))
        {
            tx = new Staged();
            _staged[txId] = tx;
            _unresolved.Add(txId);
        }

        if (mutation.Kind == MutationKind.TxAbort)
        {
            tx.AbortSeen = true;
            tx.Prepares.Clear();
            return true;
        }

        tx.NoteOffset(partition, offset, mutation.Timestamp);

        if (mutation.Kind == MutationKind.TxCommit)
        {
            tx.CommitSeen = true;
            tx.CommittedShards.Add(mutation.ShardIndex);
            if (mutation.AtomicShardCount > tx.ExpectedShardCount)
            {
                tx.ExpectedShardCount = mutation.AtomicShardCount;
            }

            return true;
        }

        if (mutation.AtomicBatchSize > tx.BatchSize)
        {
            tx.BatchSize = mutation.AtomicBatchSize;
        }

        tx.PreparedIndexes.Add(mutation.AtomicBatchIndex);
        if (tx.KeepsEntries)
        {
            // Re-staging the same index (a retried prepare) replaces it, so a
            // replay is idempotent.
            tx.Prepares[mutation.AtomicBatchIndex] = emitted;
        }

        return true;
    }

    /// <summary>
    /// The transactions staged since the last <see cref="ApplyDecisions"/> that
    /// still need their decision looked up in the capture's decision snapshot.
    /// </summary>
    public IReadOnlyList<Guid> TakeUnresolved()
    {
        if (_unresolved.Count == 0)
        {
            return Array.Empty<Guid>();
        }

        var pending = new List<Guid>(_unresolved.Count);
        foreach (var txId in _unresolved)
        {
            if (_staged.TryGetValue(txId, out var tx) && tx.Status is null && !tx.AbortSeen)
            {
                pending.Add(txId);
            }
        }

        _unresolved.Clear();
        return pending;
    }

    /// <summary>
    /// Records the decision snapshot's answer for the looked-up transactions. A
    /// transaction absent from <paramref name="decisions"/> is undecided.
    /// </summary>
    /// <param name="txIds">The transactions that were looked up.</param>
    /// <param name="decisions">The decision snapshot's answers.</param>
    public void ApplyDecisions(IReadOnlyList<Guid> txIds, IReadOnlyDictionary<Guid, TxStatus> decisions)
    {
        ArgumentNullException.ThrowIfNull(txIds);
        ArgumentNullException.ThrowIfNull(decisions);
        foreach (var txId in txIds)
        {
            if (!_staged.TryGetValue(txId, out var tx))
            {
                continue;
            }

            tx.Status = decisions.TryGetValue(txId, out var status) ? status : TxStatus.InFlight;
            if (!tx.KeepsEntries)
            {
                // Only a committed transaction is ever emitted: drop the buffered
                // writes of every other one now, keeping just its offsets.
                tx.Prepares.Clear();
            }
        }
    }

    /// <summary>
    /// Moves the writes of every committed transaction whose prepares now cover its
    /// batch into <paramref name="into"/>, each transaction once and whole.
    /// </summary>
    /// <param name="into">Receives the emitted entries.</param>
    public void DrainCommitted(List<TEntry> into)
    {
        ArgumentNullException.ThrowIfNull(into);
        foreach (var tx in _staged.Values)
        {
            if (tx.Emitted || tx.AbortSeen || tx.Status != TxStatus.Committed || !tx.PreparesComplete)
            {
                continue;
            }

            foreach (var entry in tx.Prepares.Values)
            {
                if (entry is not null)
                {
                    into.Add(entry);
                }
            }

            tx.Emitted = true;
            tx.Prepares.Clear();
        }
    }

    /// <summary>
    /// Settles the window once the drain has caught up: decides whether the delta
    /// must be abandoned for a full backup, and which transactions hold the frontier
    /// back.
    /// </summary>
    public void Finish()
    {
        foreach (var tx in _staged.Values)
        {
            if (tx.AbortSeen)
            {
                continue;
            }

            if (tx.Status == TxStatus.Committed)
            {
                if (!tx.PreparesComplete)
                {
                    // Some of the batch's prepares precede the window: the delta
                    // cannot hold the batch whole.
                    RequiresFullFallback = true;
                }

                continue;
            }

            if (tx.CommitSeen)
            {
                // A commit terminal for a transaction the decision snapshot does not
                // hold as committed. Under the gate this cannot happen (a terminal
                // follows its recorded decision); fail safe rather than guess.
                RequiresFullFallback = true;
            }
        }
    }

    /// <summary>
    /// The per-partition offset the next increment must resume from so that it
    /// reads every held transaction again: the lowest offset of any entry of a
    /// transaction that is undecided, or committed with shard terminals still to
    /// come. Partitions with nothing held are absent.
    /// </summary>
    public IReadOnlyDictionary<int, long> HeldOffsets
    {
        get
        {
            var held = new Dictionary<int, long>();
            foreach (var tx in _staged.Values)
            {
                if (!tx.HoldsFrontier)
                {
                    continue;
                }

                foreach (var (partition, offset) in tx.MinOffsetByPartition)
                {
                    ref var current = ref CollectionsMarshal.GetValueRefOrAddDefault(held, partition, out var existed);
                    if (!existed || offset < current)
                    {
                        current = offset;
                    }
                }
            }

            return held;
        }
    }

    /// <summary>
    /// The lowest timestamp of any held transaction's entry: the WAL blocked-floor
    /// pin that keeps the held entries readable by the next increment.
    /// <see langword="null"/> when nothing is held.
    /// </summary>
    public HybridLogicalClock? BlockedFloor
    {
        get
        {
            HybridLogicalClock? floor = null;
            foreach (var tx in _staged.Values)
            {
                if (tx.HoldsFrontier && tx.HasTimestamp && (floor is null || tx.OldestTimestamp < floor.Value))
                {
                    floor = tx.OldestTimestamp;
                }
            }

            return floor;
        }
    }

    private sealed class Staged
    {
        private bool _hasTimestamp;

        public Dictionary<int, TEntry?> Prepares { get; } = new();

        public HashSet<int> PreparedIndexes { get; } = new();

        public HashSet<int> CommittedShards { get; } = new();

        public Dictionary<int, long> MinOffsetByPartition { get; } = new();

        public HybridLogicalClock OldestTimestamp { get; private set; }

        public int BatchSize { get; set; }

        public int ExpectedShardCount { get; set; }

        public bool CommitSeen { get; set; }

        public bool AbortSeen { get; set; }

        public bool Emitted { get; set; }

        public bool BaseUndecided { get; set; }

        public bool HasTimestamp => _hasTimestamp;

        public TxStatus? Status { get; set; }

        /// <summary>Buffered writes are kept only while the transaction may still be emitted.</summary>
        public bool KeepsEntries => !AbortSeen && !Emitted && (Status is null || Status == TxStatus.Committed);

        public bool PreparesComplete => BatchSize > 0 && PreparedIndexes.Count >= BatchSize;

        private bool TerminalsComplete =>
            CommitSeen && (ExpectedShardCount == 0 || CommittedShards.Count >= ExpectedShardCount);

        public bool HoldsFrontier =>
            !AbortSeen
            && (Status is null or TxStatus.InFlight
                || (Status == TxStatus.Committed && PreparesComplete && !TerminalsComplete));

        public void NoteOffset(int partition, long offset, HybridLogicalClock timestamp)
        {
            ref var current = ref CollectionsMarshal.GetValueRefOrAddDefault(MinOffsetByPartition, partition, out var existed);
            if (!existed || offset < current)
            {
                current = offset;
            }

            if (!_hasTimestamp || timestamp < OldestTimestamp)
            {
                OldestTimestamp = timestamp;
                _hasTimestamp = true;
            }
        }
    }
}
