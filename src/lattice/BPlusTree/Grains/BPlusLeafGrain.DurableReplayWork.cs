using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable unresolved-replay-work partial for <see cref="BPlusLeafGrain"/>
/// (issue #2165).
/// <para>
/// Activation-time replay defers two classes of record and, because neither
/// survives a teardown, used to clamp the incremental flush ceiling below them
/// for the whole of the activation:
/// </para>
/// <list type="number">
/// <item>An <b>unresolved saga prepare</b>. Applying it populates the
/// activation-scoped <c>_pendingTx</c> bucket, which no snapshot captures, so
/// advancing the checkpoint past it would lose the prepared write when its
/// terminal arrived in a later activation.</item>
/// <item>An <b>undrained deferred terminal</b> (<c>TxCommit</c> /
/// <c>TxAbort</c> / <c>DeleteRange</c>) on a partition that pass 1 does not
/// absorb last. Its mutation is genuinely unapplied until pass 2.</item>
/// </list>
/// <para>
/// Issue #2089 narrowed the resulting livelock by awarding the single pass-1
/// drain slot to the partition with the largest backlog, and said in terms
/// what it had left behind: "This NARROWS the livelock, it does not remove it
/// - the other N-1 partitions still cannot drain in pass 1. Removing it needs
/// a durable record of unresolved deferred work so a resumed replay need not
/// re-read it." This file is that record.
/// </para>
/// <para>
/// The mechanism is deliberately small. Every deferred record is written
/// verbatim into <see cref="LeafNodeState.UnresolvedReplayWork"/> before the
/// flush that advances past it, so the two land in the SAME
/// <c>WriteStateAsync</c> - the checkpoint can never become durable without
/// the record that licenses it, and the pair can never be torn. A resumed
/// activation replays the ledger back into <c>_pendingTx</c> and the pass-2
/// deferred list before it reads a single WAL slice, so the work is
/// reconstructed rather than re-read, and the partition banks forward progress
/// on every activation instead of recomputing the identical pin.
/// </para>
/// <para>
/// Nothing here widens the pass-1 drain slot. The cross-partition dependency
/// argument that keeps that slot at one - a terminal in partition P may have
/// prepares in an unabsorbed partition Q, and a range delete in P may target
/// Sets in Q - is untouched and still holds: deferred work is still applied in
/// pass 2, and only in pass 2. What changes is that the ceiling no longer has
/// to wait for pass 2 to be REACHED before it can move.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Membership mirror of <see cref="LeafNodeState.UnresolvedReplayWork"/>,
    /// keyed by (partition, offset). The clamp sites consult it once per
    /// candidate offset, so the list is never scanned linearly on the flush
    /// path. Rebuilt from the persisted list on first use per activation and
    /// kept in step by every mutating helper below.
    /// </summary>
    private HashSet<(int Partition, long Offset)>? _durableReplayWorkIndex;

    /// <summary>
    /// Rebuilds <see cref="_durableReplayWorkIndex"/> from the persisted list.
    /// Cheap and idempotent; the list is bounded by
    /// <c>LatticeOptions.MaxDurableUnresolvedReplayWork</c> and is empty in the
    /// steady state.
    /// </summary>
    private HashSet<(int Partition, long Offset)> DurableReplayWorkIndex()
    {
        if (_durableReplayWorkIndex is not null)
            return _durableReplayWorkIndex;

        var index = new HashSet<(int, long)>();
        var work = state.State.UnresolvedReplayWork;
        if (work is not null)
        {
            foreach (var entry in work)
                index.Add((entry.Partition, entry.Offset));
        }
        return _durableReplayWorkIndex = index;
    }

    /// <summary>
    /// Reports whether (<paramref name="partition"/>,
    /// <paramref name="offset"/>) is durably recorded, and therefore whether
    /// the flush ceiling may advance past it. Consulted by both clamp sites -
    /// <c>TryFlushRecoveredCeilingAsync</c>'s deferred-offset bound and
    /// <see cref="MinUnresolvedPrepareOffsetForPartition"/>'s prepare bound -
    /// so the two can never disagree about which offsets are covered.
    /// </summary>
    internal bool IsUnresolvedReplayWorkRecorded(int partition, long offset) =>
        _durableReplayWorkIndex is null
            ? state.State.UnresolvedReplayWork is { Count: > 0 } && DurableReplayWorkIndex().Contains((partition, offset))
            : _durableReplayWorkIndex.Contains((partition, offset));

    /// <summary>
    /// Records one piece of unresolved replay work durably, returning
    /// <see langword="true"/> when the caller may therefore let the flush
    /// ceiling advance past <paramref name="offset"/>.
    /// <para>
    /// Returns <see langword="false"/> once the ledger reaches
    /// <paramref name="cap"/>. That is a deliberate safe degradation rather
    /// than an error: the caller then clamps exactly as it did before this
    /// change, which is slow but has shipped in every previous release. The
    /// cap exists only for the pathological case of sagas whose terminals
    /// never arrive, which would otherwise grow the persisted leaf row without
    /// bound.
    /// </para>
    /// <para>
    /// The write is to the in-memory state row only. It becomes durable when
    /// the checkpoint flush that it licenses persists the row, which is what
    /// makes the pair atomic: there is no window in which the checkpoint is
    /// durable and the record is not.
    /// </para>
    /// </summary>
    private bool TryRecordUnresolvedReplayWork(int partition, long offset, in LatticeMutation mutation, int cap)
    {
        if (cap <= 0)
            return false;

        var index = DurableReplayWorkIndex();
        if (index.Contains((partition, offset)))
            return true;

        var work = state.State.UnresolvedReplayWork ??= [];
        if (work.Count >= cap)
            return false;

        work.Add(new UnresolvedReplayWorkEntry(partition, offset, mutation));
        index.Add((partition, offset));
        return true;
    }

    /// <summary>
    /// Test seam for the issue #2183 regression control arm. Production always
    /// records a resident unresolved prepare unconditionally (see
    /// <see cref="EnsureUnresolvedPrepareRecorded"/>); flipping this to
    /// <see langword="false"/> reproduces the pre-fix behaviour - the prepare
    /// is recorded through the capped path and DROPPED once the ledger is full,
    /// which pins the ceiling permanently - on the same build, so the two arms
    /// differ only in the fix and not in configuration.
    /// </summary>
    internal bool RecordUnresolvedPreparesBeyondCap { get; set; } = true;

    /// <summary>
    /// Records a resident unresolved saga prepare durably, ALWAYS, bypassing
    /// the <c>MaxDurableUnresolvedReplayWork</c> cap that bounds deferred
    /// terminals (issue #2183).
    /// <para>
    /// The cap is a safe bound for a deferred TERMINAL: dropping one at the cap
    /// falls back to the in-memory clamp, and pass 2 still drains it, so the
    /// clamp is transient. It is NOT a safe bound for an unresolved PREPARE:
    /// nothing drains a prepare whose saga never terminates, so a dropped
    /// prepare pins the flush ceiling at (prepare - 1) forever and the leaf
    /// banks no durable forward progress at all - the livelock issue #2183
    /// reproduces once a long-lived leaf's ledger has saturated with
    /// never-resolving orphan prepares. Dropping a prepare is also unsafe for
    /// the aged-out-commit reason #2190 documents: a prepare whose commit
    /// terminal has truncated on another partition reads InFlight yet
    /// committed, so it must be preserved, not discarded.
    /// </para>
    /// <para>
    /// Preserving every resident unresolved prepare lets the persisted row grow
    /// while a saga-terminal leak (the parked issue #2208 orphan source) is
    /// unfixed, but that growth is bounded by the count of genuinely
    /// unresolved prepares, is observable, and resolves the instant each saga
    /// terminates through <see cref="ResolveUnresolvedReplayWorkForTransaction"/>.
    /// That is strictly preferable to the alternative it replaces, which is
    /// silent permanent write loss.
    /// </para>
    /// <para>
    /// <summary>
    /// Idempotent: a prepare already recorded at (partition, offset) is left
    /// untouched, so a restore-then-re-read cannot double it.
    /// </para>
    /// <para>
    /// Issue #2183 observability. Recording beyond <paramref name="thresholdCap"/>
    /// is safe on the default <c>local</c> SQLite profile but a persist hazard
    /// on an Azure Table deployment (1MB entity cap), so the crossing is metered
    /// (<see cref="LatticeMetrics.LeafUnresolvedPrepareLedgerBeyondCap"/>) and
    /// warned once per activation. This is observability ONLY - the prepare is
    /// still recorded unconditionally; nothing here caps or drops it.
    /// </para>
    /// </summary>
    private void EnsureUnresolvedPrepareRecorded(int partition, long offset, in LatticeMutation mutation, int thresholdCap)
    {
        var index = DurableReplayWorkIndex();
        if (!index.Add((partition, offset)))
            return;

        var work = state.State.UnresolvedReplayWork ??= [];
        work.Add(new UnresolvedReplayWorkEntry(partition, offset, mutation));

        if (thresholdCap > 0 && work.Count > thresholdCap)
        {
            LatticeMetrics.LeafUnresolvedPrepareLedgerBeyondCap.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
                LatticeTenantLabel.ForTree(state.State.TreeId));

            if (!_warnedUnresolvedPrepareLedgerBeyondCap)
            {
                _warnedUnresolvedPrepareLedgerBeyondCap = true;
                ResolveLogger()?.LogWarning(
                    "Leaf {TreeId} has {Count} unresolved replay-work entries, beyond the "
                    + "MaxDurableUnresolvedReplayWork cap of {Cap} (issue #2183). A resident "
                    + "prepare is never dropped, so the row grows while the issue #2208 "
                    + "saga-terminal leak is unfixed. This is expected and benign on the "
                    + "default `local` SQLite durability profile (~1GB row), but on an Azure "
                    + "Table deployment the 1MB entity cap makes an unbounded row a persist "
                    + "hazard - alert on orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap "
                    + "there. Observability only; the ceiling still advances.",
                    state.State.TreeId, work.Count, thresholdCap);
            }
        }
    }

    /// <summary>
    /// One-shot throttle for the issue #2183 beyond-cap warning: naturally
    /// resets to <see langword="false"/> each activation (a fresh grain
    /// instance), so the warning surfaces once per activation while the metric
    /// records every crossing.
    /// </summary>
    private bool _warnedUnresolvedPrepareLedgerBeyondCap;

    /// <summary>
    /// Strikes the record for (<paramref name="partition"/>,
    /// <paramref name="offset"/>) off the ledger once its work has actually
    /// been applied - a deferred terminal draining in pass 2. Ignores an
    /// offset that was never recorded.
    /// </summary>
    private void ResolveUnresolvedReplayWork(int partition, long offset)
    {
        var work = state.State.UnresolvedReplayWork;
        if (work is null || work.Count == 0)
            return;

        for (var i = 0; i < work.Count; i++)
        {
            if (work[i].Partition != partition || work[i].Offset != offset)
                continue;
            work.RemoveAt(i);
            _durableReplayWorkIndex?.Remove((partition, offset));
            return;
        }
    }

    /// <summary>
    /// Strikes every record belonging to <paramref name="transactionId"/> off
    /// the ledger. Called from the saga terminal paths alongside
    /// <c>RemovePendingTxOffsetsForTransaction</c>, so a committed or aborted
    /// saga releases its durable footprint at exactly the moment it releases
    /// its in-memory clamp. A saga whose per-key prepares hashed across several
    /// WAL partitions has one record per partition; all of them go.
    /// </summary>
    private void ResolveUnresolvedReplayWorkForTransaction(Guid transactionId)
    {
        var work = state.State.UnresolvedReplayWork;
        if (work is null || work.Count == 0 || transactionId == Guid.Empty)
            return;

        for (var i = work.Count - 1; i >= 0; i--)
        {
            if (work[i].Mutation.TransactionId != transactionId)
                continue;
            _durableReplayWorkIndex?.Remove((work[i].Partition, work[i].Offset));
            work.RemoveAt(i);
        }
    }

    /// <summary>
    /// Reconstructs previously recorded replay work at the start of an
    /// activation, BEFORE pass 1 reads a single WAL slice - the whole point of
    /// the ledger.
    /// <para>
    /// A recorded prepare is re-applied through the identical
    /// <see cref="ILeafProjection.Apply(in LatticeMutation)"/> path a re-read
    /// would have taken, so <c>_pendingTx</c> is rebuilt exactly as before. A
    /// recorded terminal is appended to <paramref name="deferredTerminals"/> so
    /// pass 2 drains it in the ordinary way. Neither is added back to the
    /// in-memory <c>DeferredOffsetLedger</c>: doing so would re-impose the very
    /// clamp the record exists to lift, leaving the fix inert.
    /// </para>
    /// <para>
    /// A record at an offset the imminent replay will re-read anyway (above
    /// the effective checkpoint for its partition, which is the case whenever
    /// the cold-cache override drives the checkpoint back to -1) is DROPPED
    /// rather than restored, so the WAL read remains the single source for it
    /// and nothing is applied twice. The invariant is exactly: the ledger
    /// covers the offsets this replay will not re-read.
    /// </para>
    /// </summary>
    private void RestoreUnresolvedReplayWork(
        ILeafProjection projection,
        int partitionCount,
        long? checkpointOverride,
        List<DeferredTerminal> deferredTerminals)
    {
        var work = state.State.UnresolvedReplayWork;
        if (work is null || work.Count == 0)
            return;

        // Deterministic reconstruction order: partition, then WAL offset. The
        // persisted order is already this, but a row written by a different
        // sweep order must not change what the leaf rebuilds.
        var ordered = new List<UnresolvedReplayWorkEntry>(work);
        ordered.Sort(static (a, b) =>
        {
            var byPartition = a.Partition.CompareTo(b.Partition);
            return byPartition != 0 ? byPartition : a.Offset.CompareTo(b.Offset);
        });

        List<UnresolvedReplayWorkEntry>? kept = null;
        foreach (var entry in ordered)
        {
            if (entry.Partition < 0 || entry.Partition >= partitionCount)
            {
                // The tree's partition count shrank under this leaf. The record
                // is not addressable by this activation, but discarding it
                // would lose the work outright, so keep it untouched for an
                // activation that can address it.
                (kept ??= []).Add(entry);
                continue;
            }

            var checkpoint = checkpointOverride ?? GetPersistedCheckpointForPartition(entry.Partition);
            if (entry.Offset > checkpoint)
                continue; // Replay re-reads it; the WAL stays the single source.

            (kept ??= []).Add(entry);

            using (LatticeApplyOffsetContext.BeginScope(entry.Partition, entry.Offset))
            {
                if (entry.Mutation.IsPrepared)
                {
                    projection.Apply(entry.Mutation);
                }
                else
                {
                    deferredTerminals.Add(
                        new DeferredTerminal(entry.Partition, entry.Offset, entry.Mutation));
                }
            }
        }

        state.State.UnresolvedReplayWork = kept;
        _durableReplayWorkIndex = null;
    }
}
