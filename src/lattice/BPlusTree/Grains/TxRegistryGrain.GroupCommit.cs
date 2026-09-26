using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Group commit for the per-tree saga decision registry (issue #3475).
/// <para>
/// The registry is one activation per tree, and every atomic saga records its
/// participants, its decision and its cleanup through it. Before group commit
/// each of those calls awaited its own whole-state <c>WriteStateAsync</c>, and
/// the grain was non-reentrant, so the registry's write latency capped the
/// whole tree's saga rate and every read queued behind the writes in the
/// mailbox.
/// </para>
/// <para>
/// Now a mutating call performs its existing synchronous validate-and-mutate
/// and then awaits <see cref="CommitAsync"/> instead of its own write. At most
/// one <c>WriteStateAsync</c> is in flight (<see cref="_inFlight"/>); mutations
/// arriving while it runs accumulate in <see cref="_pending"/> and are carried
/// by the next write. A caller is released only when a write that includes its
/// mutation has completed durably, so acknowledge-after-persist is unchanged.
/// </para>
/// <para>
/// <b>Read-committed.</b> Mutations are visible in memory before they are
/// durable, so no reader may return an answer derived from them. Every reader
/// computes its answer synchronously and then awaits the newest commit group
/// that could have influenced it (<see cref="WhenDurableAsync(Guid)"/> for a
/// per-transaction answer, <see cref="WhenAllDurableAsync"/> for a tree-wide
/// one). If that group committed, the answer was built from durable state and
/// is returned; if it failed, the state it saw has been rolled back and the
/// reader recomputes. When nothing un-durable touches the answer the barrier
/// completes synchronously, so a read for a quiet transaction never waits
/// behind another saga's write.
/// </para>
/// <para>
/// <b>Failure.</b> A failed write fails every caller it carried and every
/// caller queued behind it, because the queued mutations were applied on top
/// of state that never became durable. Their undo actions run newest-first, so
/// the in-memory state returns exactly to the last durable state before any
/// caller observes the fault. Callers already retry.
/// </para>
/// </summary>
internal sealed partial class TxRegistryGrain
{
    /// <summary>
    /// One batch of mutations that a single <c>WriteStateAsync</c> will make
    /// durable.
    /// <para>
    /// Allocation cost, deliberately accepted: each group allocates itself and
    /// its completion source, and each mutation allocates its undo closure.
    /// Against the in-memory bench store (where nothing coalesces) that measured
    /// about 1.9 KB per saga on <c>SetMany atomic</c>; under real storage the
    /// per-group part amortises over every mutation the write carries, and the
    /// write itself re-serialises the whole registry row, which dwarfs it.
    /// </para>
    /// </summary>
    private sealed class CommitGroup
    {
        /// <summary>
        /// Completes when the write carrying this group has finished: success
        /// when it persisted, the storage exception when it (or the write
        /// ahead of it) failed. Continuations run asynchronously so completing
        /// the group never runs a caller's continuation inline inside the
        /// flush loop.
        /// </summary>
        public readonly TaskCompletionSource Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Undo actions, in the order their mutations were applied. The first is
        // held inline and the list is allocated only for a second mutation, so
        // an uncoalesced group (one mutation, the common case when storage is
        // fast) allocates no collection.
        private Action? _firstRollback;
        private List<Action>? _moreRollbacks;

        // Transaction ids whose per-transaction rows (decision, tombstone,
        // delegation, participants, terminal tally) this group mutated. A
        // per-transaction reader waits only for a group that touched its id.
        // Same inline-first shape as the rollbacks.
        private Guid _firstTxid;
        private bool _hasFirstTxid;
        private HashSet<Guid>? _moreTxids;

        /// <summary>Number of mutations the group carries.</summary>
        public int MutationCount { get; private set; }

        /// <summary>Records one applied mutation's undo action.</summary>
        public void AddRollback(Action rollback)
        {
            if (MutationCount == 0)
            {
                _firstRollback = rollback;
            }
            else
            {
                (_moreRollbacks ??= new List<Action>(4)).Add(rollback);
            }
            MutationCount++;
        }

        /// <summary>
        /// Runs every undo action newest-first, passing each to
        /// <paramref name="run"/> so the caller owns the per-action fault
        /// handling.
        /// </summary>
        public void RunRollbacksNewestFirst(Action<Action> run)
        {
            if (_moreRollbacks is { } more)
            {
                for (var i = more.Count - 1; i >= 0; i--)
                {
                    run(more[i]);
                }
            }
            if (_firstRollback is { } first)
            {
                run(first);
            }
        }

        /// <summary>Marks <paramref name="txid"/> as touched by this group.</summary>
        public void AddTxid(Guid txid)
        {
            if (_moreTxids is { } more)
            {
                more.Add(txid);
            }
            else if (!_hasFirstTxid)
            {
                _firstTxid = txid;
                _hasFirstTxid = true;
            }
            else if (_firstTxid != txid)
            {
                _moreTxids = [_firstTxid, txid];
            }
        }

        /// <summary>
        /// <see langword="true"/> when the group's txid set (ignoring
        /// <see cref="TouchesAllTxids"/>) contains <paramref name="txid"/>.
        /// </summary>
        public bool ContainsTxid(Guid txid)
            => _moreTxids is { } more ? more.Contains(txid) : _hasFirstTxid && _firstTxid == txid;

        /// <summary>
        /// <see langword="true"/> when the group carries a mutation whose effect
        /// on per-transaction reads cannot be expressed as a txid set - a
        /// snapshot pin, which changes the read mask for every txid it covers.
        /// Every per-transaction reader then treats the group as touching it.
        /// </summary>
        public bool TouchesAllTxids;

        /// <summary>
        /// <see langword="true"/> when <paramref name="txid"/> may read
        /// differently because of this group's mutations.
        /// </summary>
        public bool Touches(Guid txid) => TouchesAllTxids || ContainsTxid(txid);
    }

    /// <summary>Mutations applied in memory and not yet handed to storage.</summary>
    private CommitGroup? _pending;

    /// <summary>Mutations whose write is currently outstanding.</summary>
    private CommitGroup? _inFlight;

    /// <summary>
    /// <see langword="true"/> while <see cref="FlushLoopAsync"/> is running.
    /// Guards the single-writer invariant: only the loop issues writes.
    /// </summary>
    private bool _flushing;

    /// <summary>
    /// Returns the group the next mutation joins, marking it as touching
    /// <paramref name="txid"/>. Call it and <see cref="CommitAsync"/> in the
    /// same synchronous block as the mutation itself.
    /// </summary>
    private CommitGroup PendingGroup(Guid txid)
    {
        var group = _pending ??= new CommitGroup();
        group.AddTxid(txid);
        return group;
    }

    /// <summary>
    /// Returns the group the next mutation joins without naming a transaction.
    /// The caller must mark what it touches (<see cref="CommitGroup.AddTxid"/> or
    /// <see cref="CommitGroup.TouchesAllTxids"/>).
    /// </summary>
    private CommitGroup PendingGroup() => _pending ??= new CommitGroup();

    /// <summary>
    /// Enlists an already-applied mutation into <paramref name="group"/> and
    /// returns a task that completes once a write carrying it is durable, or
    /// faults with the storage exception after <paramref name="rollback"/> (and
    /// every newer undo action) has run. Starts the flush loop if no write is in
    /// flight.
    /// </summary>
    private Task CommitAsync(CommitGroup group, Action rollback)
    {
        Debug.Assert(ReferenceEquals(group, _pending), "A mutation must join the pending group.");
        group.AddRollback(rollback);
        if (!_flushing)
        {
            _ = FlushLoopAsync();
        }
        return group.Completion.Task;
    }

    /// <summary>
    /// Writes pending groups one at a time until none remain. Never throws:
    /// every outcome is delivered through the groups' completion sources.
    /// </summary>
    private async Task FlushLoopAsync()
    {
        _flushing = true;
        try
        {
            while (_pending is not null)
            {
                if (!_shardHighWaterRaised)
                {
                    // Raise before detaching the pending group. The registry's
                    // mutators interleave, so a mutation applied while the raise
                    // is outstanding joins this group; were the group detached
                    // first, that mutation would ride on this group's write
                    // unrecorded, and a later failure of its own group would roll
                    // it back in memory while storage kept it.
                    var raiseStarted = Stopwatch.GetTimestamp();
                    Exception? raiseFailure = null;
                    try
                    {
                        await RaiseShardHighWaterAsync();
                    }
                    catch (Exception ex)
                    {
                        raiseFailure = ex;
                    }

                    if (raiseFailure is not null)
                    {
                        var unraised = _pending!;
                        _pending = null;
                        RecordWrite(unraised.MutationCount, Stopwatch.GetElapsedTime(raiseStarted), ok: false);
                        RunRollbacks(unraised);
                        unraised.Completion.TrySetException(TranslateWriteFailure(raiseFailure, ownWrite: false));
                        continue;
                    }
                }

                var group = _pending!;
                _pending = null;
                _inFlight = group;
                var started = Stopwatch.GetTimestamp();
                Exception? failure = null;
                try
                {
                    await state.WriteStateAsync();
                }
                catch (Exception ex)
                {
                    failure = ex;
                }

                _inFlight = null;
                RecordWrite(group.MutationCount, Stopwatch.GetElapsedTime(started), failure is null);

                if (failure is null)
                {
                    group.Completion.TrySetResult();
                    continue;
                }

                // Everything queued behind the failed write was applied on top
                // of state that never became durable, so it fails too. Undo the
                // newest mutations first so each undo restores exactly the
                // state its own mutation observed.
                var surfaced = TranslateWriteFailure(failure, ownWrite: true);
                var queued = _pending;
                _pending = null;
                if (queued is not null)
                {
                    RunRollbacks(queued);
                }
                RunRollbacks(group);
                group.Completion.TrySetException(surfaced);
                queued?.Completion.TrySetException(surfaced);
            }
        }
        finally
        {
            _flushing = false;
        }
    }

    /// <summary>
    /// Converts a failed write's fault into the
    /// <see cref="TxRegistryWriteFailedException"/> its callers observe, so a
    /// storage provider's exception type never crosses a grain-call boundary
    /// (a client without that provider cannot load it). When
    /// <paramref name="ownWrite"/> is set and the fault is an
    /// optimistic-concurrency conflict (<see cref="InconsistentStateException"/>),
    /// storage holds a row this activation never read, so the activation
    /// deactivates and the next call reloads it; the exception type alone is the
    /// discriminator, matching <c>ShardRootGrain</c>.
    /// </summary>
    private TxRegistryWriteFailedException TranslateWriteFailure(Exception failure, bool ownWrite)
    {
        if (failure is TxRegistryWriteFailedException already)
        {
            return already;
        }

        var conflict = ownWrite && IsWriteConflict(failure);
        if (conflict)
        {
            logger.LogWarning(
                failure,
                "Registry {RegistryKey} lost an optimistic-concurrency check on its state write; deactivating so the next call reloads from storage.",
                GrainKey);
            this.DeactivateOnIdle();
        }
        else
        {
            logger.LogWarning(
                failure,
                "Registry {RegistryKey} failed its state write; the group was rolled back and its callers may retry.",
                GrainKey);
        }

        return new TxRegistryWriteFailedException(GrainKey, failure, conflict);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="failure"/> (or an
    /// exception it wraps) is an <see cref="InconsistentStateException"/>.
    /// </summary>
    internal static bool IsWriteConflict(Exception failure)
    {
        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (e is InconsistentStateException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Runs a failed group's undo actions newest-first. An undo that throws
    /// leaves memory ahead of disk with no exact way back, so the activation
    /// is deactivated and the next call reloads from storage.
    /// </summary>
    private void RunRollbacks(CommitGroup group)
    {
        // Failure path only, so the per-call delegate here is not a hot-path cost.
        group.RunRollbacksNewestFirst(undo =>
        {
            try
            {
                undo();
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Registry {TreeId} could not undo a mutation after a failed state write; deactivating so the next call reloads from storage.",
                    TreeId);
                this.DeactivateOnIdle();
            }
        });
    }

    /// <summary>
    /// Whether this activation has made its shard's index durable in the tree's
    /// <see cref="ITxRegistryHighWaterGrain"/> mark (issue #3501). Always
    /// <see langword="true"/> for the legacy registry, which every tree-wide read
    /// covers unconditionally.
    /// </summary>
    private bool _shardHighWaterRaised;

    /// <summary>
    /// Raises the tree's shard high-water mark to cover this shard before the
    /// activation's first state write, so a tree-wide read that could miss this
    /// shard's decisions (its mark is below this shard's index) provably ran
    /// before any of them were durable. A failure fails the pending write group,
    /// which rolls back and surfaces the fault to its callers exactly as a
    /// storage failure would; the next write retries the raise.
    /// </summary>
    private async Task RaiseShardHighWaterAsync()
    {
        if (TxRegistryRouting.TryParseShardKey(GrainKey, out var treeId, out var shard))
        {
            var mark = await grainFactory.GetGrain<ITxRegistryHighWaterGrain>(treeId).RaiseShardHighWaterAsync(shard + 1);
            TxRegistryHighWaterCache.Observe(grainFactory, treeId, mark);
        }

        _shardHighWaterRaised = true;
    }

    /// <summary>Records one registry write on the group-commit instruments.</summary>
    private void RecordWrite(int mutations, TimeSpan elapsed, bool ok)
    {
        var outcome = ok ? LatticeMetrics.TxRegistryWriteOutcomeOk : LatticeMetrics.TxRegistryWriteOutcomeFault;
        LatticeMetrics.TxRegistryWrites.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId), outcome, LatticeTenantLabel.ForTree(TreeId));
        LatticeMetrics.TxRegistryWriteMutations.Record(mutations, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId), outcome, LatticeTenantLabel.ForTree(TreeId));
        LatticeMetrics.TxRegistryWriteDuration.Record(elapsed.TotalMilliseconds, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId), outcome, LatticeTenantLabel.ForTree(TreeId));
    }

    /// <summary>
    /// Read-committed barrier for an answer about <paramref name="txid"/>.
    /// Completes synchronously with <see langword="true"/> when no un-durable
    /// group touches the transaction. Otherwise waits for the newest group that
    /// does, returning <see langword="true"/> if it committed (everything the
    /// caller read was durable) and <see langword="false"/> if it failed (what
    /// the caller read was rolled back and must be recomputed).
    /// </summary>
    private ValueTask<bool> WhenDurableAsync(Guid txid)
    {
        // Groups are written in order and a failure fails every newer group, so
        // the newest touching group's success implies every older one's.
        var group = _pending is { } pending && pending.Touches(txid)
            ? pending
            : _inFlight is { } inFlight && inFlight.Touches(txid) ? inFlight : null;
        return group is null ? new ValueTask<bool>(true) : AwaitGroupAsync(group);
    }

    /// <summary>
    /// Read-committed barrier for an answer about several transactions. See
    /// <see cref="WhenDurableAsync(Guid)"/>.
    /// </summary>
    private ValueTask<bool> WhenDurableAsync(IReadOnlyList<Guid> txids)
    {
        var group = TouchesAny(_pending, txids) ? _pending
            : TouchesAny(_inFlight, txids) ? _inFlight : null;
        return group is null ? new ValueTask<bool>(true) : AwaitGroupAsync(group);

        static bool TouchesAny(CommitGroup? group, IReadOnlyList<Guid> txids)
        {
            if (group is null) return false;
            if (group.TouchesAllTxids) return true;
            for (var i = 0; i < txids.Count; i++)
            {
                if (group.ContainsTxid(txids[i])) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Read-committed barrier for a tree-wide answer (snapshots, the revision
    /// token, the pin and delegation censuses): waits for the newest un-durable
    /// group, whatever it touched. See <see cref="WhenDurableAsync(Guid)"/>.
    /// </summary>
    private ValueTask<bool> WhenAllDurableAsync()
    {
        var group = _pending ?? _inFlight;
        return group is null ? new ValueTask<bool>(true) : AwaitGroupAsync(group);
    }

    private static async ValueTask<bool> AwaitGroupAsync(CommitGroup group)
    {
        // Continue on the captured scheduler (the activation's), and observe a
        // failure as a value rather than an exception: the failing caller gets
        // the exception, a reader only needs to know to recompute.
        var completion = group.Completion.Task;
        await completion.ConfigureAwait(
            ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        return completion.IsCompletedSuccessfully;
    }
}
