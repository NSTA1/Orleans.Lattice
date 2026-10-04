using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Running totals of one <see cref="PreparedBucketSweep"/> run, kept outside the
/// sweep so a caller can still report how far a run that faulted got.
/// </summary>
internal sealed class PreparedBucketSweepProgress
{
    /// <summary>Prepared mutations replayed, or settled by a terminal, on the target.</summary>
    public long Replayed;

    /// <summary>Source leaves whose prepared mutations were read.</summary>
    public int LeavesVisited;
}

/// <summary>
/// Carries the prepared (not yet terminal) saga mutations a source shard holds
/// for a set of virtual slots onto a target shard that will serve those slots,
/// so a batch prepared before the target existed is not torn there. A prepared
/// bucket is not a live entry, so a copy of the source's rows never carries it,
/// and the source's mirroring carries only prepares made after it was switched
/// on. Used by an adaptive split for its moved slots (the retroactive sweep) and
/// by an online snapshot for every slot it copies (issue #4455).
/// <para>
/// For each prepared mutation the saga's decision is read under
/// <c>decisionTreeId</c>, the logical tree the saga records it under, and a
/// decision the registry masks is read as recorded (issue #4473): a decided
/// saga's terminal is applied to the target directly, with the prepared value as
/// the committed backstop; an undecided one is replayed as a prepare, which
/// registers the target as a participant, and the target is given a shadow
/// marker for the key. A post-sweep pass re-reads every replayed saga and
/// applies the terminal to those that decided while the sweep ran. Every step is
/// idempotent, so the caller recovers from a fault by re-running the whole sweep.
/// </para>
/// </summary>
internal static class PreparedBucketSweep
{
    /// <summary>
    /// Sweeps the leaf chain starting at <paramref name="firstLeafId"/> for
    /// prepared mutations in <paramref name="sortedSlots"/> and carries each onto
    /// <paramref name="target"/>. Not work-bounded: the caller's recovery contract
    /// is to re-run the whole sweep, and its cost is bounded by saga concurrency.
    /// </summary>
    internal static async Task RunAsync(
        IGrainFactory grainFactory,
        string decisionTreeId,
        GrainId firstLeafId,
        IShardRootGrain target,
        int[] sortedSlots,
        int virtualShardCount,
        PreparedBucketSweepProgress progress)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(decisionTreeId);
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(sortedSlots);
        ArgumentNullException.ThrowIfNull(progress);

        GrainId? leafId = firstLeafId;

        // Track per-txid snapshots so the post-sweep cleanup pass can
        // build per-saga committedValues payloads without re-walking
        // the source chain. Lazily allocated - the steady state is
        // zero pending mutations across the swept slots.
        Dictionary<Guid, List<PendingMutationSnapshot>>? perTxSnapshots = null;

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            progress.LeavesVisited++;
            var snapshots = await leaf.GetPendingMutationsForSlotsAsync(sortedSlots, virtualShardCount);
            foreach (var snapshot in snapshots)
            {
                // Per-snapshot pre-check: if the saga has already
                // terminalized at sweep-time, the saga's own
                // commit-phase broadcast has finished and the
                // destination cannot be reached via that path
                // (destination was not yet a participant when the
                // broadcast captured its participant set). Replaying
                // the prepare here would install an orphan in
                // destination's _pendingTx that no terminal will
                // ever drain. Instead, apply the terminal directly
                // with the snapshot value as the committedValues
                // backstop; the destination's leaf-side per-key
                // backstop path handles WAL durability and HLC
                // stamping. Aborted sagas drop the entry without
                // surfacing.
                // Read the decision under the LOGICAL tree, where the saga
                // records it; for a resized (aliased) tree the physical copy
                // has no registry rows, so a lookup keyed by physicalTreeId
                // reads InFlight for a committed saga and installs an orphan
                // (issue #4368). The post-sweep cleanup reads the same way.
                var preStatus = await DecisionForSweepAsync(grainFactory, decisionTreeId, snapshot.TransactionId);
                if (preStatus == TxStatus.Committed)
                {
                    Dictionary<string, byte[]>? committedValues = null;
                    if (!snapshot.IsTombstone && snapshot.Value is not null)
                        committedValues = new Dictionary<string, byte[]>(1) { [snapshot.Key] = snapshot.Value };
                    await target.AppendTxTerminalAsync(snapshot.TransactionId, committed: true, committedValues);
                    progress.Replayed++;
                    continue;
                }
                if (preStatus == TxStatus.Aborted)
                {
                    await target.AppendTxTerminalAsync(snapshot.TransactionId, committed: false);
                    progress.Replayed++;
                    continue;
                }

                // Saga still in flight: replay the prepare normally.
                // The replay's SetAsync also registers destination
                // as a participant via RecordAffectedLeafIfPreparedAsync,
                // so any saga broadcast that runs AFTER this point
                // will reach destination.
                await ReplayPreparedSnapshotAsync(target, snapshot, decisionTreeId);
                progress.Replayed++;

                // Install the destination-side shadow marker for
                // this in-flight saga. The drain pass that runs
                // AFTER the retroactive sweep imports the source's
                // pre-saga value with IsMigrated=true into dest's
                // Entries; without this marker, a reader observing
                // the saga as Committed after MarkCommittedAsync
                // (but BEFORE the backstop terminal reaches dest)
                // would surface that migrated pre-saga value and
                // split observation against any sibling whose
                // backstop has landed. The marker is cleared
                // automatically by ApplyTxTerminalAsync when the
                // saga's terminal reaches dest.
                //
                // Per-snapshot single-key array allocation is
                // intentional and cold-path: bounded by the count
                // of in-flight sagas at split-begin x keys-per-
                // saga in moved slots (the chaos suite caps this
                // at ~10 entries). Batching across snapshots would
                // entangle ordering with the per-snapshot
                // ReplayPreparedSnapshotAsync above, which must
                // register dest as a participant BEFORE its
                // shadow marker lands so that a terminal arriving
                // mid-replay cannot install an un-clearable marker.
                await target.MarkSagaShadowAsync(snapshot.TransactionId, new[] { snapshot.Key });

                // Track for post-sweep cleanup. Lazy allocation - the
                // chaos-free path leaves the dictionary null.
                perTxSnapshots ??= new Dictionary<Guid, List<PendingMutationSnapshot>>();
                if (!perTxSnapshots.TryGetValue(snapshot.TransactionId, out var list))
                {
                    list = new List<PendingMutationSnapshot>();
                    perTxSnapshots[snapshot.TransactionId] = list;
                }
                list.Add(snapshot);
            }
            leafId = await leaf.GetNextSiblingAsync();
        }

        // Post-sweep cleanup: close the orphan window for sagas
        // that were in-flight at per-snapshot pre-check time but
        // have since terminalized. Such a saga's broadcast may have
        // made its last participant fetch before the sweep registered
        // destination, sent the terminal only to source, and called
        // ForgetAsync - leaving the prepared entry on destination
        // orphaned. The registry's GetStatusManyAsync returns
        // Committed/Aborted while the decision is still reported -
        // including for TxDecisionRetention after ForgetAsync
        // tombstones it - then Indeterminate until the row is pruned,
        // and InFlight (the default fallback) once it is gone. An
        // Indeterminate answer is followed by the recorded decision,
        // as in the pre-check (issue #4473). For Committed/Aborted we
        // apply the terminal directly. For anything else we leave the
        // entry pending - either the saga is genuinely still in flight
        // (its eventual broadcast will reach destination, which is now
        // registered as a participant) or its decision is no longer
        // stored and the entry is a true orphan. The latter is shadowed by
        // any later prepare for the same key via the highest-HLC
        // tie-break in TryFindPendingForKey.
        if (perTxSnapshots is { Count: > 0 })
        {
            var txids = new List<Guid>(perTxSnapshots.Keys);
            var statuses = await TxRegistryFanOut.GetStatusManyAsync(
                grainFactory, decisionTreeId, txids);
            foreach (var (txid, reported) in statuses)
            {
                var status = reported == TxStatus.Indeterminate
                    ? await RecordedDecisionAsync(grainFactory, decisionTreeId, txid)
                    : reported;
                // Only a DECIDED status authorises acting. Anything else -
                // genuinely in flight, or a decision the registry currently
                // cannot determine - leaves the entry pending. Testing for
                // the decided cases rather than excluding InFlight matters:
                // the `committed` flag below is derived by elimination, so
                // an undecided status that slipped past this guard would be
                // silently treated as an abort and the prepared entry
                // discarded.
                if (status is not (TxStatus.Committed or TxStatus.Aborted)) continue;

                var committed = status == TxStatus.Committed;
                Dictionary<string, byte[]>? committedValues = null;
                if (committed)
                {
                    committedValues = new Dictionary<string, byte[]>();
                    foreach (var snap in perTxSnapshots[txid])
                    {
                        if (!snap.IsTombstone && snap.Value is not null)
                            committedValues[snap.Key] = snap.Value;
                    }
                }
                await target.AppendTxTerminalAsync(txid, committed, committedValues);
            }
        }
    }

    /// <summary>
    /// The saga's decision as the sweep acts on it: the reported status, or,
    /// when the registry masks a decision it still stores
    /// (<see cref="TxStatus.Indeterminate"/>), the recorded one. Treating a masked
    /// decision as in flight replayed the prepare, which the destination refuses
    /// for a decided saga (#4445), leaving only an activation-scoped shadow
    /// marker: a destination reactivation, or a terminal carrying no value for
    /// the moved key, then served the migrated pre-saga value (issue #4473). The
    /// sweep is finishing work its shard owns, the use
    /// <see cref="ITxRegistryGrain.GetRecordedStatusAsync"/> exists for, and a
    /// decided saga takes the terminal-with-backstop branch.
    /// </summary>
    private static async Task<TxStatus> DecisionForSweepAsync(IGrainFactory grainFactory, string decisionTreeId, Guid txid)
    {
        var registry = TxRegistryRouting.GetRegistry(grainFactory, decisionTreeId, txid);
        var status = await registry.GetStatusAsync(txid);
        return status == TxStatus.Indeterminate ? await registry.GetRecordedStatusAsync(txid) : status;
    }

    private static Task<TxStatus> RecordedDecisionAsync(IGrainFactory grainFactory, string decisionTreeId, Guid txid) =>
        TxRegistryRouting.GetRegistry(grainFactory, decisionTreeId, txid).GetRecordedStatusAsync(txid);

    /// <summary>
    /// Replays a single <see cref="Orleans.Lattice.BPlusTree.PendingMutationSnapshot"/> through
    /// the destination shard's standard write path. The four ambient
    /// scopes - transaction id, prepared flag, origin cluster, vector
    /// clock, HLC override - propagate via Orleans
    /// <see cref="Orleans.Runtime.RequestContext"/> so the destination
    /// leaf reads the same values at its HLC-tick site that the source
    /// leaf observed at prepare time. The destination's
    /// <c>BPlusLeafGrain.CommitSetAsync</c> then routes the mutation
    /// into its own pending-tx map (because <c>LatticePreparedContext.Current</c>
    /// is true) under the original <c>(txid, key)</c> identity.
    /// <para>
    /// Tombstones are replayed via <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.DeleteAsync"/>
    /// rather than the TTL-aware <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetAsync(string, byte[], long)"/>
    /// overload, so the destination's <c>CommitDeleteAsync</c> path
    /// stamps the prepared tombstone correctly. Non-tombstone replays
    /// use the TTL-aware Set overload so <c>ExpiresAtTicks</c> is
    /// preserved verbatim.
    /// </para>
    /// </summary>
    internal static async Task ReplayPreparedSnapshotAsync(IShardRootGrain target, PendingMutationSnapshot snapshot, string registryTreeId)
    {
        var previousTxId = LatticeTransactionContext.Current;
        LatticeTransactionContext.Set(snapshot.TransactionId);
        try
        {
            using var preparedScope = LatticePreparedContext.BeginScope();
            // A sweep replay can reach the destination after the saga decided
            // (the saga may decide between the sweep's pre-check and this
            // replay landing), so it is a forwarded prepare (#4445). Its
            // decision is recorded under the logical tree, as the pre-check
            // reads it (#4368).
            using var forwardedScope = LatticeForwardedPrepareContext.BeginScope(registryTreeId);
            using var originScope = LatticeOriginContext.With(snapshot.OriginClusterId);
            using var vcScope = LatticeVectorClockContext.With(snapshot.VectorClock);
            using var hlcScope = LatticeHlcOverrideContext.With(snapshot.Timestamp);
            // Carry the typed CRDT delta so the destination leaf's prepared
            // commit records it in its pending-tx delta side-map and folds it
            // on the saga's terminal (the per-replica union) rather than
            // installing the resharded LWW value verbatim. A plain LWW
            // snapshot (Delta null / Mode LwwRegister) opens no scope and
            // replays byte-for-byte as before.
            using var deltaScope = snapshot.Mode != LatticeMergeMode.LwwRegister
                    && snapshot.Delta is not null
                ? LatticeDeltaContext.With(snapshot.Delta)
                : null;

            if (snapshot.IsTombstone)
            {
                await target.DeleteAsync(snapshot.Key);
            }
            else
            {
                // Empty byte[] is the conventional value-of-a-tombstone
                // placeholder. Snapshots only carry a non-null
                // Value when IsTombstone is false, but defensively
                // substitute Array.Empty so the destination's
                // SetAsync(byte[]) parameter contract is satisfied
                // regardless of upstream shape.
                var value = snapshot.Value ?? Array.Empty<byte>();
                if (snapshot.ExpiresAtTicks > 0)
                    await target.SetAsync(snapshot.Key, value, snapshot.ExpiresAtTicks);
                else
                    await target.SetAsync(snapshot.Key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(previousTxId);
        }
    }
}
