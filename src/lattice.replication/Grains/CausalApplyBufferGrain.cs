using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="ICausalApplyBufferGrain"/>: the durable per-tree
/// causal-apply buffer (#4464). See the interface for the contract.
/// <para>
/// The in-memory <see cref="CausalApplyBuffer"/> mirrors the persisted
/// <see cref="CausalApplyBufferState"/> and enforces the same bound and
/// overflow behaviour it always has
/// (<see cref="LatticeReplicationOptions.CausalBufferMaxEntries"/> /
/// <see cref="LatticeReplicationOptions.CausalBufferMaxBytes"/>, oldest entry
/// evicted to the dead-letter queue with
/// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>). Every mutation is
/// written through: an evicted entry is dead-lettered BEFORE the write that
/// removes it, and a drained entry is removed only by a write that follows its
/// apply. When a write or a dead-letter enqueue fails, the in-memory copy is
/// rebuilt from the last persisted state, so it never holds less than storage.
/// </para>
/// </summary>
internal sealed class CausalApplyBufferGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ReplicationApplier applier,
    ILogger<CausalApplyBufferGrain> logger,
    [PersistentState("replication-causal-buffer", LatticeOptions.StorageProviderName)]
    IPersistentState<CausalApplyBufferState> state)
    : ICausalApplyBufferGrain, IGrainBase
{
    private CausalApplyBuffer? _buffer;

    // The receive-fence epoch each parked entry was admitted under (issue
    // #4593), keyed by the buffer's dedup identity. Mirrors the persisted
    // ParkedCausalEntry.AdmissionEpoch and is rebuilt with the buffer.
    private readonly Dictionary<CausalApplyBuffer.EntryKey, long> _epochs = new();

    // The source lineage each stamped parked entry's sender read it under (issue
    // #4707). Mirrors the persisted ParkedCausalEntry.SourceLineage and is
    // rebuilt with the buffer; an unstamped entry has no row.
    private readonly Dictionary<CausalApplyBuffer.EntryKey, ReplicationSourceLineageStamp> _lineages = new();
    private string? _treeId;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string TreeId => _treeId ??= context.GrainId.Key.ToString() ?? string.Empty;

    /// <inheritdoc />
    public async Task<int> ParkAsync(WalRecord entry, long admissionEpoch = 0, ReplicationSourceLineageStamp? sourceLineage = null)
    {
        var buffer = EnsureLoaded();
        var resolved = options.Get(TreeId);
        var outcome = buffer.TryAdd(
            entry,
            resolved.CausalBufferMaxEntries,
            resolved.CausalBufferMaxBytes,
            out var evicted);

        if (outcome != AddOutcome.Duplicate)
        {
            var parkedKey = CausalApplyBuffer.EntryKey.From(entry);
            _epochs[parkedKey] = admissionEpoch;
            if (sourceLineage is { } stamp)
            {
                _lineages[parkedKey] = stamp;
            }
            else
            {
                _lineages.Remove(parkedKey);
            }

            foreach (var displaced in evicted)
            {
                _epochs.Remove(CausalApplyBuffer.EntryKey.From(displaced));
            }

            try
            {
                if (outcome == AddOutcome.AddedWithEviction && evicted.Count > 0)
                {
                    // Overflow drops an entry that was acknowledged to its
                    // sender, so it must reach the dead-letter queue before
                    // the write that removes it from the durable buffer.
                    var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId);
                    foreach (var displaced in evicted)
                    {
                        await dlq.EnqueueAsync(
                            displaced,
                            failureReason: "Causal-apply buffer full; evicted blocked entry to make room.",
                            retryCount: 0,
                            reasonTag: LatticeReplicationMetrics.ReasonHlcSkew,
                            CancellationToken.None,
                            LineageOf(displaced)).ConfigureAwait(true);
                    }

                    foreach (var displaced in evicted)
                    {
                        _lineages.Remove(CausalApplyBuffer.EntryKey.From(displaced));
                    }
                }

                await PersistAsync(buffer).ConfigureAwait(true);
            }
            catch
            {
                Rebuild();
                throw;
            }
        }

        // Issue #4586: the origin's frontier lists the write as held before the
        // caller acknowledges it - also for a duplicate, whose first publication
        // may have failed - or a dependent could be released while it sits here.
        await PublishHeldAsync(entry.OriginClusterId, strict: true).ConfigureAwait(true);

        // Re-check after the insert: an advance whose drain ran between the
        // caller's dependency check and this park would otherwise leave the
        // entry parked with its dependencies already met (the lost wakeup).
        await DrainCoreAsync(buffer).ConfigureAwait(true);
        return EnsureLoaded().Count;
    }

    /// <inheritdoc />
    public async Task<int> DrainAsync()
    {
        var buffer = EnsureLoaded();
        if (buffer.Count == 0)
        {
            return 0;
        }

        await DrainCoreAsync(buffer).ConfigureAwait(true);
        return EnsureLoaded().Count;
    }

    /// <inheritdoc />
    public Task<int> CountAsync() => Task.FromResult(EnsureLoaded().Count);

    /// <inheritdoc />
    public Task<bool> IsHoldingAsync(string originClusterId, HybridLogicalClock timestamp)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        foreach (var parked in state.State.Entries)
        {
            if (parked.Entry.Timestamp == timestamp
                && string.Equals(parked.Entry.OriginClusterId, originClusterId, StringComparison.Ordinal))
            {
                return Task.FromResult(true);
            }
        }

        return Task.FromResult(false);
    }

    /// <summary>
    /// Publishes every write of <paramref name="originClusterId"/> the durable
    /// buffer holds to the origin's frontier (issue #4586). Strict publication
    /// throws on failure; a best-effort one - after a removal, when a stale
    /// listing only delays a dependent until the frontier confirms it here - does
    /// not.
    /// </summary>
    private async Task PublishHeldAsync(string? originClusterId, bool strict)
    {
        if (string.IsNullOrEmpty(originClusterId)
            || string.Equals(originClusterId, options.Get(TreeId).ClusterId, StringComparison.Ordinal))
        {
            return;
        }

        var held = new HashSet<HybridLogicalClock>();
        foreach (var parked in state.State.Entries)
        {
            if (string.Equals(parked.Entry.OriginClusterId, originClusterId, StringComparison.Ordinal))
            {
                held.Add(parked.Entry.Timestamp);
            }
        }

        try
        {
            await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(originClusterId)
                .SetHeldAsync(ReplicationOriginFrontierGrain.BufferSource(TreeId), held, CancellationToken.None)
                .ConfigureAwait(true);
        }
        catch (Exception ex) when (!strict && ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Publishing the held writes of origin {Origin} for tree {Tree} failed; the frontier confirms them on demand", originClusterId, TreeId);
        }
    }

    /// <inheritdoc />
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken token)
    {
        // Withdraw this activation's contribution from the buffer gauges; the
        // next activation restores it from durable state.
        _buffer?.Release();
        _buffer = null;
        return Task.CompletedTask;
    }

    private async Task DrainCoreAsync(CausalApplyBuffer buffer)
    {
        if (buffer.Count == 0)
        {
            return;
        }

        var resolved = options.Get(TreeId);
        var hwm = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(TreeId);
        try
        {
            // Fixed point: each drained apply may advance the local vector
            // clock and unblock further entries on the next pass.
            while (buffer.Count > 0)
            {
                var (ready, lost) = await TakeDecidedAsync(buffer, hwm, resolved.ClusterId).ConfigureAwait(true);
                if (ready.Count == 0 && lost.Count == 0)
                {
                    return;
                }

                // Entries taken out of the in-memory buffer that must stay parked
                // because the dead-letter queue is full (#4603), or because their
                // source lineage could not be checked yet (#4707); re-inserted
                // before the removal below is persisted.
                List<WalRecord>? keepParked = null;

                // A dependency on a write this tree lost for good can never be
                // satisfied (#4603): dead-letter the dependent as a terminal state.
                foreach (var ent in lost)
                {
                    if (!await TryDeadLetterAsync(
                            ent,
                            "A causal dependency of this entry names a write this cluster acknowledged and then lost "
                            + "(it was discarded from the dead-letter queue), so the entry can never be applied in causal order.",
                            LatticeReplicationMetrics.ReasonDependencyLost,
                            LineageOf(ent)).ConfigureAwait(true))
                    {
                        (keepParked ??= new List<WalRecord>()).Add(ent);
                    }
                }

                var deferred = false;
                foreach (var ent in ready)
                {
                    try
                    {
                        var lineageVerdict = await applier
                            .ApplyDrainedEntryAsync(ent, EpochOf(ent), LineageOf(ent), CancellationToken.None)
                            .ConfigureAwait(true);
                        if (lineageVerdict == ReplicationSourceLineageGate.Verdict.RefuseLineage)
                        {
                            // Issue #4707: the entry's sender read it under a
                            // source lineage this tree no longer holds - a source
                            // restore, purge and recreate, or alias move since -
                            // so it is not part of what the tree now replicates.
                            // A push of it would be refused; discard it here.
                            logger.LogInformation(
                                "Causal-apply buffer for tree {Tree} discarded an entry its sender read under a replaced source lineage (key {Key}).",
                                TreeId, ent.Key);
                        }
                        else if (lineageVerdict == ReplicationSourceLineageGate.Verdict.RefuseTransient)
                        {
                            (keepParked ??= new List<WalRecord>()).Add(ent);
                        }
                    }
                    catch (TxDecisionGateRefusedException gated)
                        when (gated.Refusal is TxDecisionGateRefusal.DecisionGated or TxDecisionGateRefusal.RegistrationFenced)
                    {
                        // Issue #4485: a snapshot capture holds the tree's saga
                        // decision gate (or a backup set its fence). That is not a
                        // fault and must never dead-letter a saga terminal: stop
                        // the drain and leave every entry not yet durably removed
                        // parked, so the next drain (or maintenance tick) re-applies
                        // it once the capture releases the registry. Re-applying an
                        // entry this pass already applied is idempotent at the leaf.
                        deferred = true;
                        break;
                    }
                    catch (CopyReceiveFencedException fenced) when (fenced.AdmittedBeforeRestore)
                    {
                        // Issue #4593: the entry was parked before a coordinated
                        // restore paused receiving, and the tree now serves the
                        // restored copy. No peer ships a post-cutover write before
                        // the saga completes globally, so the entry is a
                        // pre-cutover write, and the restore excludes it: discard
                        // it rather than re-advance the restored cut.
                        logger.LogInformation(
                            "Causal-apply buffer for tree {Tree} discarded an entry parked before a coordinated restore (key {Key}).",
                            TreeId, ent.Key);
                    }
                    catch (CopyReceiveFencedException)
                    {
                        // Issue #4593: the entry routed to a restored copy a
                        // coordinated restore still holds closed. Not a fault:
                        // stop and leave it parked until the copy opens.
                        deferred = true;
                        break;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        // The entry was acknowledged when it was parked, so it
                        // has no transport retry: dead-letter it, before the
                        // write below removes it from the durable buffer.
                        // ArgumentException / InvalidOperationException are
                        // schema-shaped faults; everything else is unknown.
                        var reasonTag = ex is ArgumentException or InvalidOperationException
                            ? LatticeReplicationMetrics.ReasonSchema
                            : LatticeReplicationMetrics.ReasonUnknown;
                        if (!await TryDeadLetterAsync(ent, ex.Message ?? "<no message>", reasonTag, LineageOf(ent)).ConfigureAwait(true))
                        {
                            (keepParked ??= new List<WalRecord>()).Add(ent);
                        }
                    }
                }

                if (deferred)
                {
                    Rebuild();
                    return;
                }

                foreach (var ent in ready)
                {
                    if (keepParked is null || !keepParked.Contains(ent))
                    {
                        _epochs.Remove(CausalApplyBuffer.EntryKey.From(ent));
                        _lineages.Remove(CausalApplyBuffer.EntryKey.From(ent));
                    }
                }

                foreach (var ent in lost)
                {
                    if (keepParked is null || !keepParked.Contains(ent))
                    {
                        _epochs.Remove(CausalApplyBuffer.EntryKey.From(ent));
                        _lineages.Remove(CausalApplyBuffer.EntryKey.From(ent));
                    }
                }

                if (keepParked is not null)
                {
                    // The dead-letter queue is full, or a source lineage could not
                    // be checked: keep these acknowledged entries parked rather
                    // than lose them, and stop this drain so they are retried on
                    // the next one instead of spinning here.
                    foreach (var ent in keepParked)
                    {
                        buffer.Restore(ent, DateTime.UtcNow.Ticks);
                    }

                    await PersistAsync(buffer).ConfigureAwait(true);
                    return;
                }

                // Durable removal strictly after each entry's apply (or
                // dead-letter) returned. A crash before this write leaves the
                // entries parked and the next drain re-applies them, which is
                // idempotent at the leaf.
                await PersistAsync(buffer).ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            // Entries already taken out of the in-memory buffer but not yet
            // durably removed are restored from storage and retried next drain.
            Rebuild();
            logger.LogWarning(ex, "Causal-apply buffer drain for tree {Tree} failed; it will be retried", TreeId);
            throw;
        }
    }

    private long EpochOf(WalRecord entry) =>
        _epochs.TryGetValue(CausalApplyBuffer.EntryKey.From(entry), out var epoch) ? epoch : 0;

    private ReplicationSourceLineageStamp? LineageOf(WalRecord entry) =>
        _lineages.TryGetValue(CausalApplyBuffer.EntryKey.From(entry), out var stamp) ? stamp : null;

    /// <summary>
    /// Dead-letters <paramref name="entry"/>, or returns <see langword="false"/>
    /// when the dead-letter queue is full (#4603) so the caller keeps it parked.
    /// </summary>
    private async Task<bool> TryDeadLetterAsync(
        WalRecord entry,
        string failureReason,
        string reasonTag,
        ReplicationSourceLineageStamp? sourceLineage)
    {
        try
        {
            await grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).EnqueueAsync(
                entry,
                failureReason,
                retryCount: 0,
                reasonTag,
                CancellationToken.None,
                sourceLineage).ConfigureAwait(true);
            return true;
        }
        catch (ReplicationDeadLetterQueueFullException)
        {
            return false;
        }
    }

    /// <summary>
    /// Asks the tree's high-water-mark grain for a verdict on every parked
    /// entry that declares dependencies, then takes the decided ones out of
    /// the in-memory buffer in FIFO order: those whose dependencies are met
    /// (or that declare none) and those that depend on a write the tree lost
    /// for good. The grain is non-reentrant, so the buffer cannot change
    /// between the snapshot and the drain.
    /// </summary>
    private static async Task<(List<WalRecord> Ready, List<WalRecord> Lost)> TakeDecidedAsync(
        CausalApplyBuffer buffer,
        IReplicationHighWaterMarkGrain hwm,
        string? localClusterId)
    {
        var parked = buffer.Snapshot();
        var verdicts = new Dictionary<WalRecord, CausalDependencyVerdict>();
        List<VersionVector>? toCheck = null;
        List<WalRecord>? checkedEntries = null;
        foreach (var (entry, _) in parked)
        {
            var required = CausalApplyBuffer.RequiredDependencies(entry, localClusterId);
            if (required is null)
            {
                verdicts[entry] = CausalDependencyVerdict.Met;
                continue;
            }

            (toCheck ??= new List<VersionVector>()).Add(required);
            (checkedEntries ??= new List<WalRecord>()).Add(entry);
        }

        if (toCheck is not null)
        {
            var results = await hwm.CheckDependenciesAsync(toCheck, CancellationToken.None).ConfigureAwait(true);
            for (var i = 0; i < checkedEntries!.Count && i < results.Length; i++)
            {
                verdicts[checkedEntries[i]] = results[i];
            }
        }

        var ready = new List<WalRecord>();
        var lost = new List<WalRecord>();
        if (verdicts.Count == 0)
        {
            return (ready, lost);
        }

        foreach (var entry in buffer.DrainSatisfied(e => verdicts.TryGetValue(e, out var v) && v != CausalDependencyVerdict.Unmet))
        {
            (verdicts[entry] == CausalDependencyVerdict.Lost ? lost : ready).Add(entry);
        }

        return (ready, lost);
    }

    private CausalApplyBuffer EnsureLoaded()
    {
        if (_buffer is not null)
        {
            return _buffer;
        }

        var buffer = new CausalApplyBuffer(TreeId);
        _epochs.Clear();
        _lineages.Clear();
        foreach (var parked in state.State.Entries)
        {
            buffer.Restore(parked.Entry, parked.ParkedAtTicks);
            var key = CausalApplyBuffer.EntryKey.From(parked.Entry);
            _epochs.TryAdd(key, parked.AdmissionEpoch);
            if (parked.SourceLineage is { } stamp)
            {
                _lineages.TryAdd(key, stamp);
            }
        }

        _buffer = buffer;
        return buffer;
    }

    private void Rebuild()
    {
        _buffer?.Release();
        _buffer = null;
        EnsureLoaded();
    }

    private async Task PersistAsync(CausalApplyBuffer buffer)
    {
        var snapshot = buffer.Snapshot();
        var entries = new List<ParkedCausalEntry>(snapshot.Count);
        foreach (var (entry, parkedAtTicks) in snapshot)
        {
            entries.Add(new ParkedCausalEntry
            {
                Entry = entry,
                ParkedAtTicks = parkedAtTicks,
                AdmissionEpoch = EpochOf(entry),
                SourceLineage = LineageOf(entry),
            });
        }

        var previous = state.State.Entries;
        state.State.Entries = entries;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.Entries = previous;
            throw;
        }

        // Issue #4586: a write removed from the buffer (applied, or moved to the
        // dead-letter queue, which published it first) leaves its origin's held set.
        var removedOrigins = new HashSet<string>(StringComparer.Ordinal);
        foreach (var parked in previous)
        {
            if (!string.IsNullOrEmpty(parked.Entry.OriginClusterId))
            {
                removedOrigins.Add(parked.Entry.OriginClusterId);
            }
        }

        foreach (var origin in removedOrigins)
        {
            await PublishHeldAsync(origin, strict: false).ConfigureAwait(true);
        }
    }
}
