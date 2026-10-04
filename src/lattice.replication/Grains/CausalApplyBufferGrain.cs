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
    private string? _treeId;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string TreeId => _treeId ??= context.GrainId.Key.ToString() ?? string.Empty;

    /// <inheritdoc />
    public async Task<int> ParkAsync(WalRecord entry, long admissionEpoch = 0)
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
            _epochs[CausalApplyBuffer.EntryKey.From(entry)] = admissionEpoch;
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
                            CancellationToken.None).ConfigureAwait(true);
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
                var localVc = await hwm.GetVectorAsync(CancellationToken.None).ConfigureAwait(true);
                var ready = buffer.DrainSatisfied(localVc, resolved.ClusterId);
                if (ready.Count == 0)
                {
                    return;
                }

                var deferred = false;
                foreach (var ent in ready)
                {
                    try
                    {
                        await applier.ApplyDrainedEntryAsync(ent, EpochOf(ent), CancellationToken.None).ConfigureAwait(true);
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
                        await grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).EnqueueAsync(
                            ent,
                            failureReason: ex.Message ?? "<no message>",
                            retryCount: 0,
                            reasonTag: reasonTag,
                            CancellationToken.None).ConfigureAwait(true);
                    }
                }

                if (deferred)
                {
                    Rebuild();
                    return;
                }

                foreach (var ent in ready)
                {
                    _epochs.Remove(CausalApplyBuffer.EntryKey.From(ent));
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

    private CausalApplyBuffer EnsureLoaded()
    {
        if (_buffer is not null)
        {
            return _buffer;
        }

        var buffer = new CausalApplyBuffer(TreeId);
        _epochs.Clear();
        foreach (var parked in state.State.Entries)
        {
            buffer.Restore(parked.Entry, parked.ParkedAtTicks);
            _epochs.TryAdd(CausalApplyBuffer.EntryKey.From(parked.Entry), parked.AdmissionEpoch);
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
            entries.Add(new ParkedCausalEntry { Entry = entry, ParkedAtTicks = parkedAtTicks, AdmissionEpoch = EpochOf(entry) });
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
    }
}
