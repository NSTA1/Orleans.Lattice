using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator for a cross-tree atomic write. See
/// <see cref="ILatticeCrossTreeTxGrain"/> for the contract. One activation per
/// <c>operationId</c> (this grain's key). Drives a two-level saga over the
/// participating trees' <see cref="IAtomicWriteGrain"/> sub-sagas:
/// <list type="number">
///   <item><description><b>Prepare.</b> Dispatch
///   <see cref="IAtomicWriteGrain.PrepareForCoordinatorAsync"/> to every tree's
///   sub-saga (keyed <c>{treeId}/{operationId}</c>). Each stages its writes into
///   hidden pending buckets, registers its per-tree registry to delegate the
///   sub-saga txid back to this coordinator, and parks.</description></item>
///   <item><description><b>Decide.</b> When every participant votes
///   <see cref="CrossTreePrepareVote.Prepared"/>, persist
///   <see cref="CrossTreeTxPhase.Committed"/> - the single global decision
///   moment that flips the batch visible on every tree at once. Any non-Prepared
///   vote persists <see cref="CrossTreeTxPhase.Aborted"/>.</description></item>
///   <item><description><b>Finalize.</b> Fan out
///   <see cref="IAtomicWriteGrain.FinalizeAsync"/> to each prepared participant
///   so it records its per-tree decision and drops/keeps the staged writes, then
///   complete.</description></item>
/// </list>
/// Crash recovery is reminder-driven: a keepalive reminder resumes
/// <see cref="RunCoordinatorAsync"/> from the persisted phase, and every step
/// (prepare, decide, finalize) is idempotent.
/// </summary>
internal sealed class LatticeCrossTreeTxGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeCrossTreeTxGrain> logger,
    [PersistentState("cross-tree-tx", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeTxState> state)
    : TtlGrain<LatticeCrossTreeTxGrain>(context, reminderRegistry, logger), ILatticeCrossTreeTxGrain
{
    private const string KeepaliveReminderName = "cross-tree-tx-keepalive";
    private const string RetentionReminderName = "cross-tree-tx-retention";

    /// <summary>This coordinator's key (the cross-tree operationId).</summary>
    private string OperationId => GrainContext.GrainId.Key.ToString()!;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cross-tree saga {OperationId}: retention window expired; clearing state.",
            OperationId);
        await state.ClearStateAsync();
    }

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        switch (state.State.Phase)
        {
            case CrossTreeTxPhase.Preparing:
            case CrossTreeTxPhase.Committed:
            case CrossTreeTxPhase.Aborted:
                try
                {
                    await RunCoordinatorAsync();
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "Cross-tree saga {OperationId} failed on reminder-driven resume.",
                        OperationId);
                }
                break;
            case CrossTreeTxPhase.Completed:
                // A crash between persisting Completed (FinalizePhaseAsync) and
                // arming retention lands here with the keepalive still
                // registered. Arm retention idempotently so the orphaned state
                // is eventually cleared, then unregister the keepalive and
                // deactivate.
                await UnregisterKeepaliveAsync();
                await SlideTtlAsync();
                this.DeactivateOnIdle();
                break;
            case CrossTreeTxPhase.NotStarted:
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
                break;
        }
    }

    /// <inheritdoc />
    public async Task<CrossTreeAtomicWriteOutcome> CommitAsync(List<LatticeTreeBatch> batches)
    {
        ArgumentNullException.ThrowIfNull(batches);

        // Fail-closed authorization of every leg up front, before any staging,
        // prepare, or memoized-outcome re-attach. Each batch's write keys are
        // authorized as Write and its tombstone-delete keys as Delete against the
        // ORIGINAL caller's identity (RequestContext propagates from the client
        // into this coordinator activation). A single denied leg throws and the
        // saga is never dispatched, so no participant tree is mutated. Zero-cost
        // and non-throwing under the default null gate / system-origin turn.
        await EnforceCrossTreeLegsAsync(batches);

        // Idempotent re-attach to an already-decided coordinator: return the
        // memoized verdict (or rethrow the original failure) without re-running.
        if (state.State.Phase == CrossTreeTxPhase.Completed)
        {
            return MemoizedOutcomeOrThrow();
        }

        if (state.State.Phase == CrossTreeTxPhase.NotStarted)
        {
            var participants = BuildParticipants(batches);

            // Empty cross-tree batch (no trees, or every tree empty): vacuous
            // commit. Nothing to stage, decide, or finalize.
            if (participants.Count == 0)
            {
                state.State.OperationId = OperationId;
                state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
                state.State.Phase = CrossTreeTxPhase.Completed;
                state.State.Outcome = CrossTreeAtomicWriteOutcome.Committed;
                await state.WriteStateAsync();
                // Arm retention so the persisted vacuous-commit state is
                // eventually cleared. This path never registered a keepalive,
                // so the TTL reminder is the only cleanup trigger.
                await SlideTtlAsync();
                return CrossTreeAtomicWriteOutcome.Committed;
            }

            state.State.OperationId = OperationId;
            state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
            state.State.Participants = participants;
            state.State.Fingerprint = ComputeFingerprint(participants);
            state.State.Phase = CrossTreeTxPhase.Preparing;
            await RegisterKeepaliveAsync();
            await state.WriteStateAsync();
        }
        else
        {
            // Re-submit of an in-flight coordinator: enforce tree-set / key-set
            // stability, mirroring the single-tree saga's idempotency contract.
            var incoming = ComputeFingerprint(BuildParticipants(batches));
            if (state.State.Fingerprint is not { } persisted
                || !CryptographicOperations.FixedTimeEquals(persisted, incoming))
            {
                throw new InvalidOperationException(
                    $"Cross-tree atomic-write operation '{OperationId}' was previously submitted with a " +
                    "different tree set or key set; reuse of an operationId requires the exact same trees and keys.");
            }
        }

        await RunCoordinatorAsync();
        return MemoizedOutcomeOrThrow();
    }

    /// <inheritdoc />
    public Task<TxStatus> GetDecisionAsync()
    {
        // The single global decision, read by every participating tree's
        // registry. Committed/Aborted are durable the instant the coordinator
        // persists the phase; Preparing/NotStarted resolve to InFlight so
        // delegated reads see the pre-saga view until the global flip.
        var status = state.State.Phase switch
        {
            CrossTreeTxPhase.Committed => TxStatus.Committed,
            CrossTreeTxPhase.Aborted => TxStatus.Aborted,
            CrossTreeTxPhase.Completed =>
                state.State.Outcome == CrossTreeAtomicWriteOutcome.Committed
                    ? TxStatus.Committed
                    : TxStatus.Aborted,
            _ => TxStatus.InFlight,
        };
        return Task.FromResult(status);
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(state.State.Phase is CrossTreeTxPhase.Completed or CrossTreeTxPhase.NotStarted);

    /// <summary>
    /// Resumable phase machine. Each branch is idempotent so a coordinator crash
    /// between any two steps is recovered by a re-issued call (direct or
    /// reminder-driven). Advances Preparing -&gt; Committed/Aborted -&gt;
    /// Completed.
    /// </summary>
    private async Task RunCoordinatorAsync()
    {
        if (state.State.Phase == CrossTreeTxPhase.Preparing)
        {
            await PreparePhaseAsync();
        }

        if (state.State.Phase is CrossTreeTxPhase.Committed or CrossTreeTxPhase.Aborted)
        {
            await FinalizePhaseAsync();
        }
    }

    /// <summary>
    /// Dispatches prepare-and-pause to every participant, collects votes, and
    /// records the single global decision (Committed iff every vote is
    /// Prepared, else Aborted). Idempotent: re-dispatched prepare returns the
    /// participant's already-recorded vote.
    /// </summary>
    private async Task PreparePhaseAsync()
    {
        var participants = state.State.Participants;
        // Canonical (ordinal-sorted, de-duplicated) participant tree-id set
        // stamped onto every sub-saga's terminal records so the
        // receiver-side cross-tree visibility barrier sees an identical
        // participant set on every terminal of this operation.
        var participantTreeIds = participants
            .Select(p => p.TreeId)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToArray();
        var voteTasks = new Task<CrossTreePrepareVote>[participants.Count];
        for (var i = 0; i < participants.Count; i++)
        {
            var p = participants[i];
            var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{p.TreeId}/{OperationId}");
            voteTasks[i] = saga.PrepareForCoordinatorAsync(
                p.TreeId, p.Entries, p.Predicate, OperationId, participantTreeIds, p.EntryDeltas, p.EntryDeletes);
        }

        CrossTreePrepareVote[] votes;
        try
        {
            votes = await Task.WhenAll(voteTasks);
        }
        catch (Exception ex)
        {
            // A participant threw rather than returning a vote (transient
            // routing/storage error). Leave the coordinator in Preparing so the
            // keepalive reminder retries the whole prepare phase idempotently.
            Logger.LogWarning(ex,
                "Cross-tree saga {OperationId} prepare dispatch faulted; will retry.",
                OperationId);
            throw;
        }

        var allPrepared = true;
        var anyFailed = false;
        for (var i = 0; i < votes.Length; i++)
        {
            participants[i].Vote = votes[i];
            if (votes[i] != CrossTreePrepareVote.Prepared) allPrepared = false;
            if (votes[i] == CrossTreePrepareVote.Failed) anyFailed = true;
        }

        if (allPrepared)
        {
            state.State.Phase = CrossTreeTxPhase.Committed;
            state.State.Outcome = CrossTreeAtomicWriteOutcome.Committed;
        }
        else
        {
            state.State.Phase = CrossTreeTxPhase.Aborted;
            state.State.Outcome = CrossTreeAtomicWriteOutcome.PreconditionFailed;
            state.State.FailureMessage = anyFailed
                ? $"Cross-tree atomic write '{OperationId}' aborted: a participating tree failed to prepare."
                : null;
        }

        await state.WriteStateAsync();
    }

    /// <summary>
    /// Fans out finalize to every prepared participant (commit when the global
    /// decision is Committed, abort otherwise), then marks the coordinator
    /// Completed and arms retention cleanup. Idempotent finalize tolerates a
    /// crash mid-fan-out.
    /// </summary>
    private async Task FinalizePhaseAsync()
    {
        var commit = state.State.Phase == CrossTreeTxPhase.Committed;
        var participants = state.State.Participants;

        var finalizeTasks = new List<Task>(participants.Count);
        foreach (var p in participants)
        {
            // Only participants that actually parked (voted Prepared) have a
            // staged sub-saga to finalize. A PreconditionFailed/Failed
            // participant already self-terminated; FinalizeAsync would be a
            // no-op but skipping avoids a needless round-trip.
            if (p.Vote != CrossTreePrepareVote.Prepared) continue;
            var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{p.TreeId}/{OperationId}");
            finalizeTasks.Add(saga.FinalizeAsync(commit));
        }

        await Task.WhenAll(finalizeTasks);

        state.State.Phase = CrossTreeTxPhase.Completed;
        await state.WriteStateAsync();

        EmitCompletionMetrics();

        await UnregisterKeepaliveAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Records the cross-tree completion counter, end-to-end duration, and
    /// participant-count histogram, all tagged by the terminal outcome.
    /// </summary>
    private void EmitCompletionMetrics()
    {
        var committed = state.State.Outcome == CrossTreeAtomicWriteOutcome.Committed;
        var outcomeTag = committed ? "committed" : "precondition_failed";
        var treeCount = state.State.Participants.Count;
        var elapsedMs = state.State.StartedAtTicks > 0
            ? Math.Max(0, (DateTime.UtcNow.Ticks - state.State.StartedAtTicks) / (double)TimeSpan.TicksPerMillisecond)
            : 0d;

        LatticeMetrics.CrossTreeAtomicWriteCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcomeTag),
            new KeyValuePair<string, object?>(LatticeMetrics.TagTreeCount, treeCount));
        LatticeMetrics.CrossTreeAtomicWriteDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcomeTag));
        LatticeMetrics.CrossTreeAtomicWriteParticipants.Record(treeCount,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcomeTag));
    }

    /// <summary>
    /// Returns the memoized terminal outcome, or rethrows the recorded failure
    /// for a saga that aborted on a genuine write failure (as opposed to a
    /// precondition miss).
    /// </summary>
    private CrossTreeAtomicWriteOutcome MemoizedOutcomeOrThrow()
    {
        if (state.State.FailureMessage is { } failure)
        {
            throw new InvalidOperationException(failure);
        }
        return state.State.Outcome ?? CrossTreeAtomicWriteOutcome.Committed;
    }

    private static readonly ILatticeAccessGate CrossTreeNullGateFallback = new NullLatticeAccessGate();

    /// <summary>
    /// Fail-closed per-leg authorization for a cross-tree atomic write. Resolves
    /// the caller subject once per participating tree and authorizes every write
    /// key (<see cref="LatticeOperation.Write"/>) and every tombstone-delete key
    /// (<see cref="LatticeOperation.Delete"/>) before the saga is dispatched, so a
    /// single denied leg aborts the whole cross-tree commit before any tree is
    /// mutated. Short-circuits with no allocation and no subject resolution under
    /// the default null gate or a system-origin turn.
    /// </summary>
    private async Task EnforceCrossTreeLegsAsync(List<LatticeTreeBatch> batches)
    {
        var gate = GrainContext.ActivationServices.GetService<ILatticeAccessGate>() ?? CrossTreeNullGateFallback;
        if (LatticeAccessGateEnforcement.SkipsEnforcement(gate))
        {
            return;
        }

        var membership = GrainContext.ActivationServices.GetService<ILatticeMembershipContext>();
        foreach (var batch in batches)
        {
            // Null / empty tree ids and null entry lists are rejected with a
            // precise ArgumentException by BuildParticipants, which runs before
            // any write; skip them here so enforcement never authorizes a
            // malformed leg.
            if (string.IsNullOrEmpty(batch.TreeId) || batch.Entries is null || batch.Entries.Count == 0)
            {
                continue;
            }

            List<string>? writeKeys = null;
            List<string>? deleteKeys = null;
            for (var i = 0; i < batch.Entries.Count; i++)
            {
                var key = batch.Entries[i].Key;
                if (key is null)
                {
                    continue;
                }

                var isDelete = batch.EntryDeletes is { } deletes && i < deletes.Count && deletes[i];
                if (isDelete)
                {
                    (deleteKeys ??= []).Add(key);
                }
                else
                {
                    (writeKeys ??= []).Add(key);
                }
            }

            if (writeKeys is not null)
            {
                await LatticeAccessGateEnforcement.EnforceManyPointsAsync(
                    gate, membership, batch.TreeId, LatticeOperation.Write, writeKeys, CancellationToken.None);
            }

            if (deleteKeys is not null)
            {
                await LatticeAccessGateEnforcement.EnforceManyPointsAsync(
                    gate, membership, batch.TreeId, LatticeOperation.Delete, deleteKeys, CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Defensively deep-copies the caller's batches into persistable
    /// participants: distinct, non-empty tree ids, cloned entry lists and value
    /// buffers, and a stable submission order (sorted by tree id) so the
    /// fingerprint is order-independent. Empty per-tree slices are dropped.
    /// </summary>
    private static List<CrossTreeParticipant> BuildParticipants(List<LatticeTreeBatch> batches)
    {
        var seen = new HashSet<string>(batches.Count, StringComparer.Ordinal);
        var result = new List<CrossTreeParticipant>(batches.Count);
        foreach (var batch in batches)
        {
            if (string.IsNullOrEmpty(batch.TreeId))
                throw new ArgumentException("Cross-tree batch contains a null or empty tree id.", nameof(batches));
            if (!seen.Add(batch.TreeId))
                throw new ArgumentException(
                    $"Cross-tree batch contains duplicate tree id '{batch.TreeId}'.", nameof(batches));
            if (batch.Entries is null)
                throw new ArgumentException(
                    $"Cross-tree batch for tree '{batch.TreeId}' has a null entries list.", nameof(batches));
            if (batch.Entries.Count == 0) continue;

            var entries = new List<KeyValuePair<string, byte[]>>(batch.Entries.Count);
            for (var i = 0; i < batch.Entries.Count; i++)
            {
                var (key, value) = batch.Entries[i];
                var isDelete = batch.EntryDeletes is { } d && i < d.Count && d[i];
                if (key is null)
                    throw new ArgumentException(
                        $"Cross-tree batch for tree '{batch.TreeId}' contains a null key.", nameof(batches));
                if (value is null && !isDelete)
                    throw new ArgumentException(
                        $"Cross-tree batch for tree '{batch.TreeId}' contains a null value for key '{key}'.",
                        nameof(batches));
                // A delete entry carries an empty (non-null) value buffer so it
                // rides the same prepared-write fan-out as the upserts; the
                // explicit per-entry delete channel (not value-nullness) is what
                // turns it into a tombstone at the leaf.
                var cloned = value is null ? Array.Empty<byte>() : (byte[])value.Clone();
                entries.Add(new KeyValuePair<string, byte[]>(key, cloned));
            }

            // Carry the optional per-entry author-delta list (flag-CRDT
            // membership rows) verbatim, aligned 1:1 with the cloned entries.
            // Defensively copied so the persisted participant never aliases the
            // caller's list/buffers. Null when the batch supplied no deltas
            // (every plain Set / SetWhere batch), which keeps a value-only
            // cross-tree write byte-identical to the pre-existing path.
            List<byte[]?>? entryDeltas = null;
            if (batch.EntryDeltas is { } sourceDeltas && sourceDeltas.Exists(static d => d is not null))
            {
                entryDeltas = new List<byte[]?>(entries.Count);
                for (var i = 0; i < entries.Count; i++)
                {
                    var delta = i < sourceDeltas.Count ? sourceDeltas[i] : null;
                    entryDeltas.Add(delta is null ? null : (byte[])delta.Clone());
                }
            }

            // Carry the optional per-entry delete (tombstone) channel verbatim,
            // aligned 1:1 with the cloned entries. Defensively copied; null when
            // the batch had no deletes (every plain upsert-only batch), which
            // keeps a value-only cross-tree write byte-identical.
            List<bool>? entryDeletes = null;
            if (batch.EntryDeletes is { } sourceDeletes && sourceDeletes.Exists(static d => d))
            {
                entryDeletes = new List<bool>(entries.Count);
                for (var i = 0; i < entries.Count; i++)
                {
                    entryDeletes.Add(i < sourceDeletes.Count && sourceDeletes[i]);
                }
            }

            result.Add(new CrossTreeParticipant
            {
                TreeId = batch.TreeId,
                Entries = entries,
                Predicate = batch.Predicate,
                EntryDeltas = entryDeltas,
                EntryDeletes = entryDeletes,
            });
        }

        result.Sort(static (a, b) => string.CompareOrdinal(a.TreeId, b.TreeId));
        return result;
    }

    /// <summary>
    /// Stable fingerprint over the participating tree set and each tree's sorted
    /// key set. A re-submit with a different tree-set or key-set is rejected.
    /// Values are intentionally excluded (the contract pins the addressed keys,
    /// not their payloads).
    /// </summary>
    private static byte[] ComputeFingerprint(List<CrossTreeParticipant> participants)
    {
        // Once-or-twice-per-saga, so not a hot path, but avoid the MemoryStream
        // growth + per-length BitConverter arrays + disposable hash object by
        // streaming directly into an IncrementalHash with a reusable 4-byte
        // little-endian length prefix.
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenPrefix = stackalloc byte[4];
        foreach (var p in participants)
        {
            AppendLengthPrefixed(hash, p.TreeId, lenPrefix);
            var keys = new string[p.Entries.Count];
            for (var i = 0; i < p.Entries.Count; i++) keys[i] = p.Entries[i].Key;
            Array.Sort(keys, StringComparer.Ordinal);
            BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, keys.Length);
            hash.AppendData(lenPrefix);
            foreach (var key in keys)
            {
                AppendLengthPrefixed(hash, key, lenPrefix);
            }
        }
        return hash.GetHashAndReset();
    }

    /// <summary>
    /// Appends a 4-byte little-endian length prefix followed by the UTF-8 bytes
    /// of <paramref name="value"/> to <paramref name="hash"/>, encoding through a
    /// stack buffer for short strings and renting from the array pool only for
    /// the rare long key.
    /// </summary>
    private static void AppendLengthPrefixed(IncrementalHash hash, string value, Span<byte> lenPrefix)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = maxBytes > 512 ? System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[512];
        var written = Encoding.UTF8.GetBytes(value, buffer);
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, written);
        hash.AppendData(lenPrefix);
        hash.AppendData(buffer[..written]);
        if (rented is not null)
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private async Task RegisterKeepaliveAsync()
    {
        try
        {
            await ReminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: GrainContext.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1));
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-tree saga {OperationId}: failed to register keepalive reminder (non-fatal).",
                OperationId);
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-tree saga {OperationId}: failed to unregister keepalive reminder (non-fatal).",
                OperationId);
        }
    }
}
