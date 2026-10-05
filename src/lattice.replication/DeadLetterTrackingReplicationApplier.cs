using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Decorator over the canonical <see cref="ReplicationApplier"/> that
/// tracks consecutive failed apply attempts for the same
/// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple in a
/// process-local <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// When the failure count reaches
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> the entry
/// is parked on the per-tree dead-letter queue and a non-applied
/// <see cref="ApplyResult"/> is returned to the caller. For point writes only
/// (not range deletes or saga terminals), the per-origin high-water-mark is also
/// advanced to at least the entry timestamp. That advance does not make a later
/// re-delivery a no-op - the canonical applier has no point-write HLC drop
/// threshold - but the
/// transport does not normally re-deliver a parked entry, because the
/// non-deferred not-applied result is acknowledged and the sender moves past it.
/// A successful apply clears the counter for
/// that tuple so later transient failures get a fresh budget.
/// <para>
/// <b>Saga records are never parked alone (#4591).</b> A prepare or a
/// <see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>
/// terminal that exhausts the budget is not parked: parking acknowledges it, so
/// the sender's terminal hold would count a parked prepare as delivered and the
/// receiver would commit the saga without that key, and a parked terminal would
/// strand the saga's buckets. Such a record returns a
/// <see cref="ApplyResult.Deferred"/> result instead - a not-accepted,
/// cursor-preserving ack - so the sender keeps and re-ships it, and on the
/// per-entry path no later record of the same saga in the batch is applied. The
/// stream from that
/// origin for that tree waits until the failure clears; each deferral counts on
/// <see cref="LatticeReplicationMetrics.SagaApplyDeferred"/>.
/// </para>
/// <para>
/// The retry counter is intentionally in-memory: the decorator is
/// registered as a singleton, so all apply paths share the same
/// counter within a silo. A silo restart resets the counters,
/// effectively giving every entry another <c>MaxApplyRetries</c>
/// attempts after a failover - this is the desired behaviour because
/// silo restart usually correlates with the very transient failure
/// the retry budget is meant to absorb.
/// </para>
/// <para>
/// <b>Inbound contact.</b> The canonical applier records the inbound per-peer
/// contact in <see cref="ReplicationPeerStats"/> on its batch entry point only. The
/// branches of <see cref="ApplyBatchAsync"/> that apply entries one at a time -
/// the single-entry fast path (every one-entry push from a low-rate sender) and
/// the per-entry slow path - bypass it, so this decorator records the contact
/// itself on exactly those branches, through the same
/// <see cref="ReplicationInboundContact"/> rule. The batch fast path is recorded
/// by the inner applier; if that batch path throws and this decorator falls back
/// to per-entry applies, entries from runs the inner path already attempted can
/// record contact a second time for the same push. On every branch the contact is
/// recorded only for a run the receiver admits - its tree enrolled here and its wire
/// mode matching - re-resolved through the same <c>replicationContext</c>
/// the canonical applier's gate consults, so a peer cannot plant a non-enrolled
/// tree id in the peer statistics (issue #4021).
/// </para>
/// </summary>
internal sealed class DeadLetterTrackingReplicationApplier(
    IReplicationApplier inner,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILogger<DeadLetterTrackingReplicationApplier> logger,
    ReplicationPeerStats? peerStats = null,
    ILatticeReplicationContext? replicationContext = null) : IReplicationApplier
{
    private readonly ConcurrentDictionary<RetryKey, int> _failures = new();
    private readonly ConcurrentDictionary<RetryKey, DateTime> _firstDeferrals = new();

    /// <inheritdoc />
    public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        => ApplyWithPoisonFilterAsync(entry, recordContact: false, cancellationToken);

    private async Task<ApplyResult> ApplyWithPoisonFilterAsync(
        WalRecord entry,
        bool recordContact,
        CancellationToken cancellationToken)
    {
        var (filtered, withheld, quarantined) = await FilterPoisonedAsync([entry], cancellationToken).ConfigureAwait(false);
        if (quarantined is not null)
        {
            // A terminal of a quarantined saga (#4692) is parked, not applied.
            return await ParkQuarantinedAsync(quarantined, cancellationToken).ConfigureAwait(false)
                ? new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero }
                : new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
        }

        if (withheld)
        {
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
        }

        return await ApplyTrackedAsync(filtered[0], recordContact, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Applies one entry under the retry-budget accounting. When
    /// <paramref name="recordContact"/> is set - on the branches of
    /// <see cref="ApplyBatchAsync"/> that apply entries one at a time, bypassing
    /// the inner batch entry point that would otherwise record it - the inbound
    /// per-peer contact is recorded too: a failure when the inner apply throws
    /// (whether the entry is then retried or parked), a success otherwise. A
    /// cancellation is not a contact attempt and records nothing.
    /// </summary>
    private async Task<ApplyResult> ApplyTrackedAsync(
        WalRecord entry,
        bool recordContact,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        ApplyResult result;
        try
        {
            result = await inner.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation is not a poison-entry signal - surface it
            // to the caller without touching the failure counter.
            throw;
        }
        catch (Exception ex)
        {
            if (recordContact)
            {
                ReplicationInboundContact.Record(peerStats, options, replicationContext, entry, success: false);
            }

            return await OnFailureAsync(entry, ex, cancellationToken).ConfigureAwait(false);
        }

        // Successful apply (or filtered re-delivery) clears any
        // accumulated failure state for the tuple.
        var key = KeyFor(entry);
        _failures.TryRemove(key, out _);
        _firstDeferrals.TryRemove(key, out _);
        if (recordContact)
        {
            ReplicationInboundContact.Record(peerStats, options, replicationContext, entry, success: true);
        }

        return result;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Steady-state fast path: when no entry in the batch has any
    /// recorded retry history we delegate the entire batch to the
    /// inner applier's optimised batch-mode implementation, which
    /// collapses the per-entry HWM round-trips to one
    /// <see cref="IReplicationHighWaterMarkGrain.GetAsync"/> + one
    /// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>
    /// per distinct origin per batch. A successful return clears any
    /// per-entry failure counters that may have accumulated for the
    /// applied entries.
    /// <para>
    /// Slow path: when at least one entry already has accumulated
    /// failure history, OR when the inner batch call throws (a poison
    /// entry mid-batch), we fall back to the per-entry decorator path
    /// so retry budgets and dead-letter parking continue to apply
    /// per-entry. Inner-batch exceptions on a clean batch fall through
    /// to per-entry retries that re-establish the correct retry-budget
    /// accounting on the offending entry.
    /// </para>
    /// </remarks>
    public async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0)
        {
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        var (unpoisonedEntries, withheld, quarantined) = await FilterPoisonedAsync(entries, cancellationToken).ConfigureAwait(false);
        if (quarantined is not null && !await ParkQuarantinedAsync(quarantined, cancellationToken).ConfigureAwait(false))
        {
            // The dead-letter queue is full (#4603): keep the push unacknowledged.
            withheld = true;
        }

        if (unpoisonedEntries.Count == 0 && !withheld)
        {
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        if (withheld)
        {
            // A record of a poisoned saga is withheld until the re-seed retires
            // the poison, so the whole push is deferred: the sender re-ships it,
            // and the other records re-apply idempotently then.
            if (unpoisonedEntries.Count > 0)
            {
                var partial = await ApplyBatchCoreAsync(unpoisonedEntries, cancellationToken).ConfigureAwait(false);
                return partial with { Deferred = true };
            }

            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
        }

        return await ApplyBatchCoreAsync(unpoisonedEntries, cancellationToken).ConfigureAwait(false);
    }

    private async Task<ApplyResult> ApplyBatchCoreAsync(
        IReadOnlyList<WalRecord> unpoisonedEntries,
        CancellationToken cancellationToken)
    {

        // Single-entry fast path: defer to the per-entry decorator so
        // there is exactly one retry-budget code path on the hot path
        // for low-rate (single-entry per push) deployments.
        if (unpoisonedEntries.Count == 1)
        {
            return await ApplyTrackedAsync(unpoisonedEntries[0], recordContact: true, cancellationToken).ConfigureAwait(false);
        }

        // Steady-state heuristic: if no entry has any prior failure
        // history we route the batch through the inner applier's
        // optimised batch path. The check is O(n) over the dictionary
        // count (~zero in steady state) so this is cheap.
        var hasHistory = false;
        if (!_failures.IsEmpty)
        {
            for (var i = 0; i < unpoisonedEntries.Count; i++)
            {
                if (_failures.ContainsKey(KeyFor(unpoisonedEntries[i])))
                {
                    hasHistory = true;
                    break;
                }
            }
        }

        if (!hasHistory)
        {
            try
            {
                var result = await inner.ApplyBatchAsync(unpoisonedEntries, cancellationToken).ConfigureAwait(false);
                for (var i = 0; i < unpoisonedEntries.Count; i++)
                {
                    var key = KeyFor(unpoisonedEntries[i]);
                    _failures.TryRemove(key, out _);
                    _firstDeferrals.TryRemove(key, out _);
                }

                return result;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // Fall through to per-entry slow path so the retry-budget
                // accounting kicks in for whichever entry caused the
                // throw (the inner applier surfaces the exception
                // partway through the batch; the per-entry retry path
                // re-establishes correct accounting for the offending
                // tuple).
            }
        }

        // Slow path: per-entry through the decorator's own ApplyAsync,
        // preserving retry-budget accounting and dead-letter parking
        // semantics for every entry in the batch.
        var applied = false;
        var highest = HybridLogicalClock.Zero;
        var anyDeferred = false;
        var anyLineageRefused = false;
        HashSet<Guid>? deferredSagas = null;
        for (var i = 0; i < unpoisonedEntries.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (deferredSagas is not null
                && unpoisonedEntries[i].TransactionId != Guid.Empty
                && deferredSagas.Contains(unpoisonedEntries[i].TransactionId))
            {
                // Nothing of a saga is applied behind its deferred record
                // (#4591): a terminal later in the batch would otherwise commit
                // the saga without the deferred prepare. The not-accepted ack
                // re-ships the batch, so the skipped record is delivered again.
                continue;
            }

            var result = await ApplyTrackedAsync(unpoisonedEntries[i], recordContact: true, cancellationToken).ConfigureAwait(false);
            if (result.Applied)
            {
                applied = true;
            }
            if (result.Deferred)
            {
                anyDeferred = true;
                if (IsSagaRecord(unpoisonedEntries[i]))
                {
                    (deferredSagas ??= new HashSet<Guid>()).Add(unpoisonedEntries[i].TransactionId);
                }
            }
            if (result.SourceLineageRefused)
            {
                anyLineageRefused = true;
            }
            if (result.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = result.HighWaterMark;
            }
        }
        return new ApplyResult { Applied = applied, HighWaterMark = highest, Deferred = anyDeferred, SourceLineageRefused = anyLineageRefused };
    }

    /// <summary>
    /// Whether <paramref name="entry"/> belongs to a saga - a prepare, or a
    /// <see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>
    /// terminal - and so must never be parked alone.
    /// </summary>
    internal static bool IsSagaRecord(in WalRecord entry) =>
        entry.TransactionId != Guid.Empty
        && (entry.IsPrepared || entry.Op is MutationKind.TxCommit or MutationKind.TxAbort);

    private async Task<ApplyResult> OnFailureAsync(
        WalRecord entry,
        Exception failure,
        CancellationToken cancellationToken)
    {
        var key = KeyFor(entry);

        // Fail-safe backstop: a structurally-invalid entry with an empty or
        // whitespace TreeId cannot be applied to any tree (the canonical
        // applier rejects it up front) and cannot be quarantined either -
        // both the per-tree dead-letter grain and the per-origin
        // high-water-mark grain are keyed on the tree id, so GetGrain would
        // throw ArgumentException on the empty key before the entry is
        // parked or the cursor advanced. Left unguarded, that turns a single
        // malformed entry into a permanent convergence wedge and an
        // unbounded re-ship/error-log loop (the retry counter never clears
        // and the HWM never advances). Well-formed producers never emit an
        // empty TreeId - the leaf/bootstrap re-replay sink re-stamps it from
        // the batch tree name, and the framing wire path re-stamps it on
        // decode - so this contains a malformed inbound entry rather than
        // masking a routine one. Contain it: record the dead-letter metric,
        // clear the counter, and return a non-applied result so the batch is
        // not wedged and the quarantine path never crashes.
        if (string.IsNullOrWhiteSpace(entry.TreeId))
        {
            logger.LogError(
                failure,
                "Replication received a structurally-invalid entry with an empty TreeId (origin {Origin}, key '{Key}', op {Op}); "
                + "it cannot be applied or quarantined per-tree and has been dropped. This indicates a producer that shipped an "
                + "entry without re-stamping its tree id.",
                entry.OriginClusterId ?? "<none>",
                entry.Key ?? string.Empty,
                entry.Op);

            LatticeReplicationMetrics.DeadLetterEnqueued.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, string.Empty),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonSchema),
                LatticeTenantLabel.ForTree(string.Empty));

            _failures.TryRemove(key, out _);
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        var attempts = _failures.AddOrUpdate(key, 1, static (_, current) => current + 1);

        var max = options.Get(entry.TreeId).MaxApplyRetries;
        if (attempts < max)
        {
            // Below the threshold - surface the original failure to
            // the caller so the transport can apply its own
            // backoff/redelivery policy on top of our local counter.
            throw failure;
        }

        if (IsSagaRecord(entry))
        {
            // A saga prepare or terminal is never parked alone (#4591). Parking
            // acknowledges it, so the sender's terminal hold would count a parked
            // prepare as delivered and release the saga's terminal, and this
            // receiver would commit the saga without that key; a parked terminal
            // would strand every bucket of the saga. Defer it instead: the
            // receive path answers with a not-accepted, cursor-preserving ack, the
            // sender keeps the record and re-ships it, and the stream from this
            // origin for this tree waits for it until the failure clears. The
            // counter is kept, so every later attempt defers again at once.
            if (attempts == max)
            {
                logger.LogError(
                    failure,
                    "Replication cannot apply saga {Op} for transaction {TransactionId} (tree '{TreeId}', origin {Origin}, key '{Key}') "
                    + "after {Attempts} attempts. It is deferred, not dead-lettered, so the saga is never served torn: the sender re-ships it "
                    + "and the stream from that origin waits until the failure clears. Fix the cause or re-bootstrap the tree from the origin.",
                    entry.Op, entry.TransactionId, entry.TreeId, entry.OriginClusterId ?? "<none>", entry.Key ?? string.Empty, attempts);
            }

            LatticeReplicationMetrics.SagaApplyDeferred.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, entry.OriginClusterId ?? string.Empty),
                LatticeTenantLabel.ForTree(entry.TreeId));

            var quarantine = await TryQuarantineAsync(entry, key, failure, attempts).ConfigureAwait(false);
            if (quarantine == QuarantineVerdict.HeldFull)
            {
                // Issue #4692: the saga must be quarantined but the bounded set is
                // full. Poisoning it again would re-seed it for ever, so the record
                // stays deferred, fail-closed: the stream from this origin for this
                // tree waits until an operator releases a quarantine.
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
            }

            if (quarantine == QuarantineVerdict.Quarantined)
            {
                // Issue #4692: the saga is quarantined. Its record is parked, not
                // applied, so the stream moves past it, and the saga is never
                // re-seeded again for this cause.
                if (!await ParkPoisonedSagaRecordAsync(
                    entry,
                    failure.Message ?? "<no message>",
                    attempts,
                    failure,
                    cancellationToken).ConfigureAwait(false))
                {
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
                }

                _failures.TryRemove(key, out _);
                _firstDeferrals.TryRemove(key, out _);
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            if (await TryPoisonTimedOutSagaRecordAsync(entry, key, failure, attempts, cancellationToken).ConfigureAwait(false))
            {
                if (!entry.IsPrepared)
                {
                    // Issue #4692: a terminal that keeps failing - a malformed
                    // record, a decision conflict, a cross-tree barrier it cannot
                    // join - gets the bound a prepare has. Its saga is poisoned and
                    // a re-seed settles it from the export; the terminal itself is
                    // withheld, never parked: parking would acknowledge it, and the
                    // terminal of a saga still in flight at the export would then
                    // never arrive. Every later copy is withheld by the poison
                    // filter until the re-seed retires the poison.
                    _failures.TryRemove(key, out _);
                    _firstDeferrals.TryRemove(key, out _);
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
                }

                if (!await ParkPoisonedSagaRecordAsync(
                    entry,
                    failure.Message ?? "<no message>",
                    attempts,
                    failure,
                    cancellationToken).ConfigureAwait(false))
                {
                    // The queue is full (#4603): the poison stands, but the prepare
                    // stays unacknowledged until a re-delivery can park it.
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
                }

                _failures.TryRemove(key, out _);
                _firstDeferrals.TryRemove(key, out _);
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
        }

        // Threshold reached: park the entry, advance the HWM past it (a
        // progress frontier only - the canonical applier does not dedupe on
        // it, so a re-delivered copy would be applied afresh; the non-deferred
        // Applied=false returned below is what lets the sender move past the
        // entry), and clear the counter so a future
        // entry against the same tuple gets a fresh budget.
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        var reasonTag = ClassifyFailure(failure);
        try
        {
            await dlq.EnqueueAsync(entry, failure.Message ?? "<no message>", attempts, reasonTag, cancellationToken, ReplicationSourceLineageScope.Current).ConfigureAwait(false);
            peerStats?.RecordDeadLetterFull(entry.TreeId, entry.OriginClusterId ?? string.Empty, ReplicationContactDirection.Inbound, since: null);
        }
        catch (ReplicationDeadLetterQueueFullException)
        {
            // Surface the stall on the peer-status path rather than as a quiet link.
            peerStats?.RecordDeadLetterFull(entry.TreeId, entry.OriginClusterId ?? string.Empty, ReplicationContactDirection.Inbound, DateTimeOffset.UtcNow);

            // The dead-letter queue is full (#4603). Parking is the only thing
            // that keeps an acknowledged entry, so do not acknowledge it: defer
            // (a not-accepted, cursor-preserving ack) so the sender keeps it and
            // re-ships, leave the high-water mark alone, and keep the retry count
            // so the re-delivery tries to park again straight away.
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true };
        }

        // Advance HWM only for point-applied entries; range deletes do
        // not consult the HWM (see ReplicationApplier) so advancing it
        // would be misleading. Saga terminal-mark records (TxCommit /
        // TxAbort) are likewise routed by the canonical applier
        // through ApplyTxTerminalCoreAsync, which deliberately
        // bypasses the per-origin HWM check: saga terminal HLCs are
        // saga linearization points, not per-origin frontiers, and
        // the receiver dedupes terminals through the per-tree
        // TxRegistry instead. Advancing HWM past a parked terminal
        // would silently dedupe any in-flight retry of a legitimate
        // point mutation from the same origin carrying an HLC at or
        // below the terminal's HLC (silent data loss on the next
        // dedup-eligible same-origin entry). Local-origin entries
        // cannot reach this path because the canonical applier
        // returns Applied=false synchronously without throwing.
        if (entry.Op != MutationKind.DeleteRange
            && entry.Op != MutationKind.TxCommit
            && entry.Op != MutationKind.TxAbort)
        {
            var hwm = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(entry.TreeId);
            await hwm.TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, cancellationToken).ConfigureAwait(false);
        }

        _failures.TryRemove(key, out _);
        return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
    }

    /// <summary>
    /// Removes every terminal of a saga this receiver has poisoned for the
    /// record's origin. Such a terminal is withheld, not applied and not parked:
    /// the caller defers the push, so the sender keeps it and re-ships it after
    /// the re-seed the poison triggered retires the poison. Applying it would
    /// commit the saga without the poisoned prepare; parking it would acknowledge
    /// it, and the terminal of a saga still in flight at the re-seed's export
    /// would then never arrive, stranding the buckets the re-seed restaged. A
    /// later prepare of a poisoned saga is applied as usual (it only stages): the
    /// re-seed keeps it for a saga still in flight and discards it for a decided
    /// one, and if it cannot be applied it is parked at once. Returns the records
    /// to apply and whether any was withheld.
    /// </summary>
    private async Task<(IReadOnlyList<WalRecord> Entries, bool Withheld, List<WalRecord>? Quarantined)> FilterPoisonedAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken)
    {
        if (LatticeBootstrapApplyContext.IsActive)
        {
            return (entries, false, null);
        }

        Dictionary<(string TreeId, string Origin), HashSet<Guid>>? byTreeOrigin = null;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Op is not (MutationKind.TxCommit or MutationKind.TxAbort)
                || entry.TransactionId == Guid.Empty
                || string.IsNullOrWhiteSpace(entry.TreeId))
            {
                continue;
            }

            var key = (entry.TreeId, entry.OriginClusterId ?? string.Empty);
            byTreeOrigin ??= new Dictionary<(string TreeId, string Origin), HashSet<Guid>>();
            if (byTreeOrigin.TryGetValue(key, out var set))
            {
                set.Add(entry.TransactionId);
            }
            else
            {
                byTreeOrigin[key] = [entry.TransactionId];
            }
        }

        if (byTreeOrigin is null)
        {
            return (entries, false, null);
        }

        Dictionary<(string TreeId, string Origin), HashSet<Guid>>? poisoned = null;
        Dictionary<(string TreeId, string Origin), HashSet<Guid>>? quarantinedSagas = null;
        foreach (var kvp in byTreeOrigin)
        {
            var classified = await grainFactory
                .GetGrain<IReceiverSagaPoisonGrain>(kvp.Key.TreeId)
                .ClassifyAsync(kvp.Key.Origin, kvp.Value)
                .ConfigureAwait(false);
            if (classified.Poisoned.Count > 0)
            {
                (poisoned ??= new Dictionary<(string TreeId, string Origin), HashSet<Guid>>())[kvp.Key] = new HashSet<Guid>(classified.Poisoned);
            }

            if (classified.Quarantined.Count > 0)
            {
                (quarantinedSagas ??= new Dictionary<(string TreeId, string Origin), HashSet<Guid>>())[kvp.Key] = new HashSet<Guid>(classified.Quarantined);
            }
        }

        if (poisoned is null && quarantinedSagas is null)
        {
            return (entries, false, null);
        }

        var unpoisoned = new List<WalRecord>(entries.Count);
        var withheld = false;
        List<WalRecord>? quarantined = null;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var treeId = entry.TreeId ?? string.Empty;
            var origin = entry.OriginClusterId ?? string.Empty;
            if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
                && quarantinedSagas is not null
                && quarantinedSagas.TryGetValue((treeId, origin), out var held)
                && held.Contains(entry.TransactionId))
            {
                (quarantined ??= new List<WalRecord>()).Add(entry);
                continue;
            }

            if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
                && poisoned is not null
                && poisoned.TryGetValue((treeId, origin), out var set)
                && set.Contains(entry.TransactionId))
            {
                withheld = true;
                LatticeReplicationMetrics.SagaApplyDeferred.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, origin),
                    LatticeTenantLabel.ForTree(treeId));
                continue;
            }

            unpoisoned.Add(entry);
        }

        return (unpoisoned, withheld, quarantined);
    }

    /// <summary>
    /// Parks every record of a quarantined saga (issue #4692) without applying
    /// it, so the stream moves past it. Returns <see langword="false"/> when the
    /// dead-letter queue is full (#4603): the caller then keeps the push
    /// unacknowledged.
    /// </summary>
    private async Task<bool> ParkQuarantinedAsync(List<WalRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (!await ParkPoisonedSagaRecordAsync(
                    record,
                    "The saga is quarantined: its record failed again after a re-seed settled it.",
                    retryCount: 0,
                    failure: null,
                    cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether the saga of a record that keeps failing is, or must now be,
    /// quarantined (issue #4692). A saga already quarantined is quarantined at
    /// once. Otherwise, past <see cref="LatticeReplicationOptions.SagaDeferralTimeout"/>,
    /// a saga whose poison a completed re-seed has already retired is quarantined
    /// rather than poisoned again: its record failed for a reason the re-seed could
    /// not remove (a malformed record, a contradictory decision, a misconfigured
    /// cluster id), so another re-seed would only repeat the cycle and the stream
    /// would never move past it. Quarantine is an input-integrity fault: the saga
    /// is withheld whole on this receiver, so all-or-nothing visibility holds, but
    /// its liveness is given up, and it is counted and logged for the operator.
    /// When the bounded quarantine set is full the verdict is
    /// <see cref="QuarantineVerdict.HeldFull"/>: the record is held unacknowledged,
    /// and the saga is never poisoned and re-seeded again, which would restart
    /// the cycle quarantine exists to end.
    /// </summary>
    private async Task<QuarantineVerdict> TryQuarantineAsync(WalRecord entry, RetryKey key, Exception failure, int attempts)
    {
        var origin = entry.OriginClusterId ?? string.Empty;
        var poison = grainFactory.GetGrain<IReceiverSagaPoisonGrain>(entry.TreeId);
        var classified = await poison.ClassifyAsync(origin, new[] { entry.TransactionId }).ConfigureAwait(false);
        if (classified.Quarantined.Count > 0)
        {
            return QuarantineVerdict.Quarantined;
        }

        if (classified.Poisoned.Count > 0)
        {
            return QuarantineVerdict.None;
        }

        var now = DateTime.UtcNow;
        var first = _firstDeferrals.GetOrAdd(key, now);
        if (now - first < options.Get(entry.TreeId).SagaDeferralTimeout
            || !await poison.IsRetiredAsync(origin, entry.TransactionId).ConfigureAwait(false))
        {
            return QuarantineVerdict.None;
        }

        if (!await poison.QuarantineAsync(origin, entry.TransactionId, failure.Message ?? "<no message>").ConfigureAwait(false))
        {
            RecordReceiverSagaPoisoned(entry.TreeId, origin, LatticeReplicationMetrics.OutcomeReceiverSagaQuarantineFull);
            logger.LogError(
                failure,
                "Receiver could not quarantine saga transaction {TransactionId} (tree '{TreeId}', origin {Origin}) because the "
                + "quarantine set is full. The record is held unacknowledged, so the stream from that origin for that tree waits; "
                + "the saga is not re-seeded again. Release resolved quarantines with "
                + "ILatticeReplicationDeadLetters.ReleaseQuarantinedSagaAsync to free capacity.",
                entry.TransactionId, entry.TreeId, origin);
            return QuarantineVerdict.HeldFull;
        }

        RecordReceiverSagaPoisoned(entry.TreeId, origin, LatticeReplicationMetrics.OutcomeReceiverSagaQuarantined);
        logger.LogError(
            failure,
            "Input-integrity fault: saga transaction {TransactionId} from origin {Origin} on tree '{TreeId}' (cross-tree operation "
            + "'{CrossTreeOperationId}') failed again after {Attempts} attempts although a re-seed already settled it, so the "
            + "re-seed cannot remove the cause. The saga is QUARANTINED: its records are parked in the dead-letter queue without "
            + "being applied, the stream moves past them, and it is not re-seeded again. Fix the cause (a malformed record, a "
            + "contradictory decision, or a misconfigured ClusterId), discard the parked records, then release the quarantine with "
            + "ILatticeReplicationDeadLetters.ReleaseQuarantinedSagaAsync.",
            entry.TransactionId, origin, entry.TreeId, entry.CrossTreeOperationId ?? string.Empty, attempts);
        return QuarantineVerdict.Quarantined;
    }

    /// <summary>The outcome of <see cref="TryQuarantineAsync"/> (issue #4692).</summary>
    private enum QuarantineVerdict
    {
        /// <summary>The saga is not quarantined; the poison bound applies as usual.</summary>
        None,

        /// <summary>The saga is quarantined: park the record and move past it.</summary>
        Quarantined,

        /// <summary>The saga must be quarantined but the set is full: hold the record, never re-seed.</summary>
        HeldFull,
    }

    /// <summary>
    /// Poisons the saga of a prepare or terminal that has kept failing past
    /// <see cref="LatticeReplicationOptions.SagaDeferralTimeout"/>, and starts a
    /// re-seed (or marks one owed). Returns whether the saga is poisoned. A
    /// prepare is refused once the receiver registry has decided its saga (its
    /// terminal can still apply); a terminal is not, because a terminal that
    /// keeps failing against a decided registry - a recorded opposite decision -
    /// is exactly the wedge the bound exists for (issue #4692).
    /// </summary>
    private async Task<bool> TryPoisonTimedOutSagaRecordAsync(
        WalRecord entry,
        RetryKey key,
        Exception failure,
        int attempts,
        CancellationToken cancellationToken)
    {
        var origin = entry.OriginClusterId ?? string.Empty;
        var poison = grainFactory.GetGrain<IReceiverSagaPoisonGrain>(entry.TreeId);

        // A failing prepare of a saga already poisoned - by an operator, or by
        // this record's timeout on another silo - is parked at once, so the
        // operator escape resumes the link without waiting out the bound here.
        var alreadyPoisoned = await poison
            .FilterPoisonedAsync(origin, new[] { entry.TransactionId })
            .ConfigureAwait(false);
        if (alreadyPoisoned.Count > 0)
        {
            return true;
        }

        var now = DateTime.UtcNow;
        var first = _firstDeferrals.GetOrAdd(key, now);
        var timeout = options.Get(entry.TreeId).SagaDeferralTimeout;
        if (now - first < timeout)
        {
            return false;
        }

        var isPrepare = entry.IsPrepared;
        var status = isPrepare
            ? await TxRegistryRouting.GetRegistry(grainFactory, entry.TreeId, entry.TransactionId)
                .GetRecordedStatusAsync(entry.TransactionId)
                .ConfigureAwait(false)
            : TxStatus.InFlight;
        if (status != TxStatus.InFlight)
        {
            RecordReceiverSagaPoisoned(entry.TreeId, origin, LatticeReplicationMetrics.OutcomeReceiverSagaPoisonRefusedDecided);
            logger.LogError(
                failure,
                "Receiver refused to poison saga prepare for transaction {TransactionId} (tree '{TreeId}', origin {Origin}, key '{Key}') "
                + "after {Attempts} attempts because the receiver registry already recorded status {Status}; the record remains deferred.",
                entry.TransactionId,
                entry.TreeId,
                origin,
                entry.Key ?? string.Empty,
                attempts,
                status);
            return false;
        }

        var poisoned = await poison
            .PoisonAsync(
                origin,
                entry.TransactionId,
                isPrepare
                    ? "Deferred prepare exceeded the receiver saga deferral timeout."
                    : "Deferred terminal exceeded the receiver saga deferral timeout.")
            .ConfigureAwait(false);
        if (!poisoned)
        {
            RecordReceiverSagaPoisoned(entry.TreeId, origin, LatticeReplicationMetrics.OutcomeReceiverSagaPoisonRefusedFull);
            logger.LogError(
                failure,
                "Receiver refused to poison saga {Op} for transaction {TransactionId} (tree '{TreeId}', origin {Origin}, key '{Key}') "
                + "after {Attempts} attempts because the receiver poison set is full; the record remains deferred.",
                entry.Op,
                entry.TransactionId,
                entry.TreeId,
                origin,
                entry.Key ?? string.Empty,
                attempts);
            return false;
        }

        RecordReceiverSagaPoisoned(
            entry.TreeId,
            origin,
            isPrepare
                ? LatticeReplicationMetrics.OutcomeReceiverSagaPoisonedTimeout
                : LatticeReplicationMetrics.OutcomeReceiverSagaPoisonedTerminalTimeout);
        LogPoisonedSaga(
            entry,
            failure,
            isPrepare
                ? "deferred prepare exceeded the receiver saga deferral timeout"
                : "deferred terminal exceeded the receiver saga deferral timeout; it is withheld until the re-seed settles the saga");
        _ = ReceiverSagaPoisonReseed.TryStartOrMarkOwedAsync(
            grainFactory,
            options,
            entry.TreeId,
            origin,
            logger,
            CancellationToken.None);
        return true;
    }

    /// <summary>
    /// Parks a record of a receiver-poisoned saga. Returns <see langword="false"/>
    /// when the dead-letter queue is full (#4603), in which case the caller must
    /// keep the record unacknowledged.
    /// </summary>
    private async Task<bool> ParkPoisonedSagaRecordAsync(
        WalRecord entry,
        string failureReason,
        int retryCount,
        Exception? failure,
        CancellationToken cancellationToken)
    {
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        try
        {
            await dlq.EnqueueAsync(
                entry,
                failureReason,
                retryCount,
                LatticeReplicationMetrics.ReasonPoisonedSaga,
                cancellationToken,
                ReplicationSourceLineageScope.Current).ConfigureAwait(false);
            peerStats?.RecordDeadLetterFull(entry.TreeId, entry.OriginClusterId ?? string.Empty, ReplicationContactDirection.Inbound, since: null);
        }
        catch (ReplicationDeadLetterQueueFullException)
        {
            peerStats?.RecordDeadLetterFull(entry.TreeId, entry.OriginClusterId ?? string.Empty, ReplicationContactDirection.Inbound, DateTimeOffset.UtcNow);
            return false;
        }

        LogPoisonedSaga(entry, failure, "record is part of a receiver-poisoned saga and was parked");
        return true;
    }

    internal static void RecordReceiverSagaPoisoned(string treeId, string originClusterId, string outcome)
    {
        LatticeReplicationMetrics.ReceiverSagaPoisoned.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, originClusterId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome),
            LatticeTenantLabel.ForTree(treeId));
    }

    private void LogPoisonedSaga(WalRecord entry, Exception? failure, string action)
    {
        if (string.IsNullOrEmpty(entry.CrossTreeOperationId))
        {
            logger.LogError(
                failure,
                "Receiver poisoned saga transaction {TransactionId} from origin {Origin} on tree '{TreeId}': {Action}.",
                entry.TransactionId,
                entry.OriginClusterId ?? string.Empty,
                entry.TreeId,
                action);
            return;
        }

        logger.LogError(
            failure,
            "Receiver poisoned saga transaction {TransactionId} from origin {Origin} on tree '{TreeId}' for cross-tree operation {CrossTreeOperationId}: {Action}. "
            + "The cross-tree receiver barrier for that operation will stay incomplete until the tree is re-bootstrapped from the origin.",
            entry.TransactionId,
            entry.OriginClusterId ?? string.Empty,
            entry.TreeId,
            entry.CrossTreeOperationId,
            action);
    }

    private static RetryKey KeyFor(WalRecord entry) =>
        new(
            entry.TreeId ?? string.Empty,
            entry.OriginClusterId ?? string.Empty,
            entry.Timestamp,
            entry.Key ?? string.Empty,
            entry.Op);

    /// <summary>
    /// Composite key identifying a single replicated entry across retry
    /// attempts. Equality is structural so the dictionary collapses
    /// repeated apply attempts of the same logical entry onto the same
    /// counter.
    /// </summary>
    private readonly record struct RetryKey(
        string TreeId,
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        MutationKind Op);

    /// <summary>
    /// Classifies the terminal apply failure into a stable
    /// <c>reason</c> tag value for the
    /// <c>orleans.lattice.replication.dead_letter.enqueued</c>
    /// counter. The mapping is intentionally conservative: only
    /// failure shapes the canonical <see cref="ReplicationApplier"/>
    /// (or another decorator under our control) is known to emit are
    /// matched explicitly; everything else lands on
    /// <see cref="LatticeReplicationMetrics.ReasonUnknown"/> rather
    /// than guessing from message-text patterns.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    ///   <item>
    ///     <see cref="ArgumentException"/> - surfaced by
    ///     <see cref="ReplicationApplier"/> for malformed entries
    ///     (null <see cref="WalRecord.Value"/> on a
    ///     <see cref="MutationKind.Set"/>, missing <see cref="WalRecord.EndExclusiveKey"/>,
    ///     empty required fields). Tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonSchema"/>.
    ///   </item>
    ///   <item>
    ///     <see cref="InvalidOperationException"/> - surfaced for
    ///     unrecognised <see cref="LatticeMergeMode"/> dispatch and
    ///     CAS-budget exhaustion on state-merge applies. Both are
    ///     payload-shape faults from the receiver's perspective and
    ///     are tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonSchema"/>.
    ///   </item>
    ///   <item>
    ///     Anything else - tagged
    ///     <see cref="LatticeReplicationMetrics.ReasonUnknown"/>.
    ///     Future iterations may decorate the applier to surface
    ///     <see cref="LatticeReplicationMetrics.ReasonOversized"/> /
    ///     <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>
    ///     classifications when the size/skew validation seams land.
    ///   </item>
    /// </list>
    /// </remarks>
    private static string ClassifyFailure(Exception failure) => failure switch
    {
        ArgumentException => LatticeReplicationMetrics.ReasonSchema,
        InvalidOperationException => LatticeReplicationMetrics.ReasonSchema,
        _ => LatticeReplicationMetrics.ReasonUnknown,
    };
}
