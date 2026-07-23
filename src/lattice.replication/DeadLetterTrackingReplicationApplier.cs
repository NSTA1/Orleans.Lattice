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
/// is parked on the per-tree dead-letter queue, the per-origin
/// high-water-mark is advanced past the entry so subsequent apply
/// attempts dedupe it, and a non-applied <see cref="ApplyResult"/> is
/// returned to the caller. A successful apply clears the counter for
/// that tuple so later transient failures get a fresh budget.
/// <para>
/// The retry counter is intentionally in-memory: the decorator is
/// registered as a singleton, so all apply paths share the same
/// counter within a silo. A silo restart resets the counters,
/// effectively giving every entry another <c>MaxApplyRetries</c>
/// attempts after a failover - this is the desired behaviour because
/// silo restart usually correlates with the very transient failure
/// the retry budget is meant to absorb.
/// </para>
/// </summary>
internal sealed class DeadLetterTrackingReplicationApplier(
    IReplicationApplier inner,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILogger<DeadLetterTrackingReplicationApplier> logger) : IReplicationApplier
{
    private readonly ConcurrentDictionary<RetryKey, int> _failures = new();

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
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
            return await OnFailureAsync(entry, ex, cancellationToken).ConfigureAwait(false);
        }

        // Successful apply (or filtered re-delivery) clears any
        // accumulated failure state for the tuple.
        _failures.TryRemove(KeyFor(entry), out _);
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

        // Single-entry fast path: defer to the per-entry decorator so
        // there is exactly one retry-budget code path on the hot path
        // for low-rate (single-entry per push) deployments.
        if (entries.Count == 1)
        {
            return await ApplyAsync(entries[0], cancellationToken).ConfigureAwait(false);
        }

        // Steady-state heuristic: if no entry has any prior failure
        // history we route the batch through the inner applier's
        // optimised batch path. The check is O(n) over the dictionary
        // count (~zero in steady state) so this is cheap.
        var hasHistory = false;
        if (!_failures.IsEmpty)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                if (_failures.ContainsKey(KeyFor(entries[i])))
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
                return await inner.ApplyBatchAsync(entries, cancellationToken).ConfigureAwait(false);
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
        for (var i = 0; i < entries.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await ApplyAsync(entries[i], cancellationToken).ConfigureAwait(false);
            if (result.Applied)
            {
                applied = true;
            }
            if (result.Deferred)
            {
                anyDeferred = true;
            }
            if (result.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = result.HighWaterMark;
            }
        }
        return new ApplyResult { Applied = applied, HighWaterMark = highest, Deferred = anyDeferred };
    }

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
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonSchema));

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

        // Threshold reached: park the entry, advance the HWM past it
        // so subsequent re-deliveries from the transport are deduped
        // by the canonical applier, and clear the counter so a future
        // entry against the same tuple gets a fresh budget.
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        var reasonTag = ClassifyFailure(failure);
        await dlq.EnqueueAsync(entry, failure.Message ?? "<no message>", attempts, reasonTag, cancellationToken).ConfigureAwait(false);

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

